use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Local, NaiveDate, NaiveTime, TimeZone};
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use fs2::FileExt as _;
use sha2::Digest as _;
use sha2::Sha256;
use std::ffi::OsStr;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "forever-ago",
    about = "Nightly tar.gz backups with checksum verification + retention pruning",
    version
)]
struct Cli {
    /// Source directory to back up.
    ///
    /// Default: current working directory (use PM2 `cwd`).
    #[arg(long, default_value = ".")]
    source: PathBuf,

    /// Destination directory where backups are written.
    ///
    /// Default: $HOME/backups
    #[arg(long)]
    dest_dir: Option<PathBuf>,

    /// Backup filename prefix.
    ///
    /// Backup files are named: <prefix>-YYYY-MM-DD.tar.gz
    #[arg(long)]
    prefix: String,

    /// Nightly backup time in local time, 24h HH:MM.
    #[arg(long, default_value = "03:00")]
    at: String,

    /// Number of backups to keep (newest). Older backups are deleted only after a successful backup + verification.
    #[arg(long, default_value_t = 7)]
    retain: usize,

    /// Run a single backup immediately and exit.
    #[arg(long)]
    once: bool,

    /// In daemon mode: run a backup immediately on startup, then continue nightly.
    #[arg(long)]
    run_now: bool,
}

#[derive(Clone, Debug)]
struct Config {
    source_dir: PathBuf,
    dest_dir: PathBuf,
    prefix: String,
    at: NaiveTime,
    retain_count: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let at = NaiveTime::parse_from_str(&cli.at, "%H:%M")
        .with_context(|| format!("invalid --at value {:?} (expected HH:MM, e.g. 03:00)", cli.at))?;

    let source_dir = abs_path(&expand_tilde(&cli.source)?)?
        .canonicalize()
        .with_context(|| format!("source directory does not exist: {}", cli.source.display()))?;

    let dest_dir = match cli.dest_dir {
        Some(p) => abs_path(&expand_tilde(&p)?)?,
        None => default_backup_dir()?,
    };

    let cfg = Config {
        source_dir,
        dest_dir,
        prefix: cli.prefix,
        at,
        retain_count: cli.retain,
    };

    fs::create_dir_all(&cfg.dest_dir).with_context(|| {
        format!(
            "failed to create destination directory {}",
            cfg.dest_dir.display()
        )
    })?;

    // Safety: avoid writing backups inside the directory being archived (self-including tarballs).
    let dest_dir_canon = cfg.dest_dir.canonicalize().unwrap_or_else(|_| cfg.dest_dir.clone());
    if dest_dir_canon.starts_with(&cfg.source_dir) {
        bail!(
            "destination directory {} is inside source directory {}; choose a destination outside the source tree",
            cfg.dest_dir.display(),
            cfg.source_dir.display()
        );
    }

    // Prevent overlapping backup daemons and/or concurrent one-shot runs.
    let _lock = acquire_lock(&cfg.dest_dir, &cfg.prefix)?;

    if cli.once {
        run_backup(&cfg)?;
        return Ok(());
    }

    if cli.run_now {
        if let Err(err) = run_backup(&cfg) {
            log("ERROR", format!("startup backup failed: {err:#}"));
        }
    }

    loop {
        let now = Local::now();
        let next = next_run_after(now, cfg.at)?;
        log("INFO", format!("next backup scheduled at {}", next.to_rfc3339()));

        let sleep_for = next
            .signed_duration_since(now)
            .to_std()
            .unwrap_or(Duration::from_secs(0));
        std::thread::sleep(sleep_for);

        if let Err(err) = run_backup(&cfg) {
            log("ERROR", format!("backup run failed: {err:#}"));
        }
    }
}

fn log(level: &str, msg: impl AsRef<str>) {
    eprintln!("{} [{level}] {}", Local::now().to_rfc3339(), msg.as_ref());
}

fn default_backup_dir() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("could not resolve $HOME"))?;
    Ok(home.join("backups"))
}

fn expand_tilde(path: &Path) -> Result<PathBuf> {
    let s = path.to_string_lossy();
    if s == "~" || s.starts_with("~/") {
        let home = dirs::home_dir().ok_or_else(|| anyhow!("could not resolve $HOME"))?;
        if s == "~" {
            return Ok(home);
        }
        return Ok(home.join(&s[2..]));
    }
    Ok(path.to_path_buf())
}

fn abs_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("failed to read current working directory")?
        .join(path))
}

fn acquire_lock(dest_dir: &Path, prefix: &str) -> Result<File> {
    let lock_path = dest_dir.join(format!("{prefix}.lock"));
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open lock file {}", lock_path.display()))?;
    f.try_lock_exclusive()
        .with_context(|| format!("failed to acquire lock {}", lock_path.display()))?;
    Ok(f)
}

fn next_run_after(now: DateTime<Local>, at: NaiveTime) -> Result<DateTime<Local>> {
    let today = now.date_naive();
    let mut candidate = local_dt(today, at)?;
    if candidate <= now {
        let tomorrow = today
            .succ_opt()
            .ok_or_else(|| anyhow!("could not compute tomorrow's date"))?;
        candidate = local_dt(tomorrow, at)?;
    }
    Ok(candidate)
}

fn local_dt(date: NaiveDate, time: NaiveTime) -> Result<DateTime<Local>> {
    let mut naive = date.and_time(time);
    for _ in 0..180 {
        match Local.from_local_datetime(&naive) {
            chrono::LocalResult::Single(dt) => return Ok(dt),
            chrono::LocalResult::Ambiguous(dt1, dt2) => return Ok(dt1.min(dt2)),
            chrono::LocalResult::None => {
                // Local time doesn't exist (DST spring-forward). Walk forward until it does.
                naive += chrono::Duration::minutes(1);
            }
        }
    }
    bail!("no valid local time found near {date} {time}")
}

fn run_backup(cfg: &Config) -> Result<()> {
    let date_str = Local::now().format("%Y-%m-%d").to_string();
    let filename = format!("{}-{}.tar.gz", cfg.prefix, date_str);
    let final_path = cfg.dest_dir.join(&filename);
    let sha_path = cfg.dest_dir.join(format!("{filename}.sha256"));

    // If today's backup already exists and verifies against its stored checksum, do nothing.
    if final_path.exists() && sha_path.exists() {
        match verify_against_sha_file(&final_path, &sha_path) {
            Ok(true) => {
                log(
                    "INFO",
                    format!(
                        "backup already exists and checksum verified, skipping: {}",
                        final_path.display()
                    ),
                );
                prune_old_backups(cfg)?;
                return Ok(());
            }
            Ok(false) => {
                log(
                    "WARN",
                    format!(
                        "existing backup/checksum did not verify, will replace: {}",
                        final_path.display()
                    ),
                );
            }
            Err(err) => {
                log(
                    "WARN",
                    format!(
                        "failed to verify existing backup/checksum, will replace: {} ({err:#})",
                        final_path.display()
                    ),
                );
            }
        }
    }

    let tmp_name = format!("{filename}.tmp-{}", std::process::id());
    let tmp_path = cfg.dest_dir.join(&tmp_name);
    if tmp_path.exists() {
        fs::remove_file(&tmp_path)
            .with_context(|| format!("failed to remove stale temp file {}", tmp_path.display()))?;
    }

    log(
        "INFO",
        format!(
            "creating backup of {} -> {}",
            cfg.source_dir.display(),
            final_path.display()
        ),
    );

    let (sha_bytes, bytes_written) = write_tar_gz(&cfg.source_dir, &tmp_path)?;
    let sha_hex = hex::encode(sha_bytes);

    // Verify by re-hashing the written file and comparing to the hash computed while writing.
    let verify_bytes = sha256_path(&tmp_path)?;
    if verify_bytes != sha_bytes {
        let _ = fs::remove_file(&tmp_path);
        bail!(
            "checksum verification failed for {} (expected {sha_hex}, got {})",
            tmp_path.display(),
            hex::encode(verify_bytes)
        );
    }

    // Atomic-ish replace: rename temp into place after verification.
    if final_path.exists() {
        fs::remove_file(&final_path)
            .with_context(|| format!("failed to remove existing backup {}", final_path.display()))?;
    }
    if sha_path.exists() {
        fs::remove_file(&sha_path)
            .with_context(|| format!("failed to remove existing checksum {}", sha_path.display()))?;
    }
    fs::rename(&tmp_path, &final_path).with_context(|| {
        format!(
            "failed to move temp backup into place {} -> {}",
            tmp_path.display(),
            final_path.display()
        )
    })?;

    write_sha256_file(&sha_path, &sha_hex, &filename)?;

    log(
        "INFO",
        format!(
            "backup complete: {} ({} bytes) sha256={}",
            final_path.display(),
            bytes_written,
            sha_hex
        ),
    );

    prune_old_backups(cfg)?;
    Ok(())
}

fn write_tar_gz(source_dir: &Path, out_path: &Path) -> Result<([u8; 32], u64)> {
    let out_file = File::create(out_path)
        .with_context(|| format!("failed to create output file {}", out_path.display()))?;
    let buf = BufWriter::new(out_file);
    let hashing = HashingWriter::new(buf);
    let gz = GzEncoder::new(hashing, Compression::default());

    let mut tar = tar::Builder::new(gz);
    tar.follow_symlinks(false);

    let root_name = source_dir
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap();
    tar.append_dir_all(root_name, source_dir).with_context(|| {
        format!(
            "failed to archive source directory {}",
            source_dir.display()
        )
    })?;

    // Finish writing tar, then gzip, then flush/sync.
    let gz = tar
        .into_inner()
        .context("failed to finalize tar stream")?;
    let hashing = gz.finish().context("failed to finalize gzip stream")?;
    let (buf, digest, bytes_written) = hashing.finish();
    let out_file = buf
        .into_inner()
        .context("failed to flush gzip output to disk")?;
    out_file
        .sync_all()
        .context("failed to fsync backup output file")?;

    Ok((digest, bytes_written))
}

fn sha256_path(path: &Path) -> Result<[u8; 32]> {
    let mut f = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

fn write_sha256_file(path: &Path, sha_hex: &str, filename: &str) -> Result<()> {
    let mut f =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    writeln!(f, "{sha_hex}  {filename}")?;
    f.sync_all().ok(); // best-effort
    Ok(())
}

fn verify_against_sha_file(backup_path: &Path, sha_path: &Path) -> Result<bool> {
    let contents = fs::read_to_string(sha_path)
        .with_context(|| format!("failed to read checksum file {}", sha_path.display()))?;
    let expected = contents.split_whitespace().next().unwrap_or("");
    if expected.len() != 64 {
        return Ok(false);
    }
    let actual = sha256_path(backup_path)?;
    Ok(hex::encode(actual).eq_ignore_ascii_case(expected))
}

fn prune_old_backups(cfg: &Config) -> Result<()> {
    let mut backups: Vec<(NaiveDate, String)> = Vec::new();

    for entry in fs::read_dir(&cfg.dest_dir)
        .with_context(|| format!("failed to read {}", cfg.dest_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy().to_string();
        if let Some(date) = parse_backup_date(&cfg.prefix, &file_name) {
            backups.push((date, file_name));
        }
    }

    backups.sort_by_key(|(d, _)| *d);
    if backups.len() <= cfg.retain_count {
        return Ok(());
    }

    let to_delete = backups.len() - cfg.retain_count;
    log(
        "INFO",
        format!(
            "found {} backups, pruning {} oldest (retain={})",
            backups.len(),
            to_delete,
            cfg.retain_count
        ),
    );

    for (_, file_name) in backups.into_iter().take(to_delete) {
        let backup_path = cfg.dest_dir.join(&file_name);
        let sha_path = cfg.dest_dir.join(format!("{file_name}.sha256"));

        log("INFO", format!("deleting old backup {}", backup_path.display()));
        if let Err(err) = fs::remove_file(&backup_path) {
            log(
                "WARN",
                format!("failed to delete {}: {err}", backup_path.display()),
            );
        }
        if sha_path.exists() {
            let _ = fs::remove_file(&sha_path);
        }
    }

    Ok(())
}

fn parse_backup_date(prefix: &str, file_name: &str) -> Option<NaiveDate> {
    let prefix_with_dash = format!("{prefix}-");
    if !file_name.starts_with(&prefix_with_dash) {
        return None;
    }
    if !file_name.ends_with(".tar.gz") {
        return None;
    }

    let date_start = prefix_with_dash.len();
    let date_end = file_name.len() - ".tar.gz".len();
    let date_str = &file_name[date_start..date_end];
    if date_str.len() != 10 {
        return None;
    }
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()
}

struct HashingWriter<W: Write> {
    inner: W,
    hasher: Sha256,
    bytes_written: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes_written: 0,
        }
    }

    fn finish(self) -> (W, [u8; 32], u64) {
        let digest: [u8; 32] = self.hasher.finalize().into();
        (self.inner, digest, self.bytes_written)
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.bytes_written = self.bytes_written.saturating_add(n as u64);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
