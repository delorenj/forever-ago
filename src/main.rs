use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Datelike, Local, NaiveDate, NaiveTime, TimeZone};
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
use std::collections::{HashMap, HashSet};
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

    /// GFS retention: how many recent daily backups to keep. Implies GFS mode.
    #[arg(long)]
    keep_daily: Option<usize>,

    /// GFS retention: how many weekly backups to keep (newest in each ISO week). Implies GFS mode.
    #[arg(long)]
    keep_weekly: Option<usize>,

    /// GFS retention: how many monthly backups to keep (newest in each month). Implies GFS mode.
    #[arg(long)]
    keep_monthly: Option<usize>,

    /// Exclude paths from the archive. Repeatable.
    ///
    /// Three forms, matched against each entry's path RELATIVE to the source root:
    ///   `name`      -> excludes any path component equal to `name` (e.g. `.venv`, `node_modules`)
    ///   `*.ext`     -> excludes files with that extension (e.g. `*.pyc`)
    ///   `a/b/c`     -> excludes that relative subtree (prefix match)
    /// Excluding a directory prunes the whole subtree — its children are never walked.
    #[arg(long = "exclude")]
    excludes: Vec<String>,

    /// Read additional --exclude patterns from a file, one per line (`#` comments allowed).
    #[arg(long)]
    exclude_from: Option<PathBuf>,
}

/// How many backups to keep, and by what rule.
#[derive(Clone, Debug, PartialEq, Eq)]
enum RetentionPolicy {
    /// Flat rolling window: keep the newest N, delete the rest.
    Count(usize),
    /// Grandfather-father-son: keep N recent dailies, plus the newest backup in
    /// each of the last N ISO weeks, plus the newest in each of the last N months.
    Gfs {
        daily: usize,
        weekly: usize,
        monthly: usize,
    },
}

#[derive(Clone, Debug)]
struct Config {
    source_dir: PathBuf,
    dest_dir: PathBuf,
    prefix: String,
    at: NaiveTime,
    retention: RetentionPolicy,
    excludes: Vec<ExcludePattern>,
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

    // Any --keep-* flag switches to GFS; unset tiers fall back to 7/4/4 so a
    // partial invocation still yields a sane ladder. Plain --retain keeps
    // working untouched, so existing deployments do not change behaviour.
    let retention = if cli.keep_daily.is_some()
        || cli.keep_weekly.is_some()
        || cli.keep_monthly.is_some()
    {
        RetentionPolicy::Gfs {
            daily: cli.keep_daily.unwrap_or(7),
            weekly: cli.keep_weekly.unwrap_or(4),
            monthly: cli.keep_monthly.unwrap_or(4),
        }
    } else {
        RetentionPolicy::Count(cli.retain)
    };

    let mut exclude_strings = cli.excludes.clone();
    if let Some(path) = &cli.exclude_from {
        let path = abs_path(&expand_tilde(path)?)?;
        let text = fs::read_to_string(&path)
            .with_context(|| format!("failed to read --exclude-from file {}", path.display()))?;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            exclude_strings.push(line.to_string());
        }
    }
    let excludes: Vec<ExcludePattern> =
        exclude_strings.iter().map(|p| ExcludePattern::parse(p)).collect();

    let cfg = Config {
        source_dir,
        dest_dir,
        prefix: cli.prefix,
        at,
        retention,
        excludes,
    };

    match &cfg.retention {
        RetentionPolicy::Count(n) => log("INFO", format!("retention: keep newest {n}")),
        RetentionPolicy::Gfs { daily, weekly, monthly } => log(
            "INFO",
            format!("retention: GFS {daily} daily / {weekly} weekly / {monthly} monthly"),
        ),
    }
    if !cfg.excludes.is_empty() {
        log("INFO", format!("{} exclude pattern(s) active", cfg.excludes.len()));
    }

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

    let (sha_bytes, bytes_written) = write_tar_gz(&cfg.source_dir, &tmp_path, &cfg.excludes)?;
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

/// A single --exclude rule. Deliberately not a full glob engine: these three
/// shapes cover everything a backup scope needs, and a predictable matcher is
/// worth more here than an expressive one you have to reason about at 3am.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ExcludePattern {
    /// `.venv` — any path component with this exact name.
    Component(String),
    /// `*.pyc` — any file with this extension.
    Extension(String),
    /// `a/b/c` — this relative subtree.
    Prefix(PathBuf),
}

impl ExcludePattern {
    fn parse(raw: &str) -> Self {
        let raw = raw.trim().trim_end_matches('/');
        if let Some(ext) = raw.strip_prefix("*.") {
            ExcludePattern::Extension(ext.to_ascii_lowercase())
        } else if raw.contains('/') {
            ExcludePattern::Prefix(PathBuf::from(raw))
        } else {
            ExcludePattern::Component(raw.to_string())
        }
    }

    /// `rel` is the path relative to the source root.
    fn matches(&self, rel: &Path) -> bool {
        match self {
            ExcludePattern::Component(name) => rel
                .components()
                .any(|c| c.as_os_str().to_string_lossy() == name.as_str()),
            ExcludePattern::Extension(ext) => rel
                .extension()
                .map(|e| e.to_string_lossy().to_ascii_lowercase() == *ext)
                .unwrap_or(false),
            ExcludePattern::Prefix(prefix) => rel.starts_with(prefix),
        }
    }
}

fn is_excluded(rel: &Path, patterns: &[ExcludePattern]) -> bool {
    patterns.iter().any(|p| p.matches(rel))
}

/// Walk `dir` and append everything not excluded. Excluded directories are
/// pruned, not merely skipped, so we never pay to descend into a 495 MB
/// .stversions tree just to drop each file individually.
fn append_filtered<W: Write>(
    tar: &mut tar::Builder<W>,
    source_root: &Path,
    dir: &Path,
    root_name: &str,
    excludes: &[ExcludePattern],
    stats: &mut ArchiveStats,
) -> Result<()> {
    let mut entries: Vec<_> = fs::read_dir(dir)
        .with_context(|| format!("failed to read {}", dir.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    // Deterministic order: two runs over an unchanged tree produce identical tars.
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let rel = match path.strip_prefix(source_root) {
            Ok(r) => r.to_path_buf(),
            Err(_) => continue,
        };
        if is_excluded(&rel, excludes) {
            stats.excluded += 1;
            continue;
        }

        let ft = entry.file_type()?;
        let name_in_tar = Path::new(root_name).join(&rel);

        if ft.is_dir() {
            tar.append_dir(&name_in_tar, &path)
                .with_context(|| format!("failed to archive dir {}", path.display()))?;
            append_filtered(tar, source_root, &path, root_name, excludes, stats)?;
        } else if ft.is_symlink() {
            let mut header = tar::Header::new_gnu();
            let meta = fs::symlink_metadata(&path)?;
            header.set_metadata(&meta);
            header.set_entry_type(tar::EntryType::Symlink);
            let target = fs::read_link(&path)?;
            tar.append_link(&mut header, &name_in_tar, &target)
                .with_context(|| format!("failed to archive symlink {}", path.display()))?;
            stats.included += 1;
        } else if ft.is_file() {
            let mut f = File::open(&path)
                .with_context(|| format!("failed to open {}", path.display()))?;
            tar.append_file(&name_in_tar, &mut f)
                .with_context(|| format!("failed to archive file {}", path.display()))?;
            stats.included += 1;
            stats.bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
        // Anything else (sockets, fifos) is deliberately skipped.
    }
    Ok(())
}

#[derive(Default, Debug)]
struct ArchiveStats {
    included: u64,
    excluded: u64,
    bytes: u64,
}

fn write_tar_gz(
    source_dir: &Path,
    out_path: &Path,
    excludes: &[ExcludePattern],
) -> Result<([u8; 32], u64)> {
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

    let mut stats = ArchiveStats::default();
    if excludes.is_empty() {
        tar.append_dir_all(root_name, source_dir).with_context(|| {
            format!(
                "failed to archive source directory {}",
                source_dir.display()
            )
        })?;
    } else {
        append_filtered(&mut tar, source_dir, source_dir, root_name, excludes, &mut stats)?;
        log(
            "INFO",
            format!(
                "archived {} entries ({:.1} MB on disk); {} path(s) excluded \
                 (a pruned directory counts once, with its whole subtree)",
                stats.included,
                stats.bytes as f64 / 1_048_576.0,
                stats.excluded
            ),
        );
    }

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

    let to_delete: Vec<String> = match &cfg.retention {
        RetentionPolicy::Count(retain) => {
            if backups.len() <= *retain {
                return Ok(());
            }
            let n = backups.len() - *retain;
            log(
                "INFO",
                format!(
                    "found {} backups, pruning {} oldest (retain={})",
                    backups.len(),
                    n,
                    retain
                ),
            );
            backups.iter().take(n).map(|(_, f)| f.clone()).collect()
        }
        RetentionPolicy::Gfs { daily, weekly, monthly } => {
            let keep = select_retained(&backups, *daily, *weekly, *monthly);
            let doomed: Vec<String> = backups
                .iter()
                .filter(|(_, f)| !keep.contains(f))
                .map(|(_, f)| f.clone())
                .collect();
            if doomed.is_empty() {
                return Ok(());
            }
            log(
                "INFO",
                format!(
                    "found {} backups, GFS keeps {} ({}d/{}w/{}m), pruning {}",
                    backups.len(),
                    keep.len(),
                    daily,
                    weekly,
                    monthly,
                    doomed.len()
                ),
            );
            doomed
        }
    };

    for file_name in to_delete {
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

/// Choose which backups survive under grandfather-father-son retention.
///
/// `backups` must be sorted ASCENDING by date; filenames are date-keyed so
/// dates are unique. Returns the union of three tiers:
///   * the `daily` most recent backups
///   * the newest backup in each of the `weekly` most recent ISO weeks
///   * the newest backup in each of the `monthly` most recent months
///
/// Anchoring is by BUCKET, not by literal calendar day. Requiring an exact
/// Sunday (or 1st-of-month) file silently forfeits that slot whenever the
/// machine was off on that one day — the backup you most want after an outage
/// is the one an outage would delete. Bucketing keeps the newest backup that
/// actually exists in each period instead.
fn select_retained(
    backups: &[(NaiveDate, String)],
    daily: usize,
    weekly: usize,
    monthly: usize,
) -> HashSet<String> {
    let mut keep = HashSet::new();

    // Newest first for all three passes.
    let desc: Vec<&(NaiveDate, String)> = backups.iter().rev().collect();

    for (_, name) in desc.iter().take(daily) {
        keep.insert((*name).clone());
    }

    // Newest-in-bucket, walking newest->oldest so the first sighting of a
    // bucket is its winner.
    let mut week_seen: HashMap<(i32, u32), ()> = HashMap::new();
    for (date, name) in desc.iter() {
        if week_seen.len() >= weekly {
            break;
        }
        let iso = date.iso_week();
        let key = (iso.year(), iso.week());
        if week_seen.insert(key, ()).is_none() {
            keep.insert((*name).clone());
        }
    }

    let mut month_seen: HashMap<(i32, u32), ()> = HashMap::new();
    for (date, name) in desc.iter() {
        if month_seen.len() >= monthly {
            break;
        }
        let key = (date.year(), date.month());
        if month_seen.insert(key, ()).is_none() {
            keep.insert((*name).clone());
        }
    }

    keep
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(dates: &[&str]) -> Vec<(NaiveDate, String)> {
        let mut v: Vec<(NaiveDate, String)> = dates
            .iter()
            .map(|d| {
                let date = NaiveDate::parse_from_str(d, "%Y-%m-%d").unwrap();
                (date, format!("vault-{d}.tar.gz"))
            })
            .collect();
        v.sort_by_key(|(d, _)| *d);
        v
    }

    fn run(dates: &[&str], d: usize, w: usize, m: usize) -> Vec<String> {
        let b = mk(dates);
        let keep = select_retained(&b, d, w, m);
        let mut out: Vec<String> = keep.into_iter().collect();
        out.sort();
        out
    }

    /// 40 consecutive days: the ladder must collapse to well under 40.
    #[test]
    fn gfs_collapses_a_long_run() {
        let dates: Vec<String> = (0..40)
            .map(|i| {
                (NaiveDate::from_ymd_opt(2026, 1, 1).unwrap() + chrono::Duration::days(i))
                    .format("%Y-%m-%d")
                    .to_string()
            })
            .collect();
        let refs: Vec<&str> = dates.iter().map(|s| s.as_str()).collect();
        let kept = run(&refs, 7, 4, 4);
        // 7 dailies + up to 4 week-winners + up to 2 month-winners (Jan/Feb),
        // minus overlap where a week/month winner is already a daily.
        assert!(kept.len() <= 13, "kept too many: {}", kept.len());
        assert!(kept.len() >= 7, "must keep at least the dailies: {}", kept.len());
        // The newest 7 are always present.
        for d in refs.iter().rev().take(7) {
            assert!(kept.contains(&format!("vault-{d}.tar.gz")), "missing daily {d}");
        }
    }

    /// The oldest backup in a long run must be pruned, not kept forever.
    #[test]
    fn oldest_is_pruned() {
        let dates: Vec<String> = (0..60)
            .map(|i| {
                (NaiveDate::from_ymd_opt(2026, 1, 1).unwrap() + chrono::Duration::days(i))
                    .format("%Y-%m-%d")
                    .to_string()
            })
            .collect();
        let refs: Vec<&str> = dates.iter().map(|s| s.as_str()).collect();
        let kept = run(&refs, 7, 4, 4);
        assert!(!kept.contains(&"vault-2026-01-01.tar.gz".to_string()));
    }

    /// A gap on Sunday must NOT forfeit the weekly slot — the whole reason for
    /// bucket anchoring rather than literal weekday matching.
    #[test]
    fn missed_sunday_still_keeps_that_week() {
        // Week of 2026-01-05..11 (Mon..Sun); Sunday the 11th is MISSING.
        let kept = run(
            &["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09",
              "2026-01-19", "2026-01-20", "2026-01-21", "2026-01-22", "2026-01-23",
              "2026-01-24", "2026-01-25", "2026-01-26"],
            3, 3, 1,
        );
        // The Jan 5-9 week is one of the 3 most recent weeks present, and its
        // newest member (the 9th) must survive despite no Sunday existing.
        assert!(
            kept.contains(&"vault-2026-01-09.tar.gz".to_string()),
            "weekly bucket lost because no Sunday existed: {kept:?}"
        );
    }

    /// Monthly tier keeps the newest backup of each month, not the 1st.
    #[test]
    fn monthly_keeps_newest_in_month() {
        let kept = run(
            &["2026-01-02", "2026-01-17", "2026-02-03", "2026-02-27", "2026-03-05"],
            1, 0, 3,
        );
        assert!(kept.contains(&"vault-2026-01-17.tar.gz".to_string()), "{kept:?}");
        assert!(kept.contains(&"vault-2026-02-27.tar.gz".to_string()), "{kept:?}");
        assert!(!kept.contains(&"vault-2026-01-02.tar.gz".to_string()), "{kept:?}");
    }

    /// Fewer backups than the ladder asks for: keep everything, delete nothing.
    #[test]
    fn small_set_keeps_all() {
        let dates = ["2026-05-01", "2026-05-02", "2026-05-03"];
        let kept = run(&dates, 7, 4, 4);
        assert_eq!(kept.len(), 3);
    }

    #[test]
    fn exclude_component_matches_any_depth() {
        let p = ExcludePattern::parse(".venv");
        assert!(p.matches(Path::new("proj/.venv/lib/x.py")));
        assert!(p.matches(Path::new(".venv")));
        assert!(!p.matches(Path::new("proj/venv/x.py")));
    }

    #[test]
    fn exclude_extension_is_case_insensitive() {
        let p = ExcludePattern::parse("*.PYC");
        assert!(p.matches(Path::new("a/b/c.pyc")));
        assert!(!p.matches(Path::new("a/b/c.py")));
    }

    #[test]
    fn exclude_prefix_matches_subtree_only() {
        let p = ExcludePattern::parse("agents/hermes/pm/runtime");
        assert!(p.matches(Path::new("agents/hermes/pm/runtime/state.db")));
        assert!(!p.matches(Path::new("agents/hermes/pm/notes.md")));
    }

    #[test]
    fn stversions_is_excluded_but_real_notes_are_not() {
        let pats: Vec<ExcludePattern> = [".stversions", ".venv", "node_modules", "*.pyc"]
            .iter().map(|p| ExcludePattern::parse(p)).collect();
        assert!(is_excluded(Path::new(".stversions/Notes/old.md"), &pats));
        assert!(is_excluded(Path::new("x/node_modules/y/z.js"), &pats));
        assert!(!is_excluded(Path::new("Notes/2026/thinking.md"), &pats));
    }
}
