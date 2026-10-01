use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Datelike, Local, NaiveDate, NaiveTime, TimeZone};
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use fs2::FileExt as _;
use sha2::Digest as _;
use sha2::Sha256;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

mod jobs;
mod list;

#[derive(Parser, Debug)]
#[command(
    name = "forever-ago",
    about = "Nightly tar.gz backups with checksum verification + retention pruning",
    version,
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

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
    #[arg(long, required = true)]
    prefix: Option<String>,

    /// Nightly backup time in local time, 24h HH:MM.
    #[arg(long, default_value = "03:00")]
    at: String,

    /// Number of backups to keep (newest). Older backups are deleted only after a successful backup + verification.
    ///
    /// Cannot be combined with --keep-daily/--keep-weekly/--keep-monthly.
    #[arg(
        long,
        default_value_t = 7,
        conflicts_with_all = ["keep_daily", "keep_weekly", "keep_monthly"]
    )]
    retain: usize,

    /// Run a single backup immediately and exit.
    #[arg(long)]
    once: bool,

    /// In daemon mode: run a backup immediately on startup, then continue nightly.
    #[arg(long)]
    run_now: bool,

    /// GFS retention: how many recent daily backups to keep. Implies GFS mode.
    ///
    /// Defaults to 7 when only another --keep-* flag is given.
    #[arg(long)]
    keep_daily: Option<usize>,

    /// GFS retention: how many weekly backups to keep (newest in each ISO week). Implies GFS mode.
    ///
    /// Defaults to 4 when only another --keep-* flag is given.
    #[arg(long)]
    keep_weekly: Option<usize>,

    /// GFS retention: how many monthly backups to keep (newest in each month). Implies GFS mode.
    ///
    /// Defaults to 4 when only another --keep-* flag is given.
    #[arg(long)]
    keep_monthly: Option<usize>,

    /// Exclude paths from the archive. Repeatable.
    ///
    /// Matched against each entry's path RELATIVE to the source root:
    ///   name      any path component named exactly `name` (e.g. `.venv`, `node_modules`)
    ///   ./name    `name` at the source root only
    ///   *.ext     files with that extension, case-insensitive (e.g. `*.pyc`)
    ///   *.ext/    directories with that extension (e.g. `*.egg-info/`)
    ///   na*e?     any path component matching the wildcards: `*` any run, `?` one char
    ///             (e.g. `*.sync-conflict-*`)
    ///   a/b/c     that subtree of the source root
    /// An excluded directory is pruned whole; its children are never walked.
    /// A pattern that could never match (wildcards inside a/b/c, a leading `/`,
    /// an empty line, a lone `*`) is rejected at startup instead of silently
    /// doing nothing.
    #[arg(long = "exclude", value_name = "PATTERN", verbatim_doc_comment)]
    excludes: Vec<String>,

    /// Read additional --exclude patterns from a file, one per line.
    ///
    /// Blank lines and lines starting with `#` are skipped; there are no trailing comments.
    #[arg(long, value_name = "FILE")]
    exclude_from: Option<PathBuf>,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// List the scheduled backup jobs covering the current directory.
    ///
    /// Reads systemd units, crontabs, the PM2 dump and running daemons. If no
    /// job backs up the current directory itself, walks up its parents (stopping
    /// at ~) and reports the nearest one that has jobs.
    Jobs(jobs::JobsArgs),

    /// List the snapshots of the current directory, newest first.
    ///
    /// Snapshots are found through the jobs that write them. If none exist for
    /// the current directory itself, walks up its parents (stopping at ~) and
    /// lists the nearest one that has snapshots.
    List,
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
    match &cli.command {
        Some(Command::Jobs(args)) => return jobs::run(args),
        Some(Command::List) => return list::run(),
        None => {}
    }
    let prefix = cli
        .prefix
        .clone()
        .ok_or_else(|| anyhow!("--prefix is required"))?;

    let at = NaiveTime::parse_from_str(&cli.at, "%H:%M")
        .with_context(|| format!("invalid --at value {:?} (expected HH:MM, e.g. 03:00)", cli.at))?;

    let source_dir = abs_path(&expand_tilde(&cli.source)?)?
        .canonicalize()
        .with_context(|| format!("source directory does not exist: {}", cli.source.display()))?;

    let dest_dir = match &cli.dest_dir {
        Some(p) => abs_path(&expand_tilde(p)?)?,
        None => default_backup_dir()?,
    };

    let retention = retention_policy(&cli)?;

    let mut exclude_strings = cli.excludes.clone();
    if let Some(path) = &cli.exclude_from {
        let path = abs_path(&expand_tilde(path)?)?;
        let lines = read_exclude_file(&path)?;
        if lines.is_empty() {
            log(
                "WARN",
                format!("--exclude-from {} contains no patterns", path.display()),
            );
        }
        exclude_strings.extend(lines);
    }
    let excludes = exclude_strings
        .iter()
        .map(|p| ExcludePattern::parse(p))
        .collect::<Result<Vec<_>>>()?;

    let cfg = Config {
        source_dir,
        dest_dir,
        prefix,
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

    if cli.run_now
        && let Err(err) = run_backup(&cfg)
    {
        log("ERROR", format!("startup backup failed: {err:#}"));
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

/// Any --keep-* flag switches to GFS; unset tiers fall back to 7/4/4 so a
/// partial invocation still yields a sane ladder. Plain --retain keeps
/// working untouched, so existing deployments do not change behaviour.
fn retention_policy(cli: &Cli) -> Result<RetentionPolicy> {
    let policy = if cli.keep_daily.is_some() || cli.keep_weekly.is_some() || cli.keep_monthly.is_some() {
        RetentionPolicy::Gfs {
            daily: cli.keep_daily.unwrap_or(7),
            weekly: cli.keep_weekly.unwrap_or(4),
            monthly: cli.keep_monthly.unwrap_or(4),
        }
    } else {
        RetentionPolicy::Count(cli.retain)
    };
    match policy {
        RetentionPolicy::Count(0) => {
            bail!("--retain 0 would delete every backup, including the one just written")
        }
        RetentionPolicy::Gfs { daily: 0, weekly: 0, monthly: 0 } => bail!(
            "--keep-daily, --keep-weekly and --keep-monthly are all 0, which would delete every backup"
        ),
        policy => Ok(policy),
    }
}

/// --exclude-from: one pattern per line, blank lines and `#` comments skipped.
fn read_exclude_file(path: &Path) -> Result<Vec<String>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read --exclude-from file {}", path.display()))?;
    Ok(text
        .trim_start_matches('\u{feff}')
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect())
}

/// "1.23GB" from a gigabyte up, "985M" / "12K" / "512B" below it (1024-based).
pub(crate) fn human_size(bytes: u64) -> String {
    const K: f64 = 1024.0;
    let b = bytes as f64;
    if b >= K * K * K * K {
        format!("{:.2}TB", b / (K * K * K * K))
    } else if b >= K * K * K {
        format!("{:.2}GB", b / (K * K * K))
    } else if b >= K * K {
        format!("{:.0}M", b / (K * K))
    } else if b >= K {
        format!("{:.0}K", b / K)
    } else {
        format!("{bytes}B")
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
        .truncate(false)
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

    remove_stale_temp_files(cfg)?;

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
                prune_old_backups(cfg, &filename)?;
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

    // Never leave a half-written archive behind: a failed --once run has no
    // later iteration that would clean it up.
    let written = write_tar_gz(&cfg.source_dir, &tmp_path, &cfg.excludes).and_then(|(sha, n)| {
        // Verify by re-hashing the written file and comparing to the hash computed while writing.
        let verify_bytes = sha256_path(&tmp_path)?;
        if verify_bytes != sha {
            bail!(
                "checksum verification failed for {} (expected {}, got {})",
                tmp_path.display(),
                hex::encode(sha),
                hex::encode(verify_bytes)
            );
        }
        Ok((sha, n))
    });
    let (sha_bytes, bytes_written) = match written {
        Ok(ok) => ok,
        Err(err) => {
            let _ = fs::remove_file(&tmp_path);
            return Err(err);
        }
    };
    let sha_hex = hex::encode(sha_bytes);

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

    prune_old_backups(cfg, &filename)?;
    Ok(())
}

/// Temp files from runs that were killed (OOM, timeout, reboot) before they
/// could clean up. Safe to remove: we hold the prefix lock, so no live run of
/// this job is writing one.
fn remove_stale_temp_files(cfg: &Config) -> Result<()> {
    for entry in fs::read_dir(&cfg.dest_dir)
        .with_context(|| format!("failed to read {}", cfg.dest_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        let Some((archive, pid)) = name.rsplit_once(".tmp-") else { continue };
        let ours = parse_backup_date(&cfg.prefix, archive).is_some()
            && !pid.is_empty()
            && pid.chars().all(|c| c.is_ascii_digit());
        if ours && entry.file_type()?.is_file() {
            log("WARN", format!("removing stale temp file from an interrupted run: {name}"));
            if let Err(err) = fs::remove_file(entry.path()) {
                log("WARN", format!("failed to remove {name}: {err}"));
            }
        }
    }
    Ok(())
}

/// A single --exclude rule. Deliberately not a full glob engine: these four
/// shapes cover everything a backup scope needs, and a predictable matcher is
/// worth more here than an expressive one you have to reason about at 3am.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ExcludePattern {
    /// `.venv` — any path component with this exact name.
    Component(String),
    /// `*.pyc` — any file (not directory) with this extension.
    Extension(String),
    /// `*.egg-info/` — any directory with this extension.
    DirExtension(String),
    /// `*.sync-conflict-*` — any path component matching `*` / `?` wildcards.
    Glob(String),
    /// `a/b/c` — this relative subtree.
    Prefix(PathBuf),
}

impl ExcludePattern {
    /// Rejects anything that could never match. A typo'd exclude must fail the
    /// run loudly, not quietly archive what it was meant to drop.
    fn parse(raw: &str) -> Result<Self> {
        let pat = raw.trim();
        let wild = |s: &str| s.contains(['*', '?']);
        if pat.starts_with('/') {
            bail!(
                "invalid exclude pattern {raw:?}: patterns are relative to the source root; \
                 write `./name` to match only at the root, or `name` to match at any depth"
            );
        }
        // `./name` anchors at the source root, like gitignore's `/name`.
        let anchored = pat.starts_with("./");
        let dir_only = pat.ends_with('/');
        let pat = pat.strip_prefix("./").unwrap_or(pat).trim_end_matches('/');
        if pat.is_empty() || pat == "." || pat == ".." {
            bail!("invalid exclude pattern {raw:?}: it names no path");
        }
        if pat.contains('/') || anchored {
            if wild(pat) {
                bail!(
                    "invalid exclude pattern {raw:?}: wildcards only work in single-name patterns, not in ./name or a/b/c paths"
                );
            }
            if pat.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..") {
                bail!("invalid exclude pattern {raw:?}: empty, `.` or `..` path segment");
            }
            return Ok(ExcludePattern::Prefix(PathBuf::from(pat)));
        }
        // `*`, `**`, `*?`: every name. A lone `?` only matches one-character names.
        if pat.contains('*') && pat.chars().all(|c| c == '*' || c == '?') && pat.matches('?').count() <= 1 {
            bail!("invalid exclude pattern {raw:?}: it would exclude everything");
        }
        if let Some(ext) = pat.strip_prefix("*.")
            && !wild(ext)
            && !ext.contains('.')
        {
            let ext = ext.to_ascii_lowercase();
            return Ok(if dir_only { ExcludePattern::DirExtension(ext) } else { ExcludePattern::Extension(ext) });
        }
        if wild(pat) {
            return Ok(ExcludePattern::Glob(pat.to_string()));
        }
        Ok(ExcludePattern::Component(pat.to_string()))
    }

    /// `rel` is the path relative to the source root.
    fn matches(&self, rel: &Path, is_dir: bool) -> bool {
        match self {
            ExcludePattern::Component(name) => rel.components().any(|c| c.as_os_str() == name.as_str()),
            // Files only: a notes folder called `Research.db` is not a database.
            ExcludePattern::Extension(ext) => !is_dir && has_extension(rel, ext),
            ExcludePattern::DirExtension(ext) => is_dir && has_extension(rel, ext),
            ExcludePattern::Glob(pattern) => rel
                .components()
                .any(|c| wildcard_match(pattern, &c.as_os_str().to_string_lossy())),
            ExcludePattern::Prefix(prefix) => rel.starts_with(prefix),
        }
    }
}

fn has_extension(rel: &Path, ext: &str) -> bool {
    rel.extension().is_some_and(|e| e.to_string_lossy().eq_ignore_ascii_case(ext))
}

/// `*` matches any run of characters (including none), `?` exactly one.
fn wildcard_match(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    let (mut pi, mut ni) = (0, 0);
    // Where the last `*` was, and how much of the name it has swallowed so far.
    let mut star: Option<(usize, usize)> = None;
    while ni < n.len() {
        // `*` first: a name may itself contain a literal `*`.
        if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ni));
            pi += 1;
        } else if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if let Some((sp, sn)) = star {
            pi = sp + 1;
            ni = sn + 1;
            star = Some((sp, sn + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&c| c == '*')
}

fn is_excluded(rel: &Path, is_dir: bool, patterns: &[ExcludePattern]) -> bool {
    patterns.iter().any(|p| p.matches(rel, is_dir))
}

/// A path that disappears mid-walk is ordinary churn in a live tree (an
/// editor's swap file, a rotated log): note it and move on. Any other error
/// is a real problem and fails the run, so a backup is never quietly partial.
fn vanished(err: &std::io::Error, path: &Path, stats: &mut ArchiveStats) -> bool {
    if err.kind() != std::io::ErrorKind::NotFound {
        return false;
    }
    log("WARN", format!("vanished while archiving, skipped: {}", path.display()));
    stats.vanished += 1;
    true
}

/// Walk `dir` and append everything not excluded. Excluded directories are
/// pruned, not merely skipped, so we never pay to descend into a 495 MB
/// .stversions tree just to drop each file individually.
///
/// Symlinks are stored as links, never followed: following them would pull
/// in whatever they point at outside the source, and a dangling one would
/// abort the whole run.
fn append_filtered<W: Write>(
    tar: &mut tar::Builder<W>,
    source_root: &Path,
    dir: &Path,
    root_name: &str,
    excludes: &[ExcludePattern],
    stats: &mut ArchiveStats,
) -> Result<()> {
    let mut entries = match fs::read_dir(dir).and_then(|rd| rd.collect::<std::io::Result<Vec<_>>>()) {
        Ok(entries) => entries,
        Err(err) if vanished(&err, dir, stats) => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("failed to read directory {}", dir.display())),
    };
    // Deterministic order: two runs over an unchanged tree produce identical tars.
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let rel = match path.strip_prefix(source_root) {
            Ok(r) => r.to_path_buf(),
            Err(_) => continue,
        };
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(err) if vanished(&err, &path, stats) => continue,
            Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
        };
        if is_excluded(&rel, ft.is_dir(), excludes) {
            stats.excluded += 1;
            continue;
        }
        if ft.is_dir() {
            for p in excludes {
                if let ExcludePattern::Extension(ext) = p
                    && has_extension(&rel, ext)
                    && stats.dir_extension_warned.insert(ext.clone())
                {
                    log(
                        "WARN",
                        format!(
                            "`*.{ext}` excludes files only, so directory {} is kept; write `*.{ext}/` to exclude such directories",
                            rel.display()
                        ),
                    );
                }
            }
        }

        let name_in_tar = Path::new(root_name).join(&rel);

        if ft.is_dir() {
            match tar.append_dir(&name_in_tar, &path) {
                Ok(()) => {}
                Err(err) if vanished(&err, &path, stats) => continue,
                Err(err) => {
                    return Err(err).with_context(|| format!("failed to archive dir {}", path.display()));
                }
            }
            stats.dirs += 1;
            append_filtered(tar, source_root, &path, root_name, excludes, stats)?;
        } else if ft.is_symlink() {
            let link = fs::symlink_metadata(&path).and_then(|meta| Ok((meta, fs::read_link(&path)?)));
            let (meta, target) = match link {
                Ok(link) => link,
                Err(err) if vanished(&err, &path, stats) => continue,
                Err(err) => return Err(err).with_context(|| format!("failed to read symlink {}", path.display())),
            };
            let mut header = tar::Header::new_gnu();
            header.set_metadata(&meta);
            header.set_entry_type(tar::EntryType::Symlink);
            tar.append_link(&mut header, &name_in_tar, &target)
                .with_context(|| format!("failed to archive symlink {}", path.display()))?;
            stats.symlinks += 1;
        } else if ft.is_file() {
            let mut f = match File::open(&path) {
                Ok(f) => f,
                Err(err) if vanished(&err, &path, stats) => continue,
                Err(err) => return Err(err).with_context(|| format!("failed to open {}", path.display())),
            };
            let before = f
                .metadata()
                .with_context(|| format!("failed to stat {}", path.display()))?;
            let len = before.len();
            let mut header = tar::Header::new_gnu();
            header.set_metadata(&before);
            header.set_size(len);
            // Copy exactly the `len` bytes the header promises, padding or
            // truncating if the file changes underneath us. tar's own
            // append_file copies to EOF, so a file saved in place mid-copy
            // misaligns every member after it — and the archive still
            // "verifies", because the checksum covers whatever was written.
            let body = (&mut f).take(len).chain(std::io::repeat(0)).take(len);
            tar.append_data(&mut header, &name_in_tar, body)
                .with_context(|| format!("failed to archive file {}", path.display()))?;
            // Size, mtime and ctime to the nanosecond. A write through a
            // shared mmap can still slip past all three.
            let stamp = |m: &fs::Metadata| {
                use std::os::unix::fs::MetadataExt;
                (m.len(), m.mtime(), m.mtime_nsec(), m.ctime(), m.ctime_nsec())
            };
            if f.metadata().ok().map(|m| stamp(&m)) != Some(stamp(&before)) {
                log(
                    "WARN",
                    format!("changed while being archived, this copy may be inconsistent: {}", path.display()),
                );
                stats.changed += 1;
            }
            stats.files += 1;
            stats.bytes += len;
        } else {
            log("WARN", format!("skipped special file (socket/fifo/device): {}", path.display()));
            stats.special += 1;
        }
    }
    Ok(())
}

#[derive(Default, Debug)]
struct ArchiveStats {
    files: u64,
    dirs: u64,
    symlinks: u64,
    bytes: u64,
    excluded: u64,
    special: u64,
    vanished: u64,
    changed: u64,
    /// `*.ext` patterns already warned about matching a directory.
    dir_extension_warned: HashSet<String>,
}

impl ArchiveStats {
    fn summary(&self) -> String {
        let mut s = format!(
            "archived {} files ({}), {} dirs, {} symlinks; {} path(s) excluded \
             (a pruned directory counts once, with its whole subtree)",
            self.files,
            human_size(self.bytes),
            self.dirs,
            self.symlinks,
            self.excluded
        );
        for (n, what) in [
            (self.special, "special file(s) skipped"),
            (self.vanished, "vanished mid-walk"),
            (self.changed, "changed while being read"),
        ] {
            if n > 0 {
                s.push_str(&format!("; {n} {what}"));
            }
        }
        s
    }
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

    // Use the source dir name as the archive root, e.g. `.openclaw/...`
    let root_name = source_dir
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| anyhow!("cannot name the archive root after {}", source_dir.display()))?;

    let mut stats = ArchiveStats::default();
    tar.append_dir(root_name, source_dir)
        .with_context(|| format!("failed to archive source directory {}", source_dir.display()))?;
    append_filtered(&mut tar, source_dir, source_dir, root_name, excludes, &mut stats)?;
    log("INFO", stats.summary());

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

/// `keep` is the backup this run just wrote or verified. It is never deleted,
/// whatever the policy says: if the clock went backwards and newer-dated
/// archives exist, tonight's would otherwise be the first thing pruned.
fn prune_old_backups(cfg: &Config, keep: &str) -> Result<()> {
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

    let today = Local::now().date_naive();
    if let Some((newest, name)) = backups.last()
        && *newest > today
    {
        log(
            "WARN",
            format!("{name} is dated after today ({today}); is the clock wrong? keeping {keep} regardless"),
        );
    }

    let (kept, rule) = match &cfg.retention {
        RetentionPolicy::Count(retain) => {
            let skip = backups.len().saturating_sub(*retain);
            let kept: HashSet<String> = backups.iter().skip(skip).map(|(_, f)| f.clone()).collect();
            (kept, format!("retain={retain}"))
        }
        RetentionPolicy::Gfs { daily, weekly, monthly } => (
            select_retained(&backups, *daily, *weekly, *monthly),
            format!("GFS {daily}d/{weekly}w/{monthly}m"),
        ),
    };
    let to_delete: Vec<String> = backups
        .iter()
        .map(|(_, f)| f.clone())
        .filter(|f| !kept.contains(f) && f != keep)
        .collect();
    if to_delete.is_empty() {
        return Ok(());
    }
    log(
        "INFO",
        format!(
            "found {} backups, {rule} keeps {}, pruning {}",
            backups.len(),
            backups.len() - to_delete.len(),
            to_delete.len()
        ),
    );

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

    fn names(dates: &[&str]) -> Vec<String> {
        let mut v: Vec<String> = dates.iter().map(|d| format!("vault-{d}.tar.gz")).collect();
        v.sort();
        v
    }

    fn days(start: (i32, u32, u32), n: i64) -> Vec<String> {
        (0..n)
            .map(|i| {
                (NaiveDate::from_ymd_opt(start.0, start.1, start.2).unwrap() + chrono::Duration::days(i))
                    .format("%Y-%m-%d")
                    .to_string()
            })
            .collect()
    }

    /// 40 consecutive days (Thu 2026-01-01 .. Mon 2026-02-09), 7/4/4.
    #[test]
    fn gfs_collapses_a_long_run() {
        let dates = days((2026, 1, 1), 40);
        let refs: Vec<&str> = dates.iter().map(|s| s.as_str()).collect();
        // dailies Feb 3-9; weeks W07 Feb 9, W06 Feb 8, W05 Feb 1, W04 Jan 25;
        // months Feb 9, Jan 31.
        assert_eq!(
            run(&refs, 7, 4, 4),
            names(&[
                "2026-01-25", "2026-01-31", "2026-02-01", "2026-02-03", "2026-02-04",
                "2026-02-05", "2026-02-06", "2026-02-07", "2026-02-08", "2026-02-09",
            ])
        );
    }

    /// The oldest backup in a long run must be pruned, not kept forever.
    #[test]
    fn oldest_is_pruned() {
        let dates = days((2026, 1, 1), 60);
        let refs: Vec<&str> = dates.iter().map(|s| s.as_str()).collect();
        let kept = run(&refs, 7, 4, 4);
        assert!(!kept.contains(&"vault-2026-01-01.tar.gz".to_string()));
    }

    /// ISO 2026 has 53 weeks: Mon Dec 28 2026 .. Sun Jan 3 2027 is ONE week
    /// spanning two calendar years. Bucketing by calendar year would split it.
    #[test]
    fn iso_week_53_is_one_bucket_across_new_year() {
        let kept = run(
            &["2026-12-27", "2026-12-29", "2026-12-31", "2027-01-01", "2027-01-03", "2027-01-04"],
            0, 3, 0,
        );
        assert_eq!(kept, names(&["2026-12-27", "2027-01-03", "2027-01-04"]));
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
        // dailies Jan 24-26; weeks Jan 26 (W05), Jan 25 (W04), Jan 9 (W02).
        assert_eq!(kept, names(&["2026-01-09", "2026-01-24", "2026-01-25", "2026-01-26"]));
    }

    /// Monthly tier keeps the newest backup of each month, not the 1st.
    #[test]
    fn monthly_keeps_newest_in_month() {
        let kept = run(
            &["2026-01-02", "2026-01-17", "2026-02-03", "2026-02-27", "2026-03-05"],
            1, 0, 3,
        );
        assert_eq!(kept, names(&["2026-01-17", "2026-02-27", "2026-03-05"]));
    }

    /// Fewer backups than the ladder asks for: keep everything, delete nothing.
    #[test]
    fn small_set_keeps_all() {
        let dates = ["2026-05-01", "2026-05-02", "2026-05-03"];
        assert_eq!(run(&dates, 7, 4, 4), names(&dates));
    }

    fn cli(args: &[&str]) -> std::result::Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("forever-ago").chain(args.iter().copied()))
    }

    #[test]
    fn retention_flags_resolve_and_guard_against_deleting_everything() {
        let policy = |args: &[&str]| retention_policy(&cli(args).unwrap());
        assert_eq!(policy(&["--prefix", "v"]).unwrap(), RetentionPolicy::Count(7));
        assert_eq!(
            policy(&["--prefix", "v", "--keep-weekly", "2"]).unwrap(),
            RetentionPolicy::Gfs { daily: 7, weekly: 2, monthly: 4 }
        );
        assert!(policy(&["--prefix", "v", "--retain", "0"]).is_err());
        assert!(policy(&["--prefix", "v", "--keep-daily", "0", "--keep-weekly", "0", "--keep-monthly", "0"]).is_err());
        // --retain would otherwise be silently ignored next to --keep-*.
        assert!(cli(&["--prefix", "v", "--retain", "30", "--keep-daily", "7"]).is_err());
    }

    #[test]
    fn jobs_subcommand_needs_no_prefix_but_backups_do() {
        assert!(matches!(cli(&["jobs"]).unwrap().command, Some(Command::Jobs(_))));
        assert!(matches!(cli(&["list"]).unwrap().command, Some(Command::List)));
        assert!(cli(&[]).is_err());
        assert!(cli(&["--prefix", "v", "jobs"]).is_err());
    }

    #[test]
    fn backup_dates_parse_only_for_the_exact_prefix() {
        assert_eq!(
            parse_backup_date("vault", "vault-2026-10-01.tar.gz"),
            NaiveDate::from_ymd_opt(2026, 10, 1)
        );
        assert_eq!(parse_backup_date("vault", "vault-old-2026-10-01.tar.gz"), None);
        assert_eq!(parse_backup_date("vault", "vault-2026-10-01.tar.gz.sha256"), None);
        assert_eq!(parse_backup_date("vault", "vault-2026-10-01.tar.gz.tmp-123"), None);
    }

    fn test_cfg(dest: &Path, retention: RetentionPolicy) -> Config {
        Config {
            source_dir: PathBuf::from("/nonexistent"),
            dest_dir: dest.to_path_buf(),
            prefix: "vault".into(),
            at: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
            retention,
            excludes: Vec::new(),
        }
    }

    #[test]
    fn prune_never_deletes_the_backup_it_just_wrote() {
        let tmp = tempfile::tempdir().unwrap();
        // Clock went backwards: five archives are dated after "today".
        for d in ["2099-01-01", "2099-01-02", "2099-01-03", "2099-01-04", "2099-01-05", "2026-10-01"] {
            fs::write(tmp.path().join(format!("vault-{d}.tar.gz")), b"x").unwrap();
        }
        prune_old_backups(&test_cfg(tmp.path(), RetentionPolicy::Count(3)), "vault-2026-10-01.tar.gz").unwrap();
        let mut left: Vec<String> = fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left, names(&["2026-10-01", "2099-01-03", "2099-01-04", "2099-01-05"]));
    }

    #[test]
    fn stale_temp_files_are_swept_but_other_prefixes_are_not() {
        let tmp = tempfile::tempdir().unwrap();
        for name in [
            "vault-2026-09-30.tar.gz.tmp-4242",
            "vault-old-2026-09-30.tar.gz.tmp-4242",
            "vault-2026-09-30.tar.gz",
            "x.tmp-y-2026-09-30.tar.gz.tmp-77",
        ] {
            fs::write(tmp.path().join(name), b"x").unwrap();
        }
        remove_stale_temp_files(&test_cfg(tmp.path(), RetentionPolicy::Count(7))).unwrap();
        assert!(!tmp.path().join("vault-2026-09-30.tar.gz.tmp-4242").exists());
        assert!(tmp.path().join("vault-old-2026-09-30.tar.gz.tmp-4242").exists());
        assert!(tmp.path().join("vault-2026-09-30.tar.gz").exists());

        // A prefix that itself contains `.tmp-` is still recognised.
        let mut cfg = test_cfg(tmp.path(), RetentionPolicy::Count(7));
        cfg.prefix = "x.tmp-y".into();
        remove_stale_temp_files(&cfg).unwrap();
        assert!(!tmp.path().join("x.tmp-y-2026-09-30.tar.gz.tmp-77").exists());
    }

    fn pat(raw: &str) -> ExcludePattern {
        ExcludePattern::parse(raw).unwrap()
    }

    #[test]
    fn exclude_component_matches_any_depth() {
        let p = pat(".venv");
        assert!(p.matches(Path::new("proj/.venv/lib/x.py"), false));
        assert!(p.matches(Path::new(".venv"), true));
        assert!(!p.matches(Path::new("proj/venv/x.py"), false));
    }

    #[test]
    fn exclude_extension_is_case_insensitive_and_files_only() {
        let p = pat("*.PYC");
        assert!(p.matches(Path::new("a/b/c.pyc"), false));
        assert!(!p.matches(Path::new("a/b/c.py"), false));
        // A directory named like a database is a folder of notes, not a database.
        assert!(!pat("*.db").matches(Path::new("Research.db"), true));
    }

    #[test]
    fn exclude_prefix_matches_subtree_only() {
        let p = pat("./agents/hermes/pm/runtime/");
        assert_eq!(p, ExcludePattern::Prefix(PathBuf::from("agents/hermes/pm/runtime")));
        assert!(p.matches(Path::new("agents/hermes/pm/runtime/state.db"), false));
        assert!(!p.matches(Path::new("agents/hermes/pm/notes.md"), false));
        assert!(!p.matches(Path::new("agents/hermes/pm/runtime2"), true));
    }

    /// The vault's real `*.sync-conflict-*` line: it parsed as an extension and
    /// matched nothing, so 513 MB of Syncthing conflict copies rode along nightly.
    #[test]
    fn exclude_glob_catches_syncthing_conflicts() {
        let p = pat("*.sync-conflict-*");
        assert!(matches!(p, ExcludePattern::Glob(_)));
        for hit in [
            "TODAY.sync-conflict-20260920-021511-DSLLPB2.md",
            "Makefile.sync-conflict-20260920-021507-DSLLPB2",
            ".sync-conflict-20260920-021511-DSLLPB2.gitmodules",
            "agents/state.sync-conflict-20260722-212242-DSLLPB2.db",
        ] {
            assert!(p.matches(Path::new(hit), false), "{hit}");
        }
        assert!(!p.matches(Path::new("Notes/sync-conflicts-explained.md"), false));
        assert!(pat("*.tar.gz").matches(Path::new("x/a.tar.gz"), false));
        assert!(pat("notes-202?").matches(Path::new("notes-2026/a.md"), false));
    }

    #[test]
    fn exclude_patterns_that_cannot_match_are_rejected() {
        for bad in ["", "   ", "/", "./", ".", "..", "/abs/path", "/name", "a/*/b", "./a*", "a//b", "a/../b", "*", "**", "*?"] {
            assert!(ExcludePattern::parse(bad).is_err(), "{bad:?} should be rejected");
        }
        // `?` alone only matches one-character names: narrow, but legitimate.
        assert!(pat("?").matches(Path::new("x"), false));
        assert!(!pat("?").matches(Path::new("xy"), false));
    }

    /// `./name` means the root's `name` only; bare `name` means any depth.
    #[test]
    fn exclude_dot_slash_anchors_at_the_root() {
        let p = pat("./build");
        assert_eq!(p, ExcludePattern::Prefix(PathBuf::from("build")));
        assert!(p.matches(Path::new("build/out.o"), false));
        assert!(!p.matches(Path::new("src/build/out.o"), false));
        assert!(pat("build").matches(Path::new("src/build"), true));
    }

    #[test]
    fn exclude_dir_extension_with_trailing_slash() {
        let p = pat("*.egg-info/");
        assert_eq!(p, ExcludePattern::DirExtension("egg-info".into()));
        assert!(p.matches(Path::new("pkg/foo.egg-info"), true));
        assert!(!p.matches(Path::new("pkg/notes.egg-info"), false));
    }

    #[test]
    fn exclude_file_skips_comments_blank_lines_and_bom() {
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("ex");
        fs::write(&f, "\u{feff}# header\n\n.venv\r\n  *.pyc  \n").unwrap();
        assert_eq!(read_exclude_file(&f).unwrap(), vec![".venv", "*.pyc"]);
    }

    #[test]
    fn wildcard_matcher() {
        assert!(wildcard_match("a*c", "abbbc"));
        assert!(wildcard_match("a*c", "ac"));
        assert!(wildcard_match("*x*", "zzxzz"));
        assert!(wildcard_match("a?c", "abc"));
        assert!(!wildcard_match("a?c", "ac"));
        assert!(!wildcard_match("a*c", "abcd"));
        assert!(wildcard_match("*.*.*", "a.b.c"));
        // A literal `*` in the name must not eat the pattern's wildcard.
        assert!(wildcard_match("*x", "*yx"));
        assert!(wildcard_match("*.sync-conflict-*", "*a*.sync-conflict-1.md"));
    }

    /// Build a real archive and read it back: excludes prune, `*.ext` spares
    /// directories, symlinks (even dangling ones) are stored as links, and the
    /// root directory entry is present.
    #[test]
    fn archive_round_trip() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("Vault");
        fs::create_dir_all(src.join("Research.db")).unwrap();
        fs::create_dir_all(src.join("node_modules/pkg")).unwrap();
        fs::create_dir_all(src.join("Notes")).unwrap();
        fs::write(src.join("Research.db/thesis.md"), "keep me").unwrap();
        fs::write(src.join("state.db"), "drop me").unwrap();
        fs::write(src.join("node_modules/pkg/index.js"), "drop me").unwrap();
        fs::write(src.join("Notes/a.md"), "hello").unwrap();
        fs::write(src.join("Notes/a.sync-conflict-20260920-021511-X.md"), "drop me").unwrap();
        symlink("Notes/a.md", src.join("link.md")).unwrap();
        symlink("/nonexistent/target", src.join("dangling")).unwrap();

        let excludes: Vec<ExcludePattern> =
            ["*.db", "node_modules", "*.sync-conflict-*"].iter().map(|p| pat(p)).collect();
        let out = tmp.path().join("out.tar.gz");
        let (sha, _) = write_tar_gz(&src, &out, &excludes).unwrap();
        assert_eq!(sha256_path(&out).unwrap(), sha);

        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(File::open(&out).unwrap()));
        let mut entries: Vec<String> = Vec::new();
        let mut contents: HashMap<String, String> = HashMap::new();
        for e in archive.entries().unwrap() {
            let mut e = e.unwrap();
            let name = e.path().unwrap().to_string_lossy().trim_end_matches('/').to_string();
            let mut body = String::new();
            if e.header().entry_type().is_file() {
                e.read_to_string(&mut body).unwrap();
                contents.insert(name.clone(), body);
            }
            entries.push(name);
        }
        entries.sort();
        assert_eq!(
            entries,
            vec!["Vault", "Vault/Notes", "Vault/Notes/a.md", "Vault/Research.db", "Vault/Research.db/thesis.md", "Vault/dangling", "Vault/link.md"]
        );
        assert_eq!(contents["Vault/Research.db/thesis.md"], "keep me");
        assert_eq!(contents["Vault/Notes/a.md"], "hello");
    }
}
