//! `forever-ago jobs` — which scheduled backups cover this directory?
//!
//! forever-ago keeps no registry of its own: a "job" is just an invocation
//! sitting in some scheduler's config. So this reads each scheduler's own
//! source of truth — systemd unit files, crontabs, the PM2 dump, and running
//! daemons — and re-parses every invocation with the real `Cli`, so defaults
//! and flag semantics are exactly what an actual run would use.

use crate::{
    Cli, ExcludePattern, RetentionPolicy, human_size, parse_backup_date, read_exclude_file, retention_policy,
};
use anyhow::{Result, anyhow};
use clap::Parser;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

const BIN: &str = "forever-ago";

const SHELLS: &[&str] = &["sh", "bash", "zsh", "dash", "ksh", "fish"];

/// Note left on a process whose working directory we cannot read.
const UNKNOWN_CWD: &str = "its working directory is not readable by you";

/// A command that runs another command: `nice -n 10 forever-ago ...` is
/// still a forever-ago job. To find the command it runs, step over the
/// wrapper's options (`value_opts` take the next word), then `positionals`
/// words of its own (timeout's duration, flock's lock file). `command_opts`
/// take a whole shell command line (`flock -c "..."`), and `chdir_opts` move
/// the command's working directory.
struct Wrapper {
    name: &'static str,
    value_opts: &'static [&'static str],
    command_opts: &'static [&'static str],
    chdir_opts: &'static [&'static str],
    positionals: usize,
}

const fn wrapper(name: &'static str, value_opts: &'static [&'static str], positionals: usize) -> Wrapper {
    Wrapper { name, value_opts, command_opts: &[], chdir_opts: &[], positionals }
}

const WRAPPERS: &[Wrapper] = &[
    Wrapper {
        name: "env",
        value_opts: &["-u", "--unset", "-C", "--chdir", "-S", "--split-string"],
        command_opts: &["-S", "--split-string"],
        chdir_opts: &["-C", "--chdir"],
        positionals: 0,
    },
    wrapper("nice", &["-n", "--adjustment"], 0),
    wrapper("ionice", &["-c", "--class", "-n", "--classdata", "-p", "--pid", "-P", "--pgid", "-u", "--uid"], 0),
    wrapper("nohup", &[], 0),
    wrapper("setsid", &[], 0),
    wrapper("time", &["-f", "--format", "-o", "--output"], 0),
    wrapper("exec", &["-a"], 0),
    wrapper("command", &[], 0),
    wrapper("timeout", &["-s", "--signal", "-k", "--kill-after"], 1),
    Wrapper {
        name: "flock",
        value_opts: &["-w", "--wait", "--timeout", "-E", "--conflict-exit-code", "-c", "--command"],
        command_opts: &["-c", "--command"],
        chdir_opts: &[],
        positionals: 1,
    },
    wrapper("chrt", &["-T", "--sched-runtime", "-P", "--sched-period", "-D", "--sched-deadline"], 1),
    wrapper("taskset", &[], 1),
    wrapper("stdbuf", &["-i", "--input", "-o", "--output", "-e", "--error"], 0),
    wrapper("systemd-cat", &["-t", "--identifier", "-p", "--priority", "--stderr-priority"], 0),
    wrapper("systemd-inhibit", &["--what", "--who", "--why", "--mode"], 0),
    Wrapper {
        name: "sudo",
        value_opts: &[
            "-u", "--user", "-g", "--group", "-C", "--close-from", "-D", "--chdir", "-h", "--host", "-p",
            "--prompt", "-r", "--role", "-t", "--type", "-U", "--other-user", "-T", "--command-timeout",
        ],
        command_opts: &[],
        chdir_opts: &["-D", "--chdir"],
        positionals: 0,
    },
    wrapper("doas", &["-u", "-C"], 0),
    Wrapper {
        name: "runuser",
        value_opts: &["-u", "--user", "-g", "--group", "-G", "--supp-group", "-s", "--shell", "-c", "--command"],
        command_opts: &["-c", "--command"],
        chdir_opts: &[],
        positionals: 0,
    },
    Wrapper {
        name: "su",
        value_opts: &["-g", "--group", "-G", "--supp-group", "-s", "--shell", "-c", "--command", "-w"],
        command_opts: &["-c", "--command"],
        chdir_opts: &[],
        positionals: 1,
    },
    wrapper("chronic", &[], 0),
    wrapper("cronic", &[], 0),
];

#[derive(clap::Args, Debug)]
pub(crate) struct JobsArgs {
    /// List every job on this machine, not just the ones covering the current directory.
    #[arg(long)]
    all: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Scheduler {
    SystemdUser,
    SystemdSystem,
    Cron,
    Pm2,
    Daemon,
}

impl Scheduler {
    fn label(self) -> &'static str {
        match self {
            Scheduler::SystemdUser => "systemd --user",
            Scheduler::SystemdSystem => "systemd (system)",
            Scheduler::Cron => "cron",
            Scheduler::Pm2 => "pm2",
            Scheduler::Daemon => "running daemon (no scheduler manages it)",
        }
    }
}

/// A forever-ago command line found in some scheduler, before interpretation.
#[derive(Clone, Debug)]
struct Invocation {
    scheduler: Scheduler,
    name: String,
    defined_in: String,
    argv: Vec<String>,
    /// Directory the scheduler starts the process in; relative paths resolve here.
    cwd: PathBuf,
    /// Home of the user the job runs as; `~` and the default --dest-dir resolve here.
    home: PathBuf,
    /// What fires it, in the scheduler's own syntax (OnCalendar=..., a cron spec, ...).
    triggers: Vec<String>,
    trigger_notes: Vec<String>,
    /// Will the scheduler actually fire this? `None` = cannot tell.
    enabled: Option<bool>,
    /// Extra rows to display, e.g. pm2's saved status or systemd's next/last run.
    state: Vec<(String, String)>,
    /// systemd units (service first, then its timers) to ask systemctl about.
    units: Vec<String>,
    /// Caveats found while reading the scheduler's config (unexpanded specifiers, ...).
    notes: Vec<String>,
}

impl Invocation {
    fn new(scheduler: Scheduler, name: String, defined_in: String, argv: Vec<String>, cwd: PathBuf, home: PathBuf) -> Self {
        Self {
            scheduler,
            name,
            defined_in,
            argv,
            cwd,
            home,
            triggers: Vec::new(),
            trigger_notes: Vec::new(),
            enabled: None,
            state: Vec::new(),
            units: Vec::new(),
            notes: Vec::new(),
        }
    }
}

/// An invocation interpreted the way forever-ago itself would interpret it.
#[derive(Debug)]
pub(crate) struct Job {
    inv: Invocation,
    pub(crate) source: PathBuf,
    pub(crate) dest_dir: PathBuf,
    pub(crate) prefix: Option<String>,
    once: bool,
    run_now: bool,
    at: String,
    retention: Option<RetentionPolicy>,
    exclude_from: Option<PathBuf>,
    excludes: Vec<(String, ExcludePattern)>,
    problems: Vec<String>,
    pids: Vec<u32>,
}

impl Job {
    /// "forever-ago-vault.service (systemd --user)"
    pub(crate) fn describe(&self) -> String {
        format!("{} ({})", self.inv.name, self.inv.scheduler.label())
    }

    fn same_target(&self, other: &Job) -> bool {
        self.source == other.source && self.dest_dir == other.dest_dir && self.prefix == other.prefix
    }
}

/// Where discovery looks. Built from the host in `run`, from temp dirs in tests.
struct Sources {
    home: PathBuf,
    user_unit_dirs: Vec<PathBuf>,
    system_unit_dirs: Vec<PathBuf>,
    /// Output of `crontab -l` for the current user.
    crontab: Option<String>,
    /// /etc/crontab and /etc/cron.d/* — these carry a user field.
    system_crontabs: Vec<PathBuf>,
    pm2_dump: Option<PathBuf>,
    proc_root: Option<PathBuf>,
    passwd: PathBuf,
    /// Who is running `jobs`, for systemd's %u/%U and the user manager's $USER.
    user: String,
    uid: String,
    runtime_dir: PathBuf,
    host: String,
    /// Ask systemctl for next/last run and live unit state.
    live: bool,
}

impl Sources {
    fn from_host() -> Result<Self> {
        let home = dirs::home_dir().ok_or_else(|| anyhow!("could not resolve $HOME"))?;
        let xdg = |var: &str, fallback: &str| {
            std::env::var_os(var)
                .map(PathBuf::from)
                .filter(|p| p.is_absolute())
                .unwrap_or_else(|| home.join(fallback))
        };
        let config = xdg("XDG_CONFIG_HOME", ".config");
        let data = xdg("XDG_DATA_HOME", ".local/share");

        let crontab = Command::new("crontab")
            .arg("-l")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned());

        let mut system_crontabs = vec![PathBuf::from("/etc/crontab")];
        if let Ok(rd) = fs::read_dir("/etc/cron.d") {
            // cron itself skips names with anything but [A-Za-z0-9_-]
            // (forever-ago.disabled, foo.dpkg-old), so do the same.
            let mut more: Vec<PathBuf> = rd
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.is_file())
                .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(cron_reads))
                .collect();
            more.sort();
            system_crontabs.extend(more);
        }

        let pm2_home = std::env::var_os("PM2_HOME").map(PathBuf::from).unwrap_or_else(|| home.join(".pm2"));
        let uid = fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| s.lines().find_map(|l| l.strip_prefix("Uid:")).and_then(|ids| ids.split_whitespace().next().map(str::to_string)))
            .unwrap_or_default();
        let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(format!("/run/user/{uid}")));

        // systemd's own answer, including transient (systemd-run), runtime,
        // control and generator directories. The fallback is its documented order.
        let user_unit_dirs = unit_paths(true).unwrap_or_else(|| {
            let rt = runtime_dir.join("systemd");
            vec![
                config.join("systemd/user.control"),
                rt.join("user.control"),
                rt.join("transient"),
                rt.join("generator.early"),
                config.join("systemd/user"),
                PathBuf::from("/etc/systemd/user"),
                rt.join("user"),
                PathBuf::from("/run/systemd/user"),
                rt.join("generator"),
                data.join("systemd/user"),
                PathBuf::from("/usr/local/share/systemd/user"),
                PathBuf::from("/usr/share/systemd/user"),
                PathBuf::from("/usr/local/lib/systemd/user"),
                PathBuf::from("/usr/lib/systemd/user"),
                rt.join("generator.late"),
            ]
        });
        let system_unit_dirs = unit_paths(false).unwrap_or_else(|| {
            [
                "/etc/systemd/system.control",
                "/run/systemd/system.control",
                "/run/systemd/transient",
                "/run/systemd/generator.early",
                "/etc/systemd/system",
                "/etc/systemd/system.attached",
                "/run/systemd/system",
                "/run/systemd/system.attached",
                "/run/systemd/generator",
                "/usr/local/lib/systemd/system",
                "/usr/lib/systemd/system",
                "/run/systemd/generator.late",
            ]
            .map(PathBuf::from)
            .to_vec()
        });

        Ok(Self {
            user_unit_dirs,
            system_unit_dirs,
            user: std::env::var("USER").unwrap_or_default(),
            uid,
            runtime_dir,
            host: hostname(),
            crontab,
            system_crontabs,
            pm2_dump: Some(pm2_home.join("dump.pm2")),
            proc_root: Some(PathBuf::from("/proc")).filter(|p| p.is_dir()),
            passwd: PathBuf::from("/etc/passwd"),
            live: true,
            home,
        })
    }

    fn home_of(&self, user_or_uid: &str) -> Option<PathBuf> {
        let text = fs::read_to_string(&self.passwd).ok()?;
        text.lines().find_map(|line| {
            let f: Vec<&str> = line.split(':').collect();
            (f.len() >= 6 && (f[0] == user_or_uid || f[2] == user_or_uid)).then(|| PathBuf::from(f[5]))
        })
    }
}

/// `systemd-analyze [--user] unit-paths`: the search path in precedence order.
fn unit_paths(user: bool) -> Option<Vec<PathBuf>> {
    let mut cmd = Command::new("systemd-analyze");
    if user {
        cmd.arg("--user");
    }
    let out = cmd.arg("unit-paths").output().ok().filter(|o| o.status.success())?;
    let dirs: Vec<PathBuf> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(PathBuf::from)
        .collect();
    (!dirs.is_empty()).then_some(dirs)
}

/// Whether cron reads a file in /etc/cron.d with this name (run-parts rules).
fn cron_reads(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn hostname() -> String {
    ["/proc/sys/kernel/hostname", "/etc/hostname"]
        .iter()
        .find_map(|p| fs::read_to_string(p).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()))
        .unwrap_or_else(|| "localhost".into())
}

pub(crate) struct Discovery {
    pub(crate) home: PathBuf,
    pub(crate) jobs: Vec<Job>,
}

/// Every forever-ago job on this machine. `live` also asks systemctl for
/// next/last run, which only `jobs` displays.
pub(crate) fn discover_host(live: bool) -> Result<Discovery> {
    let mut src = Sources::from_host()?;
    src.live = live;
    let (jobs, warnings) = discover(&src);
    for w in &warnings {
        eprintln!("warning: {w}");
    }
    Ok(Discovery { home: src.home, jobs })
}

pub(crate) fn run(args: &JobsArgs) -> Result<()> {
    let found = discover_host(true)?;
    let cwd = std::env::current_dir()?;
    print!("{}", report(&found.jobs, &cwd, &found.home, args.all));
    Ok(())
}

/// Everything the user sees, as one string so tests can assert on it.
fn report(jobs: &[Job], cwd: &Path, home: &Path, all: bool) -> String {
    let mut out = String::new();
    if all {
        if jobs.is_empty() {
            out.push_str("no scheduled forever-ago jobs found on this machine\n");
            return out;
        }
        let mut groups: BTreeMap<&Path, Vec<&Job>> = BTreeMap::new();
        for job in jobs {
            groups.entry(job.source.as_path()).or_default().push(job);
        }
        let blocks: Vec<String> = groups
            .into_iter()
            .map(|(source, group)| render_group(source, &group, home))
            .collect();
        out.push_str(&blocks.join("\n"));
        return out;
    }

    let cwd = canonical_or_normalized(cwd);
    let stop = canonical_or_normalized(home);
    let coverage = covering(jobs, &cwd, &stop);
    let excluding = coverage.excluding_note(&cwd, home);
    match coverage.found {
        Some((dir, group)) => {
            if dir != cwd {
                out.push_str(&format!(
                    "no jobs back up {} itself; nearest ancestor with jobs: {}\n",
                    tilde(&cwd, home),
                    tilde(&dir, home)
                ));
            }
            out.push_str(&excluding);
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&render_group(&dir, &group, home));
        }
        None => {
            let limit = if cwd.starts_with(&stop) { tilde(&stop, home) } else { "/".to_string() };
            out.push_str(&format!(
                "no scheduled forever-ago jobs cover {} (searched it and every parent up to {limit})\n",
                tilde(&cwd, home)
            ));
            out.push_str(&excluding);
            if !jobs.is_empty() {
                out.push_str(&format!(
                    "{} job(s) back up other directories; `forever-ago jobs --all` lists them\n",
                    jobs.len()
                ));
            }
        }
    }
    out
}

pub(crate) struct Coverage<'a> {
    /// The nearest directory whose jobs really back `start` up, and those jobs.
    pub(crate) found: Option<(PathBuf, Vec<&'a Job>)>,
    /// Jobs passed on the way whose excludes leave `start` out, with the pattern.
    pub(crate) excluding: Vec<(&'a Job, &'a str)>,
}

impl Coverage<'_> {
    fn excluding_note(&self, cwd: &Path, home: &Path) -> String {
        excluding_note(&self.excluding, cwd, home)
    }
}

/// One line per job that was passed over because it excludes `cwd`.
pub(crate) fn excluding_note(excluding: &[(&Job, &str)], cwd: &Path, home: &Path) -> String {
    excluding
        .iter()
        .map(|(job, pattern)| {
            format!(
                "{} backs up {} but excludes {} (`{pattern}`)\n",
                job.describe(),
                tilde(&job.source, home),
                tilde(cwd, home)
            )
        })
        .collect()
}

/// Climb from `start` toward the root, stopping after `stop` (the home dir),
/// and return the first directory with a job that backs `start` up. A job
/// whose excludes leave `start` out does not count — its archives do not
/// contain it — so the climb continues past it.
pub(crate) fn covering<'a>(jobs: &'a [Job], start: &Path, stop: &Path) -> Coverage<'a> {
    let mut excluding = Vec::new();
    let found = climb(start, stop, |dir| {
        let mut hits = Vec::new();
        for job in jobs.iter().filter(|j| j.source == dir) {
            match excluded_by(job, start) {
                Some(pattern) => excluding.push((job, pattern)),
                None => hits.push(job),
            }
        }
        (!hits.is_empty()).then_some(hits)
    });
    Coverage { found, excluding }
}

/// Walk `start`, then each parent, until `found` answers or `stop` (the home
/// dir) has been checked. Outside `stop` the walk ends at the filesystem root.
pub(crate) fn climb<T>(start: &Path, stop: &Path, mut found: impl FnMut(&Path) -> Option<T>) -> Option<(PathBuf, T)> {
    let mut dir = Some(start);
    while let Some(d) = dir {
        if let Some(hit) = found(d) {
            return Some((d.to_path_buf(), hit));
        }
        if d == stop {
            break;
        }
        dir = d.parent();
    }
    None
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

fn discover(src: &Sources) -> (Vec<Job>, Vec<String>) {
    let mut invs = Vec::new();
    let mut warnings = Vec::new();

    invs.extend(systemd_invocations(src, Scheduler::SystemdUser));
    invs.extend(systemd_invocations(src, Scheduler::SystemdSystem));

    if let Some(text) = &src.crontab {
        invs.extend(parse_crontab(text, "crontab -l", false, src));
    }
    for path in &src.system_crontabs {
        if let Ok(text) = fs::read_to_string(path) {
            invs.extend(parse_crontab(&text, &path.display().to_string(), true, src));
        }
    }

    if let Some(dump) = &src.pm2_dump
        && let Ok(text) = fs::read_to_string(dump)
    {
        match parse_pm2_dump(&text, &dump.display().to_string(), &src.home) {
            Ok(found) => invs.extend(found),
            Err(err) => warnings.push(format!("ignoring unreadable pm2 dump {}: {err:#}", dump.display())),
        }
    }

    let mut jobs: Vec<Job> = invs.into_iter().filter_map(resolve).collect();

    // A running forever-ago is either a scheduled job in progress (attach its
    // pid) or a daemon someone started by hand — still a schedule, since the
    // daemon loop fires nightly on its own.
    if let Some(proc_root) = &src.proc_root {
        for (pid, inv) in scan_processes(proc_root, src) {
            let Some(mut job) = resolve(inv) else { continue };
            if let Some(owner) = jobs
                .iter_mut()
                .find(|j| j.inv.scheduler != Scheduler::Daemon && j.same_target(&job))
            {
                owner.pids.push(pid);
            } else if !job.once {
                job.pids.push(pid);
                job.inv.enabled = Some(true);
                jobs.push(job);
            }
        }
    }

    if src.live {
        apply_live_state(&mut jobs);
    }

    jobs.sort_by(|a, b| {
        (&a.source, a.inv.scheduler, &a.inv.name).cmp(&(&b.source, b.inv.scheduler, &b.inv.name))
    });
    (jobs, warnings)
}

/// Interpret an invocation with the real CLI. `None` means it is not a backup
/// run at all (`forever-ago jobs`, `--help`, `--version`), or it cannot be
/// tied to a directory.
fn resolve(inv: Invocation) -> Option<Job> {
    let cwd_unknown = inv.notes.iter().any(|n| n == UNKNOWN_CWD);
    let relative = |p: &Path| !p.is_absolute() && !p.starts_with("~");
    let resolve_in = |p: &Path| resolve_path(p, &inv.cwd, &inv.home);
    match Cli::try_parse_from(&inv.argv) {
        Ok(cli) => {
            if cli.command.is_some() || (cwd_unknown && relative(&cli.source)) {
                return None;
            }
            let mut problems = Vec::new();
            let source = resolve_in(&cli.source);
            let dest_dir = cli
                .dest_dir
                .as_deref()
                .map(resolve_in)
                .unwrap_or_else(|| inv.home.join("backups"));
            let exclude_from = cli.exclude_from.as_deref().map(resolve_in);
            let excludes = load_excludes(cli.excludes.clone(), exclude_from.as_deref(), &mut problems);
            let retention = match retention_policy(&cli) {
                Ok(r) => Some(r),
                Err(err) => {
                    problems.push(format!("{err:#}; the real run will refuse to start"));
                    None
                }
            };
            Some(Job {
                source,
                dest_dir,
                prefix: cli.prefix.clone(),
                once: cli.once,
                run_now: cli.run_now,
                at: cli.at.clone(),
                retention,
                exclude_from,
                excludes,
                problems,
                pids: Vec::new(),
                inv,
            })
        }
        Err(err) => {
            use clap::error::ErrorKind;
            if matches!(
                err.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            ) {
                return None;
            }
            // Flags this build does not know (an older or newer forever-ago).
            // A backup run names its source or prefix; anything else (a newer
            // subcommand, say) is not a job.
            let source = flag_value(&inv.argv, "--source");
            let prefix = flag_value(&inv.argv, "--prefix");
            if source.is_none() && prefix.is_none() {
                return None;
            }
            let source = source.unwrap_or_else(|| ".".into());
            if cwd_unknown && relative(Path::new(&source)) {
                return None;
            }
            let reason = err.to_string();
            let reason = reason.lines().next().unwrap_or("").trim_start_matches("error: ").to_string();
            let mut problems = vec![format!(
                "this build of forever-ago cannot parse the job's arguments ({reason}); details here are partial"
            )];
            let dest_dir = flag_value(&inv.argv, "--dest-dir")
                .map(|d| resolve_in(Path::new(&d)))
                .unwrap_or_else(|| inv.home.join("backups"));
            let exclude_from = flag_value(&inv.argv, "--exclude-from").map(|f| resolve_in(Path::new(&f)));
            let excludes = load_excludes(flag_values(&inv.argv, "--exclude"), exclude_from.as_deref(), &mut problems);
            Some(Job {
                source: resolve_in(Path::new(&source)),
                dest_dir,
                prefix,
                once: inv.argv.iter().any(|a| a == "--once"),
                run_now: inv.argv.iter().any(|a| a == "--run-now"),
                at: flag_value(&inv.argv, "--at").unwrap_or_else(|| "03:00".into()),
                retention: None,
                exclude_from,
                excludes,
                problems,
                pids: Vec::new(),
                inv,
            })
        }
    }
}

/// The job's excludes, parsed by the same rules the real run uses.
fn load_excludes(mut raw: Vec<String>, exclude_from: Option<&Path>, problems: &mut Vec<String>) -> Vec<(String, ExcludePattern)> {
    if let Some(path) = exclude_from {
        match read_exclude_file(path) {
            Ok(more) => raw.extend(more),
            Err(err) => {
                let kind = err.root_cause().downcast_ref::<std::io::Error>().map(std::io::Error::kind);
                problems.push(match kind {
                    // Not proof of a broken job: it may run as a user who can read it.
                    Some(std::io::ErrorKind::PermissionDenied) => format!(
                        "cannot read --exclude-from {} as you; its excludes are not shown",
                        path.display()
                    ),
                    _ => format!("{err:#}; the real run will fail here too"),
                });
            }
        }
    }
    raw.into_iter()
        .filter_map(|r| match ExcludePattern::parse(&r) {
            Ok(p) => Some((r, p)),
            Err(err) => {
                problems.push(format!("{err:#}; the real run will refuse to start"));
                None
            }
        })
        .collect()
}

fn flag_values(argv: &[String], flag: &str) -> Vec<String> {
    let eq = format!("{flag}=");
    let mut out = Vec::new();
    let mut i = 0;
    while i < argv.len() {
        if argv[i] == flag {
            out.extend(argv.get(i + 1).cloned());
            i += 2;
            continue;
        }
        if let Some(v) = argv[i].strip_prefix(&eq) {
            out.push(v.to_string());
        }
        i += 1;
    }
    out
}

fn flag_value(argv: &[String], flag: &str) -> Option<String> {
    let eq = format!("{flag}=");
    argv.iter().enumerate().find_map(|(i, a)| {
        if a == flag {
            argv.get(i + 1).cloned()
        } else {
            a.strip_prefix(&eq).map(str::to_string)
        }
    })
}

// --- systemd -----------------------------------------------------------------

struct Entry {
    section: String,
    key: String,
    value: String,
    /// Index into `UnitFile::files`: 0 is the unit file, the rest drop-ins.
    file: usize,
}

struct UnitFile {
    name: String,
    files: Vec<PathBuf>,
    entries: Vec<Entry>,
}

impl UnitFile {
    /// All values of a list-valued key, with the file each came from,
    /// honouring systemd's "empty assignment resets the list".
    fn list_from(&self, section: &str, key: &str) -> Vec<(String, usize)> {
        let mut out = Vec::new();
        for e in self.entries.iter().filter(|e| e.section == section && e.key == key) {
            if e.value.is_empty() {
                out.clear();
            } else {
                out.push((e.value.clone(), e.file));
            }
        }
        out
    }

    fn list(&self, section: &str, key: &str) -> Vec<String> {
        self.list_from(section, key).into_iter().map(|(v, _)| v).collect()
    }

    /// A single-valued key: the last assignment wins.
    fn last(&self, section: &str, key: &str) -> Option<String> {
        self.entries
            .iter()
            .rev()
            .find(|e| e.section == section && e.key == key)
            .map(|e| e.value.clone())
            .filter(|v| !v.is_empty())
    }

    fn mentions(&self, needle: &str) -> bool {
        self.entries.iter().any(|e| e.value.contains(needle))
    }
}

fn parse_unit_text(text: &str, file: usize, out: &mut Vec<Entry>) {
    let push = |section: &str, line: &str, out: &mut Vec<Entry>| {
        if let Some((k, v)) = line.split_once('=') {
            out.push(Entry { section: section.to_string(), key: k.trim().to_string(), value: v.trim().to_string(), file });
        }
    };

    let mut section = String::new();
    let mut pending: Option<String> = None;
    for raw in text.lines() {
        let line = raw.trim();
        if let Some(acc) = pending.as_mut() {
            // systemd ignores comment lines inside a continued value.
            if line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            acc.push(' ');
            if let Some(more) = line.strip_suffix('\\') {
                acc.push_str(more.trim());
                continue;
            }
            acc.push_str(line);
            let full = pending.take().unwrap_or_default();
            push(&section, &full, out);
            continue;
        }
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line[1..line.len() - 1].to_string();
            continue;
        }
        if let Some(head) = line.strip_suffix('\\') {
            pending = Some(head.trim_end().to_string());
            continue;
        }
        push(&section, line, out);
    }
    if let Some(full) = pending {
        push(&section, &full, out);
    }
}

/// `foo@bar.service` -> ("foo", Some("bar"), "service"); `foo.service` -> ("foo", None, "service").
fn split_unit_name(name: &str) -> Option<(&str, Option<&str>, &str)> {
    let (stem, kind) = name.rsplit_once('.')?;
    Some(match stem.split_once('@') {
        Some((prefix, instance)) => (prefix, Some(instance), kind),
        None => (stem, None, kind),
    })
}

/// One systemd search path (earlier directories win), indexed by file name.
struct UnitDirs<'a> {
    dirs: &'a [PathBuf],
    index: BTreeMap<String, PathBuf>,
}

impl<'a> UnitDirs<'a> {
    fn new(dirs: &'a [PathBuf]) -> Self {
        let mut index = BTreeMap::new();
        for dir in dirs {
            let Ok(rd) = fs::read_dir(dir) else { continue };
            for entry in rd.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if split_unit_name(&name).is_some() && !entry.path().is_dir() {
                    index.entry(name).or_insert_with(|| entry.path());
                }
            }
        }
        Self { dirs, index }
    }

    /// Every concrete unit of a kind worth looking at: plain units, plus
    /// template instances that something enables (`timers.target.wants/
    /// foo@bar.timer`). Templates themselves and alias symlinks are skipped;
    /// the alias's target is listed under its own name.
    fn names(&self, kind: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .index
            .iter()
            .filter(|(name, path)| {
                let Some((_, instance, k)) = split_unit_name(name) else { return false };
                let alias = fs::read_link(path).is_ok_and(|t| t.file_name().is_some_and(|f| f != name.as_str()) && t != Path::new("/dev/null"));
                k == kind && instance != Some("") && !alias
            })
            .map(|(name, _)| name.clone())
            .collect();
        for wants in self.wants_dirs() {
            let Ok(rd) = fs::read_dir(&wants) else { continue };
            for entry in rd.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if split_unit_name(&name).is_some_and(|(_, i, k)| k == kind && i.is_some_and(|i| !i.is_empty())) {
                    out.push(name);
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    fn wants_dirs(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for dir in self.dirs {
            let Ok(rd) = fs::read_dir(dir) else { continue };
            for entry in rd.flatten() {
                let n = entry.file_name().to_string_lossy().into_owned();
                if n.ends_with(".wants") || n.ends_with(".requires") {
                    out.push(entry.path());
                }
            }
        }
        out
    }

    /// Targets that pull `unit` in (`default.target.wants/unit`, ...).
    fn wanted_by(&self, unit: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .wants_dirs()
            .into_iter()
            .filter(|w| w.join(unit).exists())
            .filter_map(|w| {
                let n = w.file_name()?.to_string_lossy().into_owned();
                Some(n.trim_end_matches(".wants").trim_end_matches(".requires").to_string())
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// The unit as systemd would load it: the instance file if one exists,
    /// else its template; masked units are `None`; drop-ins are applied from
    /// the least specific directory (`service.d`, then `foo-.service.d` prefix
    /// dirs, then the template's, then the unit's own) with a same-named file
    /// in a more specific or earlier directory overriding the rest.
    fn load(&self, name: &str) -> Option<UnitFile> {
        let (prefix, instance, kind) = split_unit_name(name)?;
        let template = instance.map(|_| format!("{prefix}@.{kind}"));
        let path = self.index.get(name).or_else(|| template.as_ref().and_then(|t| self.index.get(t)))?;
        if fs::canonicalize(path).is_ok_and(|p| p == Path::new("/dev/null")) {
            return None;
        }
        let text = fs::read_to_string(path).ok()?;
        if text.trim().is_empty() {
            return None;
        }

        let mut levels = vec![format!("{kind}.d")];
        let dashes: Vec<usize> = prefix.match_indices('-').map(|(i, _)| i).collect();
        for i in dashes {
            levels.push(format!("{}-.{kind}.d", &prefix[..i]));
        }
        levels.extend(template.map(|t| format!("{t}.d")));
        levels.push(format!("{name}.d"));
        let mut dropins: BTreeMap<String, PathBuf> = BTreeMap::new();
        for level in &levels {
            for dir in self.dirs.iter().rev() {
                let Ok(rd) = fs::read_dir(dir.join(level)) else { continue };
                for entry in rd.flatten() {
                    let fname = entry.file_name().to_string_lossy().into_owned();
                    if fname.ends_with(".conf") {
                        dropins.insert(fname, entry.path());
                    }
                }
            }
        }

        let mut files = vec![path.clone()];
        let mut entries = Vec::new();
        parse_unit_text(&text, 0, &mut entries);
        for dropin in dropins.into_values() {
            if let Ok(text) = fs::read_to_string(&dropin) {
                parse_unit_text(&text, files.len(), &mut entries);
                files.push(dropin);
            }
        }
        Some(UnitFile { name: name.to_string(), files, entries })
    }
}

/// What systemd substitutes for `%x` in a unit, minus the per-unit parts.
struct ManagerInfo {
    user_scope: bool,
    /// %h / %u: the user running the *manager*. For the system manager that is
    /// root even when the unit says User= — systemd documents this explicitly.
    home: PathBuf,
    user: String,
    uid: String,
    runtime_dir: PathBuf,
    host: String,
}

/// systemd-escape in reverse: `-` is `/`, `\xNN` is a byte.
fn unescape_unit(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'-' => out.push(b'/'),
            b'\\' if bytes.get(i + 1) == Some(&b'x') => {
                if let Some(b) = s.get(i + 2..i + 4).and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    out.push(b);
                    i += 4;
                    continue;
                }
                out.push(b'\\');
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Expand `%x` specifiers; returns the text and any specifier it could not resolve.
fn expand_specifiers(s: &str, unit: &str, m: &ManagerInfo) -> (String, Vec<char>) {
    let (prefix, instance, _) = split_unit_name(unit).unwrap_or((unit, None, ""));
    let instance = instance.unwrap_or("");
    let stem = unit.rsplit_once('.').map_or(unit, |(stem, _)| stem);
    let last_dash = |p: &str| p.rsplit_once('-').map_or(p, |(_, tail)| tail).to_string();
    let (state, cache, logs, config) = if m.user_scope {
        let h = &m.home;
        (h.join(".local/state"), h.join(".cache"), h.join(".local/state/log"), h.join(".config"))
    } else {
        ("/var/lib".into(), "/var/cache".into(), "/var/log".into(), "/etc".into())
    };
    let mut out = String::with_capacity(s.len());
    let mut unknown = Vec::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(spec) = chars.next() else {
            out.push('%');
            break;
        };
        let value: Option<String> = match spec {
            '%' => Some("%".into()),
            'n' => Some(unit.into()),
            'N' => Some(stem.into()),
            'p' => Some(prefix.into()),
            'P' => Some(unescape_unit(prefix)),
            'i' => Some(instance.into()),
            'I' => Some(unescape_unit(instance)),
            'j' => Some(last_dash(prefix)),
            'J' => Some(unescape_unit(&last_dash(prefix))),
            'f' => Some(format!("/{}", unescape_unit(if instance.is_empty() { prefix } else { instance }))),
            'h' => Some(m.home.to_string_lossy().into_owned()),
            'u' => Some(m.user.clone()),
            'U' => Some(m.uid.clone()),
            'H' => Some(m.host.clone()),
            'l' => Some(m.host.split('.').next().unwrap_or("").into()),
            't' => Some(m.runtime_dir.to_string_lossy().into_owned()),
            'S' => Some(state.to_string_lossy().into_owned()),
            'C' => Some(cache.to_string_lossy().into_owned()),
            'L' => Some(logs.to_string_lossy().into_owned()),
            'E' => Some(config.to_string_lossy().into_owned()),
            'T' => Some("/tmp".into()),
            'V' => Some("/var/tmp".into()),
            _ => None,
        };
        match value {
            Some(v) => out.push_str(&v),
            None => {
                unknown.push(spec);
                out.push('%');
                out.push(spec);
            }
        }
    }
    (out, unknown)
}

/// `EnvironmentFile=`: KEY=VALUE lines, `#`/`;` comments, optional quotes.
fn read_env_file(path: &Path, vars: &mut HashMap<String, String>) {
    let Ok(text) = fs::read_to_string(path) else { return };
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            let v = v.trim();
            let v = v
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .or_else(|| v.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
                .unwrap_or(v);
            vars.insert(k.trim().to_string(), v.to_string());
        }
    }
}

const TIMER_KEYS: &[&str] = &["OnCalendar", "OnBootSec", "OnStartupSec", "OnActiveSec", "OnUnitActiveSec", "OnUnitInactiveSec"];
const PATH_KEYS: &[&str] = &["PathExists", "PathExistsGlob", "PathChanged", "PathModified", "DirectoryNotEmpty"];

fn systemd_invocations(src: &Sources, scheduler: Scheduler) -> Vec<Invocation> {
    let user_scope = scheduler == Scheduler::SystemdUser;
    let dirs = UnitDirs::new(if user_scope { &src.user_unit_dirs } else { &src.system_unit_dirs });
    let manager = ManagerInfo {
        user_scope,
        home: if user_scope { src.home.clone() } else { PathBuf::from("/root") },
        user: if user_scope { src.user.clone() } else { "root".into() },
        uid: if user_scope { src.uid.clone() } else { "0".into() },
        runtime_dir: if user_scope { src.runtime_dir.clone() } else { PathBuf::from("/run") },
        host: src.host.clone(),
    };

    // What starts each service: timers and path units (Unit=, defaulting to
    // the same name), including enabled instances of templated ones.
    let mut triggers_for: BTreeMap<String, Vec<UnitFile>> = BTreeMap::new();
    for kind in ["timer", "path"] {
        for name in dirs.names(kind) {
            let Some(unit) = dirs.load(&name) else { continue };
            let section = if kind == "timer" { "Timer" } else { "Path" };
            let target = unit
                .last(section, "Unit")
                .map(|u| expand_specifiers(&u, &name, &manager).0)
                .unwrap_or_else(|| format!("{}.service", name.trim_end_matches(&format!(".{kind}"))));
            triggers_for.entry(target).or_default().push(unit);
        }
    }

    let mut services = dirs.names("service");
    services.extend(triggers_for.keys().cloned());
    services.sort();
    services.dedup();

    let mut out = Vec::new();
    for name in services {
        let Some(svc) = dirs.load(&name) else { continue };
        if !svc.mentions(BIN) {
            continue;
        }
        let mut notes = Vec::new();
        let spec = |s: &str, notes: &mut Vec<String>| {
            let (text, unknown) = expand_specifiers(s, &name, &manager);
            for u in unknown {
                notes.push(format!("uses the systemd specifier %{u}, shown unexpanded"));
            }
            text
        };

        // The job's own user: what `~`, $HOME and the default --dest-dir mean.
        let run_as = if user_scope { None } else { svc.last("Service", "User").map(|u| spec(&u, &mut notes)) };
        let home = match &run_as {
            _ if user_scope => src.home.clone(),
            Some(u) => src.home_of(u).unwrap_or_else(|| PathBuf::from("/")),
            None => PathBuf::from("/root"),
        };

        // Environment the manager provides, then Environment=, then
        // EnvironmentFile= (which overrides, as in systemd).
        let mut vars: HashMap<String, String> = HashMap::new();
        if let Some(user) = if user_scope { Some(src.user.clone()) } else { run_as.clone() } {
            vars.insert("HOME".into(), home.to_string_lossy().into_owned());
            vars.insert("USER".into(), user.clone());
            vars.insert("LOGNAME".into(), user);
        }
        for assignment in svc.list("Service", "Environment") {
            for word in tokenize(&spec(&assignment, &mut notes)) {
                if let Some((k, v)) = word.split_once('=') {
                    vars.insert(k.to_string(), v.to_string());
                }
            }
        }
        for file in svc.list("Service", "EnvironmentFile") {
            let file = spec(file.trim_start_matches('-'), &mut notes);
            read_env_file(Path::new(&file), &mut vars);
        }

        let base_cwd = match svc.last("Service", "WorkingDirectory") {
            Some(wd) => {
                let wd = spec(wd.trim_start_matches('-'), &mut notes);
                if wd == "~" { home.clone() } else { PathBuf::from(wd) }
            }
            // systemd's default: the user's home for user managers, / for the system one.
            None if user_scope => home.clone(),
            None => PathBuf::from("/"),
        };

        let starts: Vec<(Found, usize)> = svc
            .list_from("Service", "ExecStart")
            .iter()
            .filter_map(|(line, file)| {
                let words = strip_exec_prefixes(split_words(&spec(line, &mut notes), false));
                find_invocation(&words, 0).map(|f| (f, *file))
            })
            .collect();
        let count = starts.len();

        let triggers = triggers_for.get(&name).map(Vec::as_slice).unwrap_or(&[]);
        let wanted_by = dirs.wanted_by(&name);
        for (n, (found, file)) in starts.into_iter().enumerate() {
            let label = if count > 1 { format!("{name} (ExecStart #{})", n + 1) } else { name.clone() };
            let mut notes = notes.clone();
            let argv = expand_words(&found.words, &vars, false, &mut notes);
            let cwd = apply_cds(&base_cwd, &found.cds, &home, &vars);
            let mut defined_in = svc.files[0].display().to_string();
            if file > 0 {
                defined_in.push_str(&format!(" (ExecStart from {})", svc.files[file].display()));
            }
            let mut inv = Invocation::new(scheduler, label, defined_in, argv, cwd, home.clone());
            inv.notes = notes;
            inv.units.push(name.clone());

            for trigger in triggers {
                inv.units.push(trigger.name.clone());
                let (section, keys) = if trigger.name.ends_with(".timer") { ("Timer", TIMER_KEYS) } else { ("Path", PATH_KEYS) };
                for key in keys {
                    for value in trigger.list(section, key) {
                        inv.triggers.push(format!("{key}={value}"));
                    }
                }
                if trigger.last("Timer", "Persistent").is_some_and(|v| is_truthy(&v)) {
                    inv.trigger_notes.push("catches up after downtime".into());
                }
                if let Some(delay) = trigger.last("Timer", "RandomizedDelaySec") {
                    inv.trigger_notes.push(format!("random delay up to {delay}"));
                }
            }
            if triggers.is_empty() {
                // No timer: a unit some target wants still runs, once per boot/login.
                for target in &wanted_by {
                    inv.triggers.push(format!("whenever {target} starts"));
                }
                inv.enabled = Some(!wanted_by.is_empty());
            } else {
                inv.enabled = Some(triggers.iter().any(|t| !dirs.wanted_by(&t.name).is_empty()));
            }
            out.push(inv);
        }
    }
    out
}

fn is_truthy(v: &str) -> bool {
    matches!(v.to_ascii_lowercase().as_str(), "1" | "yes" | "true" | "on")
}

/// `ExecStart=-/usr/bin/foo` and friends: strip systemd's exec prefixes. With
/// `@`, the second word is argv[0] rather than a real argument, so drop it.
fn strip_exec_prefixes(mut words: Vec<Word>) -> Vec<Word> {
    let Some(first) = words.first_mut() else { return words };
    let n = first.text.chars().take_while(|c| "@-:+!|".contains(*c)).count();
    let had_at = first.text[..n].contains('@');
    first.text.replace_range(..n, "");
    if had_at && words.len() > 1 {
        words.remove(1);
    }
    words
}

/// Ask systemctl for next/last run and whether the units are really live.
/// Best-effort: no systemd, no user manager, or an old systemctl just means
/// the offline answer stands.
fn apply_live_state(jobs: &mut [Job]) {
    for (scheduler, user_flag) in [(Scheduler::SystemdUser, true), (Scheduler::SystemdSystem, false)] {
        let units: Vec<String> = jobs
            .iter()
            .filter(|j| j.inv.scheduler == scheduler)
            .flat_map(|j| j.inv.units.iter().cloned())
            .collect();
        if units.is_empty() {
            continue;
        }
        let mut cmd = Command::new("systemctl");
        if user_flag {
            cmd.arg("--user");
        }
        cmd.args([
            "show",
            "--no-pager",
            "--property=Id,ActiveState,UnitFileState,NextElapseUSecRealtime,LastTriggerUSec,Result",
        ])
        .args(&units);
        let Ok(out) = cmd.output() else { continue };
        if !out.status.success() {
            continue;
        }
        let live = parse_systemctl_show(&String::from_utf8_lossy(&out.stdout));
        for job in jobs.iter_mut().filter(|j| j.inv.scheduler == scheduler) {
            apply_unit_state(&mut job.inv, &live);
        }
    }
}

fn parse_systemctl_show(text: &str) -> HashMap<String, HashMap<String, String>> {
    let mut out = HashMap::new();
    for block in text.split("\n\n") {
        let props: HashMap<String, String> = block
            .lines()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        if let Some(id) = props.get("Id").cloned() {
            out.insert(id, props);
        }
    }
    out
}

fn apply_unit_state(inv: &mut Invocation, live: &HashMap<String, HashMap<String, String>>) {
    let known = |v: Option<&String>| v.filter(|s| !s.is_empty() && *s != "n/a").cloned();
    let Some(service) = inv.units.first().cloned() else { return };
    let timers: Vec<String> = inv.units[1..].to_vec();
    let svc = live.get(&service);

    if timers.is_empty() {
        if let Some(svc) = svc {
            let active = svc.get("ActiveState").map(String::as_str);
            inv.enabled = Some(active == Some("active") || svc.get("UnitFileState").is_some_and(|s| s.starts_with("enabled")));
            inv.state.push(("state".into(), describe_unit(svc)));
        }
    } else {
        let mut any_active = false;
        for timer in &timers {
            let Some(t) = live.get(timer) else { continue };
            any_active |= t.get("ActiveState").is_some_and(|s| s == "active");
            inv.state.push(("timer".into(), format!("{timer}: {}", describe_unit(t))));
            if let Some(next) = known(t.get("NextElapseUSecRealtime")) {
                inv.state.push(("next run".into(), next));
            }
            if let Some(last) = known(t.get("LastTriggerUSec")) {
                let result = svc.and_then(|s| known(s.get("Result"))).unwrap_or_else(|| "unknown".into());
                inv.state.push(("last run".into(), format!("{last} ({result})")));
            }
        }
        if timers.iter().any(|t| live.contains_key(t)) {
            inv.enabled = Some(any_active);
        }
        if let Some(svc) = svc
            && matches!(svc.get("ActiveState").map(String::as_str), Some("activating" | "active"))
        {
            inv.state.push(("running".into(), format!("{service} is running right now")));
        }
    }
}

fn describe_unit(props: &HashMap<String, String>) -> String {
    let active = props.get("ActiveState").map_or("unknown", String::as_str);
    match props.get("UnitFileState").filter(|s| !s.is_empty()) {
        Some(file_state) => format!("{active} ({file_state})"),
        None => active.to_string(),
    }
}

// --- cron ----------------------------------------------------------------------

fn parse_crontab(text: &str, origin: &str, has_user_field: bool, src: &Sources) -> Vec<Invocation> {
    let mut vars: HashMap<String, String> = HashMap::new();
    let mut out = Vec::new();
    for (idx, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = cron_assignment(line) {
            vars.insert(k, v);
            continue;
        }
        if !line.contains(BIN) {
            continue;
        }
        let spec_fields = if line.starts_with('@') { 1 } else { 5 };
        let Some((mut fields, command)) = split_fields(line, spec_fields + usize::from(has_user_field)) else {
            continue;
        };
        let user = if has_user_field { fields.pop() } else { None };
        let passwd_home = match user {
            Some(u) => src.home_of(u).unwrap_or_else(|| PathBuf::from("/")),
            None => src.home.clone(),
        };
        let mut vars = vars.clone();
        vars.entry("HOME".into()).or_insert_with(|| passwd_home.to_string_lossy().into_owned());
        // cron starts every command in $HOME, which a `HOME=` line in the
        // crontab overrides, and the shell expands `~` from it too.
        let home = PathBuf::from(&vars["HOME"]);

        let Some(found) = find_invocation(&split_words(command, true), 0) else { continue };
        let mut notes = Vec::new();
        let argv = expand_words(&found.words, &vars, true, &mut notes);
        let cwd = apply_cds(&home, &found.cds, &home, &vars);
        let mut inv = Invocation::new(
            Scheduler::Cron,
            format!("{origin}, line {}", idx + 1),
            origin.to_string(),
            argv,
            cwd,
            home,
        );
        inv.notes = notes;
        inv.triggers.push(fields.join(" "));
        inv.enabled = Some(true);
        out.push(inv);
    }
    out
}

/// `NAME=value` lines in a crontab set the environment for the lines below.
fn cron_assignment(line: &str) -> Option<(String, String)> {
    let (k, v) = line.split_once('=')?;
    let k = k.trim();
    let is_ident = !k.is_empty()
        && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !k.starts_with(|c: char| c.is_ascii_digit());
    is_ident.then(|| (k.to_string(), v.trim().trim_matches(|c| c == '"' || c == '\'').to_string()))
}

/// Split off the first `n` whitespace-separated fields; the rest is the raw command.
fn split_fields(line: &str, n: usize) -> Option<(Vec<&str>, &str)> {
    let mut fields = Vec::with_capacity(n);
    let mut rest = line;
    for _ in 0..n {
        rest = rest.trim_start();
        let end = rest.find(char::is_whitespace)?;
        fields.push(&rest[..end]);
        rest = &rest[end..];
    }
    let rest = rest.trim();
    (!rest.is_empty()).then_some((fields, rest))
}

// --- pm2 -----------------------------------------------------------------------

/// `~/.pm2/dump.pm2` is what `pm2 save` wrote and `pm2 resurrect` restores,
/// readable without a working pm2 (or node) install.
fn parse_pm2_dump(text: &str, origin: &str, home: &Path) -> Result<Vec<Invocation>> {
    let procs: Vec<serde_json::Value> = serde_json::from_str(text)?;
    let mut out = Vec::new();
    for p in procs {
        let get = |k: &str| p.get(k).and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let Some(exec) = get("pm_exec_path").or_else(|| get("script")) else { continue };
        if basename(exec) != BIN {
            continue;
        }
        let mut argv = vec![exec.to_string()];
        match p.get("args") {
            Some(serde_json::Value::Array(items)) => {
                argv.extend(items.iter().filter_map(|v| v.as_str().map(str::to_string)));
            }
            Some(serde_json::Value::String(s)) => argv.extend(tokenize(s)),
            _ => {}
        }
        let cwd = get("pm_cwd").or_else(|| get("cwd")).map_or_else(|| home.to_path_buf(), PathBuf::from);
        let name = get("name").unwrap_or(BIN).to_string();
        let mut inv = Invocation::new(Scheduler::Pm2, name, origin.to_string(), argv, cwd, home.to_path_buf());
        // pm2 fires cron_restart even for a stopped app; "0" disables it.
        let cron = get("cron_restart").filter(|c| *c != "0");
        if let Some(cron) = cron {
            inv.triggers.push(format!("cron_restart {cron}"));
        }
        if let Some(status) = get("status") {
            inv.enabled = Some(status == "online" || cron.is_some());
            inv.state.push(("pm2 status".into(), format!("{status} (as of the last `pm2 save`)")));
        }
        out.push(inv);
    }
    Ok(out)
}

// --- running processes -----------------------------------------------------------

fn scan_processes(proc_root: &Path, src: &Sources) -> Vec<(u32, Invocation)> {
    let me = std::process::id();
    let Ok(rd) = fs::read_dir(proc_root) else { return Vec::new() };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else { continue };
        if pid == me {
            continue;
        }
        let dir = entry.path();
        let Ok(raw) = fs::read(dir.join("cmdline")) else { continue };
        let argv: Vec<String> = raw
            .split(|b| *b == 0)
            .filter(|s| !s.is_empty())
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect();
        if argv.first().is_none_or(|a| basename(a) != BIN) {
            continue;
        }
        let home = fs::read_to_string(dir.join("status"))
            .ok()
            .and_then(|s| {
                s.lines()
                    .find_map(|l| l.strip_prefix("Uid:"))
                    .and_then(|ids| ids.split_whitespace().next().map(str::to_string))
            })
            .and_then(|uid| src.home_of(&uid))
            .unwrap_or_else(|| src.home.clone());
        // Another user's daemon hides its cwd. Inventing one (their home)
        // would fabricate a job; resolve() drops it if a relative path needs it.
        let cwd_known = fs::read_link(dir.join("cwd")).ok();
        let cwd = cwd_known.clone().unwrap_or_default();
        let mut inv = Invocation::new(
            Scheduler::Daemon,
            format!("pid {pid}"),
            dir.join("cmdline").display().to_string(),
            argv,
            cwd,
            home,
        );
        inv.state.push(("pid".into(), pid.to_string()));
        if cwd_known.is_none() {
            inv.notes.push(UNKNOWN_CWD.into());
        }
        out.push((pid, inv));
    }
    out
}

// ---------------------------------------------------------------------------
// Command-line archaeology
// ---------------------------------------------------------------------------

/// One word of a command line. `quoted` words are never word-split when
/// expanded; `op` marks unquoted shell operators (`&&`, `;`, `(`, ...).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Word {
    text: String,
    quoted: bool,
    op: bool,
}

/// Split a command line into words: single and double quotes, backslash
/// escapes, operators even when glued (`cd /x;forever-ago`). `shell` adds what
/// only a shell does: `#` comments and `( ... )` subshells. `$(...)` and
/// backticks are kept verbatim inside their word.
fn split_words(s: &str, shell: bool) -> Vec<Word> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_word = false;
    let mut quoted = false;
    let mut chars = s.chars().peekable();

    fn flush(out: &mut Vec<Word>, cur: &mut String, in_word: &mut bool, quoted: &mut bool) {
        if *in_word {
            out.push(Word { text: std::mem::take(cur), quoted: *quoted, op: false });
        }
        *in_word = false;
        *quoted = false;
    }
    // Copy a `$( ... )` or `` `...` `` through its closing delimiter.
    fn substitution(chars: &mut std::iter::Peekable<std::str::Chars>, cur: &mut String, close: char) {
        let mut depth = 1;
        for c in chars.by_ref() {
            cur.push(c);
            if close == ')' && c == '(' {
                depth += 1;
            } else if c == close {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
        }
    }

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_word = true;
                quoted = true;
                for c in chars.by_ref() {
                    if c == '\'' {
                        break;
                    }
                    cur.push(c);
                }
            }
            '"' => {
                in_word = true;
                quoted = true;
                while let Some(c) = chars.next() {
                    match c {
                        '"' => break,
                        '\\' if matches!(chars.peek(), Some('"' | '\\' | '$' | '`')) => {
                            cur.extend(chars.next());
                        }
                        '$' if chars.peek() == Some(&'(') => {
                            cur.push(c);
                            cur.extend(chars.next());
                            substitution(&mut chars, &mut cur, ')');
                        }
                        '`' => {
                            cur.push(c);
                            substitution(&mut chars, &mut cur, '`');
                        }
                        _ => cur.push(c),
                    }
                }
            }
            '\\' => {
                in_word = true;
                cur.extend(chars.next());
            }
            '$' if chars.peek() == Some(&'(') => {
                in_word = true;
                cur.push(c);
                cur.extend(chars.next());
                substitution(&mut chars, &mut cur, ')');
            }
            '`' => {
                in_word = true;
                cur.push(c);
                substitution(&mut chars, &mut cur, '`');
            }
            '#' if shell && !in_word => break,
            // `2>&1` is a redirection, not a background `&`.
            '&' if cur.ends_with(['>', '<']) => cur.push(c),
            ';' | '&' | '|' | '(' | ')' if c != '(' && c != ')' || shell => {
                flush(&mut out, &mut cur, &mut in_word, &mut quoted);
                let mut op = c.to_string();
                if (c == '&' && chars.peek() == Some(&'&')) || (c == '|' && matches!(chars.peek(), Some('|' | '&'))) {
                    op.extend(chars.next());
                }
                out.push(Word { text: op, quoted: false, op: true });
            }
            c if c.is_whitespace() => flush(&mut out, &mut cur, &mut in_word, &mut quoted),
            _ => {
                in_word = true;
                cur.push(c);
            }
        }
    }
    flush(&mut out, &mut cur, &mut in_word, &mut quoted);
    out
}

/// Just the words, for values that are not commands (Environment=, pm2 args).
fn tokenize(s: &str) -> Vec<String> {
    split_words(s, false).into_iter().map(|w| w.text).collect()
}

fn basename(word: &str) -> &str {
    word.rsplit('/').next().unwrap_or(word)
}

fn is_redirect(word: &str) -> bool {
    let w = word.trim_start_matches(|c: char| c.is_ascii_digit());
    w.starts_with('>') || w.starts_with('<') || w.starts_with("&>")
}

fn is_assignment(word: &str) -> bool {
    word.split_once('=').is_some_and(|(k, _)| {
        !k.is_empty()
            && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !k.starts_with(|c: char| c.is_ascii_digit())
    })
}

/// Shell words that leave the next word in command position.
const SHELL_KEYWORDS: &[&str] = &["if", "then", "else", "elif", "do", "while", "until", "!", "{", "}"];

/// A forever-ago command found inside a command line: any `cd DIR` that runs
/// before it (so relative paths resolve correctly), and its words, cut off at
/// the first shell operator or redirection.
#[derive(Debug, PartialEq, Eq)]
struct Found {
    cds: Vec<String>,
    words: Vec<Word>,
}

/// Find the forever-ago command in a command line. Only a word in command
/// position counts: the first word, after an operator or keyword, after
/// `VAR=x` assignments, or the command a wrapper like `nice`/`flock` runs.
/// So `restic backup ~/code/forever-ago` and `flock /run/lock/forever-ago
/// restic ...` are not mistaken for jobs.
fn find_invocation(words: &[Word], depth: u8) -> Option<Found> {
    if depth > 3 {
        return None;
    }
    let mut cds: Vec<String> = Vec::new();
    let mut command_position = true;
    let mut i = 0;
    while i < words.len() {
        let w = &words[i];
        if w.op {
            command_position = true;
            i += 1;
            continue;
        }
        if !command_position {
            i += 1;
            continue;
        }
        if (!w.quoted && SHELL_KEYWORDS.contains(&w.text.as_str())) || is_assignment(&w.text) {
            i += 1;
            continue;
        }
        let name = basename(&w.text);
        if name == BIN && !w.text.ends_with('/') {
            let words = words[i..]
                .iter()
                .take_while(|a| !a.op && !(is_redirect(&a.text) && !a.quoted))
                .cloned()
                .collect();
            return Some(Found { cds, words });
        }
        if name == "cd" {
            cds.push(words.get(i + 1).filter(|a| !a.op).map_or_else(|| "~".into(), |a| a.text.clone()));
        }
        if SHELLS.contains(&name) {
            let script = words[i + 1..]
                .iter()
                .take_while(|a| !a.op)
                .position(|a| a.text == "-c" || (a.text.starts_with('-') && !a.text.starts_with("--") && a.text.ends_with('c')))
                .and_then(|p| words.get(i + 1 + p + 1));
            if let Some(script) = script
                && let Some(found) = find_invocation(&split_words(&script.text, true), depth + 1)
            {
                cds.extend(found.cds);
                return Some(Found { cds, words: found.words });
            }
        }
        if let Some(wrapper) = WRAPPERS.iter().find(|wr| wr.name == name) {
            match step_over(wrapper, words, i + 1, depth, &mut cds) {
                Step::Command(next) => {
                    i = next; // still in command position
                    continue;
                }
                Step::Found(found) => {
                    cds.extend(found.cds);
                    return Some(Found { cds, words: found.words });
                }
                Step::Nothing => {}
            }
        }
        command_position = false;
        i += 1;
    }
    None
}

enum Step {
    /// Index of the command the wrapper runs.
    Command(usize),
    /// The wrapper runs a whole command line (`flock -c "..."`) and forever-ago is in it.
    Found(Found),
    Nothing,
}

fn step_over(wrapper: &Wrapper, words: &[Word], mut i: usize, depth: u8, cds: &mut Vec<String>) -> Step {
    let mut positionals = wrapper.positionals;
    while i < words.len() && !words[i].op {
        let w = words[i].text.as_str();
        if w == "--" {
            return Step::Command(i + 1);
        }
        // --opt=value
        let (opt, inline) = match w.split_once('=') {
            Some((o, v)) if o.starts_with("--") => (o, Some(v)),
            _ => (w, None),
        };
        if opt.starts_with('-') {
            let takes_value = wrapper.value_opts.contains(&opt);
            let value = inline.map(str::to_string).or_else(|| takes_value.then(|| words.get(i + 1).map(|v| v.text.clone())).flatten());
            if let Some(value) = &value {
                if wrapper.command_opts.contains(&opt) {
                    if let Some(found) = find_invocation(&split_words(value, true), depth + 1) {
                        return Step::Found(found);
                    }
                    return Step::Nothing;
                }
                if wrapper.chdir_opts.contains(&opt) {
                    cds.push(value.clone());
                }
            }
            i += if takes_value && inline.is_none() { 2 } else { 1 };
            continue;
        }
        if wrapper.name == "env" && is_assignment(w) {
            i += 1;
            continue;
        }
        if positionals > 0 {
            positionals -= 1;
            i += 1;
            continue;
        }
        return Step::Command(i);
    }
    Step::Nothing
}

fn expand_vars(word: &str, vars: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(word.len());
    let mut rest = word;
    while let Some(pos) = rest.find('$') {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + 1..];
        let (name, consumed) = if let Some(inner) = after.strip_prefix('{') {
            match inner.find('}') {
                Some(end) => (&inner[..end], end + 2),
                None => ("", 0),
            }
        } else {
            let end = after
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(after.len());
            (&after[..end], end)
        };
        match vars.get(name).filter(|_| !name.is_empty()) {
            Some(value) => out.push_str(value),
            None => out.push_str(&rest[pos..pos + 1 + consumed]),
        }
        rest = &after[consumed..];
    }
    out.push_str(rest);
    out
}

/// Expand variables in a found command the way its runner would. An unquoted
/// word that is exactly `$VAR` (or `${VAR}` in a shell) becomes several
/// arguments; anything left unexpandable is noted rather than guessed at.
fn expand_words(words: &[Word], vars: &HashMap<String, String>, shell: bool, notes: &mut Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for w in words {
        if !w.quoted
            && let Some(name) = whole_var(&w.text, shell)
            && let Some(value) = vars.get(name)
        {
            out.extend(tokenize(value));
            continue;
        }
        let text = expand_hostname(&expand_vars(&w.text, vars));
        if text.contains("$(") || text.contains('`') {
            notes.push(format!("`{text}` uses command substitution, shown unexpanded"));
        } else if has_unresolved_var(&text) {
            notes.push(format!("`{text}` uses a variable only set at run time, shown unexpanded"));
        }
        out.push(text);
    }
    out
}

/// `$NAME`, or `${NAME}` in a shell (systemd keeps braced variables as one word).
fn whole_var(text: &str, shell: bool) -> Option<&str> {
    let rest = text.strip_prefix('$')?;
    let name = match rest.strip_prefix('{') {
        Some(inner) if shell => inner.strip_suffix('}')?,
        Some(_) => return None,
        None => rest,
    };
    (!name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')).then_some(name)
}

fn has_unresolved_var(text: &str) -> bool {
    text.match_indices('$')
        .any(|(i, _)| text[i + 1..].starts_with(|c: char| c == '{' || c == '_' || c.is_ascii_alphabetic()))
}

/// The one command substitution worth resolving: the README's own
/// `--prefix name-$(hostname)`.
fn expand_hostname(text: &str) -> String {
    if !text.contains("hostname") && !text.contains("uname -n") {
        return text.to_string();
    }
    let host = hostname();
    let short = host.split('.').next().unwrap_or(&host).to_string();
    let mut out = text.to_string();
    for (pattern, value) in [
        ("$(hostname -s)", &short),
        ("`hostname -s`", &short),
        ("$(hostname)", &host),
        ("`hostname`", &host),
        ("$(uname -n)", &host),
        ("`uname -n`", &host),
    ] {
        out = out.replace(pattern, value);
    }
    out
}

fn apply_cds(base: &Path, cds: &[String], home: &Path, vars: &HashMap<String, String>) -> PathBuf {
    let mut cwd = base.to_path_buf();
    for dir in cds {
        cwd = resolve_path(Path::new(&expand_vars(dir, vars)), &cwd, home);
    }
    cwd
}

/// Resolve a path the way the job's own process would: `~` is the job user's
/// home, relative paths hang off the job's working directory.
fn resolve_path(path: &Path, cwd: &Path, home: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    let expanded = if s == "~" {
        home.to_path_buf()
    } else if let Some(rest) = s.strip_prefix("~/") {
        home.join(rest)
    } else {
        path.to_path_buf()
    };
    let absolute = if expanded.is_absolute() { expanded } else { cwd.join(expanded) };
    canonical_or_normalized(&absolute)
}

/// Canonical when the path exists (so symlinked spellings compare equal),
/// otherwise lexically cleaned — a job whose source is gone still shows up.
pub(crate) fn canonical_or_normalized(path: &Path) -> PathBuf {
    if let Ok(p) = fs::canonicalize(path) {
        return p;
    }
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// `~/x` for paths under home, whether spelled through a symlinked $HOME or
/// its canonical target.
pub(crate) fn tilde(path: &Path, home: &Path) -> String {
    let canonical = canonical_or_normalized(home);
    match path.strip_prefix(home).or_else(|_| path.strip_prefix(&canonical)) {
        Ok(rest) if rest.as_os_str().is_empty() => "~".to_string(),
        Ok(rest) => format!("~/{}", rest.display()),
        Err(_) => path.display().to_string(),
    }
}

fn render_group(source: &Path, jobs: &[&Job], home: &Path) -> String {
    let mut out = format!(
        "{}  ({} job{})\n",
        tilde(source, home),
        jobs.len(),
        if jobs.len() == 1 { "" } else { "s" }
    );
    for job in jobs {
        out.push('\n');
        out.push_str(&render_job(job, home));
    }
    out
}

fn render_job(job: &Job, home: &Path) -> String {
    let inv = &job.inv;
    let mut rows: Vec<(String, String)> = Vec::new();
    // Caveats first: they qualify everything below them.
    for p in inv.notes.iter().chain(&job.problems) {
        rows.push(("problem".into(), p.clone()));
    }

    let mut schedule: Vec<String> = Vec::new();
    if !inv.triggers.is_empty() {
        let mut s = inv.triggers.join(", ");
        if !inv.trigger_notes.is_empty() {
            s.push_str(&format!("  ({})", inv.trigger_notes.join("; ")));
        }
        schedule.push(s);
    }
    if !job.once {
        let mut s = format!("daily at {} by forever-ago's own loop", job.at);
        if job.run_now {
            s.push_str(", plus once at startup");
        }
        schedule.push(s);
    }
    if schedule.is_empty() {
        schedule.push("NONE: a one-shot (--once) that nothing triggers; runs only when started by hand".into());
    }
    for s in schedule {
        rows.push(("schedule".into(), s));
    }

    if inv.enabled == Some(false) {
        let why = match inv.scheduler {
            Scheduler::Pm2 => "pm2 has it stopped",
            Scheduler::SystemdUser | Scheduler::SystemdSystem => "its unit is not enabled/active",
            _ => "the scheduler has it disabled",
        };
        if !job.once && !job.pids.is_empty() {
            // The daemon loop is the schedule, and it is running right now.
            rows.push((
                "note".into(),
                format!("{why}, but the running daemon still backs up daily at {}; it will not come back after it stops", job.at),
            ));
        } else {
            rows.push(("WARNING".into(), format!("will not fire: {why}")));
        }
    }
    rows.extend(inv.state.iter().cloned());
    if !job.pids.is_empty() && inv.scheduler != Scheduler::Daemon {
        let pids: Vec<String> = job.pids.iter().map(u32::to_string).collect();
        rows.push(("running".into(), format!("now, pid {}", pids.join(", "))));
    }

    let prefix = job.prefix.as_deref().unwrap_or("<missing --prefix>");
    rows.push(("writes".into(), format!("{}/{prefix}-YYYY-MM-DD.tar.gz", tilde(&job.dest_dir, home))));
    if let Some(retention) = &job.retention {
        let r = match retention {
            RetentionPolicy::Count(n) => format!("newest {n}"),
            RetentionPolicy::Gfs { daily, weekly, monthly } => {
                format!("GFS {daily} daily / {weekly} weekly / {monthly} monthly")
            }
        };
        rows.push(("retention".into(), r));
    }
    let excludes = match (&job.exclude_from, job.excludes.len()) {
        (Some(file), n) => format!("{n} pattern{} incl. {}", if n == 1 { "" } else { "s" }, tilde(file, home)),
        (None, 0) => "none".to_string(),
        (None, n) => format!("{n} pattern{}", if n == 1 { "" } else { "s" }),
    };
    rows.push(("excludes".into(), excludes));
    if let Some(prefix) = &job.prefix {
        rows.push(("on disk".into(), describe_backups(&job.dest_dir, prefix)));
    }
    rows.push(("defined in".into(), tilde(Path::new(&inv.defined_in), home)));

    let mut out = format!("  {}  [{}]\n", inv.name, inv.scheduler.label());
    for (label, value) in rows {
        out.push_str(&format!("    {label:<11}{value}\n"));
    }
    out
}

/// If `cwd` (inside the job's source) is pruned by an exclude, which pattern did it.
pub(crate) fn excluded_by<'a>(job: &'a Job, cwd: &Path) -> Option<&'a str> {
    let rel = cwd.strip_prefix(&job.source).ok()?;
    // Check every ancestor too: an excluded directory takes its whole subtree with it.
    let mut prefix = PathBuf::new();
    for part in rel.components() {
        prefix.push(part);
        if let Some((raw, _)) = job.excludes.iter().find(|(_, p)| p.matches(&prefix, true)) {
            return Some(raw.as_str());
        }
    }
    None
}

fn describe_backups(dest_dir: &Path, prefix: &str) -> String {
    let rd = match fs::read_dir(dest_dir) {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            return "cannot read the destination as you; not checked".to_string();
        }
        Err(_) => return "nothing yet (destination does not exist)".to_string(),
    };
    let mut backups: Vec<(chrono::NaiveDate, String, u64)> = rd
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let date = parse_backup_date(prefix, &name)?;
            Some((date, name, e.metadata().map(|m| m.len()).unwrap_or(0)))
        })
        .collect();
    backups.sort();
    match backups.last() {
        None => "no backups yet".to_string(),
        Some((_, name, size)) => format!(
            "{} backup{}, newest {name} ({})",
            backups.len(),
            if backups.len() == 1 { "" } else { "s" },
            human_size(*size)
        ),
    }
}

/// A job as if found in a crontab, for other modules' tests.
#[cfg(test)]
pub(crate) fn test_job(argv: &[&str], cwd: &Path, home: &Path) -> Job {
    let argv = argv.iter().map(|s| s.to_string()).collect();
    let inv = Invocation::new(Scheduler::Cron, "test-job".into(), "test".into(), argv, cwd.into(), home.into());
    resolve(inv).expect("a backup invocation")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn w(s: &str) -> Vec<Word> {
        split_words(s, true)
    }

    fn texts(words: &[Word]) -> Vec<&str> {
        words.iter().map(|w| w.text.as_str()).collect()
    }

    /// The argv forever-ago would be found with in a shell command line.
    fn found(s: &str) -> Option<Vec<String>> {
        find_invocation(&w(s), 0).map(|f| f.words.into_iter().map(|w| w.text).collect())
    }

    struct Fixture {
        _tmp: tempfile::TempDir,
        root: PathBuf,
        home: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let root = fs::canonicalize(tmp.path()).unwrap();
            let home = root.join("home/me");
            fs::create_dir_all(&home).unwrap();
            Self { _tmp: tmp, root, home }
        }

        fn write(&self, rel: &str, body: &str) -> PathBuf {
            let path = self.root.join(rel);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, body).unwrap();
            path
        }

        fn mkdir(&self, rel: &str) -> PathBuf {
            let path = self.root.join(rel);
            fs::create_dir_all(&path).unwrap();
            path
        }

        fn link(&self, target: impl AsRef<Path>, rel: &str) {
            let path = self.root.join(rel);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            symlink(target, path).unwrap();
        }

        fn sources(&self) -> Sources {
            Sources {
                home: self.home.clone(),
                user_unit_dirs: vec![self.root.join("user-units"), self.root.join("vendor-user-units")],
                system_unit_dirs: vec![self.root.join("system-units")],
                crontab: None,
                system_crontabs: Vec::new(),
                pm2_dump: None,
                proc_root: None,
                passwd: self.write("etc/passwd", "root:x:0:0::/root:/bin/sh\nbob:x:1001:1001::/srv/bob:/bin/sh\n"),
                user: "me".into(),
                uid: "1000".into(),
                runtime_dir: self.root.join("run/user/1000"),
                host: "box.example.com".into(),
                live: false,
            }
        }

        fn jobs(&self) -> Vec<Job> {
            discover(&self.sources()).0
        }

        fn job_with(&self, source: &str, extra: &[&str]) -> Job {
            let mut argv = vec!["forever-ago", "--source", source, "--prefix", "p", "--once"];
            argv.extend(extra);
            test_job(&argv, &self.home, &self.home)
        }
    }

    // --- tokenizing and finding the command --------------------------------

    #[test]
    fn split_words_handles_quotes_escapes_operators_and_comments() {
        assert_eq!(
            texts(&w(r#"forever-ago --source "/a b/c" --prefix 'x y' --dest-dir /d\ e"#)),
            vec!["forever-ago", "--source", "/a b/c", "--prefix", "x y", "--dest-dir", "/d e"]
        );
        assert_eq!(
            texts(&w("cd /x;forever-ago&&b||c 2>&1 'a;b' # trailing comment")),
            vec!["cd", "/x", ";", "forever-ago", "&&", "b", "||", "c", "2>&1", "a;b"]
        );
        assert_eq!(texts(&w("(cd /x && y)")), vec!["(", "cd", "/x", "&&", "y", ")"]);
        assert_eq!(texts(&w("--prefix v-$(hostname -s) `date`")), vec!["--prefix", "v-$(hostname -s)", "`date`"]);
        // systemd values are not shell: `#` and parens are literal there.
        assert_eq!(tokenize("a#b (c)"), vec!["a#b", "(c)"]);
        let quoted = w(r#""$ARGS" $ARGS"#);
        assert!(quoted[0].quoted && !quoted[1].quoted);
    }

    #[test]
    fn unit_parser_joins_continuations_and_skips_comments_inside_them() {
        let mut e = Vec::new();
        parse_unit_text(
            "[Service]\n# c\nExecStart=/bin/forever-ago \\\n    --prefix v \\\n# ignored\n    --once\nUser=bob\n",
            0,
            &mut e,
        );
        assert_eq!((e[0].key.as_str(), e[0].value.as_str()), ("ExecStart", "/bin/forever-ago --prefix v --once"));
        assert_eq!((e[1].key.as_str(), e[1].value.as_str()), ("User", "bob"));
    }

    #[test]
    fn finds_invocation_only_in_command_position() {
        assert_eq!(found("forever-ago --prefix v --once").unwrap()[0], "forever-ago");
        assert_eq!(found("FOO=1 nice -n 10 /opt/forever-ago --prefix v").unwrap()[0], "/opt/forever-ago");
        assert_eq!(
            found("cd /x && forever-ago --prefix v >> /var/log/fa.log 2>&1").unwrap(),
            vec!["forever-ago", "--prefix", "v"]
        );
        assert_eq!(found("/bin/sh -lc 'cd /x; forever-ago --prefix v'").unwrap()[0], "forever-ago");
        assert_eq!(found("if true; then forever-ago --prefix v; fi").unwrap()[0], "forever-ago");
        assert!(found("notify-send 'forever-ago failed'").is_none());
        assert!(found("restic backup /home/me/code/forever-ago/").is_none());
        assert!(found("env FOO=/x/forever-ago restic backup").is_none());
    }

    /// Arguments of a wrapped command are not commands, even when they end in
    /// /forever-ago; the command a wrapper runs is found by its real grammar.
    #[test]
    fn wrappers_are_stepped_over_by_their_grammar() {
        assert!(found("nice -n 19 restic backup ~/code/forever-ago").is_none());
        assert!(found("timeout 1h rsync -a $HOME/code/forever-ago /mnt/usb").is_none());
        assert!(found("flock /run/lock/forever-ago restic backup /x").is_none());
        assert_eq!(
            found("systemd-cat -t forever-ago /usr/bin/forever-ago --prefix v").unwrap(),
            vec!["/usr/bin/forever-ago", "--prefix", "v"]
        );
        assert_eq!(found("sudo -u bob -- forever-ago --prefix v").unwrap()[0], "forever-ago");
        assert_eq!(found("ionice -c 3 nice forever-ago --prefix v").unwrap()[0], "forever-ago");
        assert_eq!(found("mise exec -- forever-ago --prefix v"), None, "mise is not a known wrapper");
    }

    #[test]
    fn shells_behind_wrappers_are_descended() {
        let f = find_invocation(&w("flock -n /tmp/l sh -c 'cd /x && forever-ago --prefix v'"), 0).unwrap();
        assert_eq!(f.cds, vec!["/x"]);
        assert_eq!(found("timeout 1h bash -c \"forever-ago --prefix v\"").unwrap()[0], "forever-ago");
        assert_eq!(found("flock /tmp/l -c 'forever-ago --prefix v'").unwrap()[0], "forever-ago");
        let f = find_invocation(&w("env -C /data forever-ago --prefix v"), 0).unwrap();
        assert_eq!(f.cds, vec!["/data"]);
        let f = find_invocation(&w("( cd /x && forever-ago --prefix v )"), 0).unwrap();
        assert_eq!(f.cds, vec!["/x"]);
    }

    /// Classifying a word must never look at the jobs process's own cwd: a
    /// `forever-ago/` checkout next to you does not make `forever-ago` a directory.
    #[test]
    fn bare_name_is_a_command_wherever_jobs_runs() {
        let fx = Fixture::new();
        fx.mkdir("home/me/forever-ago"); // the repo checkout, say
        let words = w("forever-ago --source vault --prefix v --once");
        // Same answer no matter what exists on disk.
        assert!(find_invocation(&words, 0).is_some());
    }

    #[test]
    fn expansion_splits_bare_vars_and_resolves_hostname() {
        let vars = HashMap::from([("ARGS".to_string(), "--source /x --prefix v".to_string()), ("HOME".to_string(), "/h".to_string())]);
        let mut notes = Vec::new();
        let argv = expand_words(&w("forever-ago $ARGS --dest-dir ${HOME}/b"), &vars, true, &mut notes);
        assert_eq!(argv, vec!["forever-ago", "--source", "/x", "--prefix", "v", "--dest-dir", "/h/b"]);
        // systemd keeps `${VAR}` as one word; quotes always do.
        let argv = expand_words(&split_words("forever-ago ${ARGS} \"$ARGS\"", false), &vars, false, &mut notes);
        assert_eq!(argv, vec!["forever-ago", "--source /x --prefix v", "--source /x --prefix v"]);
        assert!(notes.is_empty(), "{notes:?}");

        let argv = expand_words(&w("--prefix v-$(hostname) --at $(date +%H:00) $UNSET"), &vars, true, &mut notes);
        assert_eq!(argv[1], format!("v-{}", hostname()));
        assert!(notes.iter().any(|n| n.contains("command substitution")), "{notes:?}");
        assert!(notes.iter().any(|n| n.contains("only set at run time")), "{notes:?}");
    }

    // --- systemd -------------------------------------------------------------

    #[test]
    fn systemd_service_with_timer_is_discovered() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        fx.write(
            "user-units/fa-vault.service",
            "[Service]\nType=oneshot\nEnvironment=\"DEST=%h/backups/v\"\n\
             ExecStart=-/opt/bin/forever-ago \\\n  --source %h/vault \\\n  --dest-dir ${DEST} --prefix vault \\\n  --keep-daily 7 --once\n",
        );
        fx.write("user-units/fa-vault.timer", "[Timer]\nOnCalendar=*-*-* 03:00:00\nPersistent=true\nRandomizedDelaySec=15m\n");
        fx.link(fx.root.join("user-units/fa-vault.timer"), "user-units/timers.target.wants/fa-vault.timer");
        // Mentions forever-ago, but does not run it.
        fx.write("user-units/fa-alert.service", "[Service]\nExecStart=/usr/bin/notify-send \"forever-ago failed\"\n");

        let jobs = fx.jobs();
        assert_eq!(jobs.len(), 1, "{jobs:#?}");
        let job = &jobs[0];
        assert_eq!(job.inv.name, "fa-vault.service");
        assert_eq!(job.source, vault);
        assert_eq!(job.dest_dir, fx.home.join("backups/v"));
        assert_eq!(job.inv.triggers, vec!["OnCalendar=*-*-* 03:00:00"]);
        assert_eq!(job.inv.enabled, Some(true));
        assert_eq!(job.inv.cwd, fx.home);
        assert!(matches!(job.retention, Some(RetentionPolicy::Gfs { daily: 7, weekly: 4, monthly: 4 })));
    }

    #[test]
    fn template_instances_are_discovered() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        fx.write("user-units/fa@.service", "[Service]\nExecStart=/opt/forever-ago --source %h/%i --prefix %i --once\n");
        fx.write("user-units/fa@.timer", "[Timer]\nOnCalendar=daily\n");
        fx.link("../fa@.timer", "user-units/timers.target.wants/fa@vault.timer");
        let jobs = fx.jobs();
        assert_eq!(jobs.len(), 1, "{jobs:#?}");
        assert_eq!(jobs[0].inv.name, "fa@vault.service");
        assert_eq!(jobs[0].source, vault);
        assert_eq!(jobs[0].prefix.as_deref(), Some("vault"));
        assert_eq!(jobs[0].inv.triggers, vec!["OnCalendar=daily"]);
        assert_eq!(jobs[0].inv.enabled, Some(true));
    }

    #[test]
    fn manager_env_and_environment_files_feed_expansion() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        fx.write("home/me/.fa.env", "# settings\nPREFIX=\"vault\"\nARGS=--keep-daily 3 --once\n");
        fx.write(
            "user-units/fa.service",
            "[Service]\nEnvironmentFile=-%h/.fa.env\nEnvironmentFile=-%h/missing.env\n\
             ExecStart=forever-ago --source ${HOME}/vault --prefix ${PREFIX} $ARGS\n",
        );
        let jobs = fx.jobs();
        assert_eq!(jobs[0].source, vault);
        assert_eq!(jobs[0].prefix.as_deref(), Some("vault"));
        assert!(jobs[0].once);
        assert!(matches!(jobs[0].retention, Some(RetentionPolicy::Gfs { daily: 3, .. })));
    }

    /// systemd documents %h as the *manager's* home, which for the system
    /// manager is /root even with User=; `~` and $HOME follow User=.
    #[test]
    fn system_units_resolve_specifiers_and_user_homes_separately() {
        let fx = Fixture::new();
        fx.write(
            "system-units/fa.service",
            "[Service]\nUser=bob\nWorkingDirectory=~\nExecStart=/usr/bin/forever-ago --source %h/data --dest-dir ~/b --prefix d-%l\n",
        );
        let jobs = fx.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].inv.scheduler, Scheduler::SystemdSystem);
        assert_eq!(jobs[0].source, PathBuf::from("/root/data"));
        assert_eq!(jobs[0].dest_dir, PathBuf::from("/srv/bob/b"));
        assert_eq!(jobs[0].prefix.as_deref(), Some("d-box"));
        assert!(!jobs[0].once, "no --once means forever-ago's own daemon loop");
    }

    #[test]
    fn specifiers() {
        let m = ManagerInfo {
            user_scope: true,
            home: "/h".into(),
            user: "me".into(),
            uid: "1000".into(),
            runtime_dir: "/run/user/1000".into(),
            host: "box.lan".into(),
        };
        let (s, unknown) = expand_specifiers("%n %N %p %i %I %j %f %h %u %U %H %l %t %S %C %L %E %T %V %% %Y", "fa-x@a-b.service", &m);
        assert_eq!(
            s,
            "fa-x@a-b.service fa-x@a-b fa-x a-b a/b x /a/b /h me 1000 box.lan box /run/user/1000 \
             /h/.local/state /h/.cache /h/.local/state/log /h/.config /tmp /var/tmp % %Y"
        );
        assert_eq!(unknown, vec!['Y']);
        assert_eq!(unescape_unit(r"home-me-My\x20Notes"), "home/me/My Notes");
    }

    #[test]
    fn dropins_apply_in_systemd_order_and_reset_execstart() {
        let fx = Fixture::new();
        fx.mkdir("home/me/vault");
        // The vendor copy is shadowed by the user's own file of the same name.
        fx.write("vendor-user-units/a.service", "[Service]\nExecStart=/bin/true\n");
        fx.write("user-units/a.service", "[Service]\nExecStart=forever-ago --source vault --prefix a --once\n");
        // b runs forever-ago in its main file, but a drop-in replaces the command.
        fx.write("user-units/b.service", "[Service]\nExecStart=forever-ago --source vault --prefix b --once\n");
        fx.write("user-units/b.service.d/override.conf", "[Service]\nExecStart=\nExecStart=/bin/true\n");
        // fa-c is switched off by a prefix drop-in for every fa-* unit.
        fx.write("user-units/fa-c.service", "[Service]\nExecStart=forever-ago --source vault --prefix c --once\n");
        fx.write("user-units/fa-.service.d/off.conf", "[Service]\nExecStart=\nExecStart=/bin/true\n");
        // d gets its forever-ago command from a drop-in.
        fx.write("user-units/d.service", "[Service]\nExecStart=/bin/true\n");
        let dropin = fx.write("user-units/d.service.d/10-run.conf", "[Service]\nExecStart=\nExecStart=forever-ago --source vault --prefix d --once\n");
        // e is masked.
        fx.link("/dev/null", "user-units/e.service");

        let jobs = fx.jobs();
        let names: Vec<&str> = jobs.iter().map(|j| j.inv.name.as_str()).collect();
        assert_eq!(names, vec!["a.service", "d.service"]);
        assert_eq!(jobs[0].inv.enabled, Some(false), "no trigger and not wanted by any target");
        assert!(jobs[1].inv.defined_in.ends_with(&format!("(ExecStart from {})", dropin.display())), "{}", jobs[1].inv.defined_in);
    }

    #[test]
    fn aliases_are_listed_once() {
        let fx = Fixture::new();
        fx.mkdir("home/me/vault");
        fx.write("user-units/fa.service", "[Service]\nExecStart=forever-ago --source vault --prefix v --once\n");
        fx.link("fa.service", "user-units/vault-backup.service");
        // A linked unit kept elsewhere under its own name is not an alias.
        fx.write("elsewhere/fb.service", "[Service]\nExecStart=forever-ago --source vault --prefix w --once\n");
        fx.link(fx.root.join("elsewhere/fb.service"), "user-units/fb.service");
        let names: Vec<String> = fx.jobs().into_iter().map(|j| j.inv.name).collect();
        assert_eq!(names, vec!["fa.service", "fb.service"]);
    }

    #[test]
    fn units_started_by_paths_or_targets_are_scheduled() {
        let fx = Fixture::new();
        fx.mkdir("home/me/vault");
        fx.write("user-units/fa-boot.service", "[Service]\nExecStart=forever-ago --source vault --prefix b --once\n");
        fx.link("../fa-boot.service", "user-units/default.target.wants/fa-boot.service");
        fx.write("user-units/fa-watch.service", "[Service]\nExecStart=forever-ago --source vault --prefix w --once\n");
        fx.write("user-units/fa-watch.path", "[Path]\nPathChanged=%h/vault/.trigger\n");
        fx.link("../fa-watch.path", "user-units/paths.target.wants/fa-watch.path");
        let jobs = fx.jobs();
        let boot = jobs.iter().find(|j| j.inv.name == "fa-boot.service").unwrap();
        assert_eq!(boot.inv.triggers, vec!["whenever default.target starts"]);
        assert_eq!(boot.inv.enabled, Some(true));
        let watch = jobs.iter().find(|j| j.inv.name == "fa-watch.service").unwrap();
        assert_eq!(watch.inv.triggers, vec!["PathChanged=%h/vault/.trigger"]);
        assert_eq!(watch.inv.enabled, Some(true));
    }

    #[test]
    fn live_state_overrides_offline_guess() {
        let mut inv = Invocation::new(Scheduler::SystemdUser, "x.service".into(), "x".into(), vec![], "/".into(), "/".into());
        inv.units = vec!["x.service".into(), "x.timer".into()];
        inv.enabled = Some(true);
        let live = parse_systemctl_show(
            "Result=success\nId=x.service\nActiveState=inactive\nUnitFileState=static\n\n\
             NextElapseUSecRealtime=\nLastTriggerUSec=Thu 2026-10-01 03:06:22 EDT\nId=x.timer\nActiveState=inactive\nUnitFileState=disabled\n",
        );
        apply_unit_state(&mut inv, &live);
        assert_eq!(inv.enabled, Some(false), "an inactive timer will not fire");
        assert!(inv.state.iter().any(|(k, v)| k == "last run" && v.contains("(success)")), "{:?}", inv.state);
        assert!(!inv.state.iter().any(|(k, _)| k == "next run"));
    }

    // --- cron, pm2, processes ---------------------------------------------------

    #[test]
    fn crontab_lines_become_jobs() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        let mut src = fx.sources();
        src.crontab = Some(
            "MAILTO=\"\"\n# nightly\n15 3 * * * cd $HOME/vault && forever-ago --prefix v-$(hostname -s) --retain 3 --once >> ~/fa.log 2>&1 # keep\n\
             @daily echo forever-ago is great\n"
                .into(),
        );
        src.system_crontabs = vec![fx.write("etc/cron.d/fa", "0 4 * * 0 bob forever-ago --source /srv/bob --prefix b --once\n")];

        let (jobs, _) = discover(&src);
        assert_eq!(jobs.len(), 2, "{jobs:#?}");
        let user_job = jobs.iter().find(|j| j.inv.name.starts_with("crontab -l")).unwrap();
        assert_eq!(user_job.source, vault);
        assert_eq!(user_job.inv.triggers, vec!["15 3 * * *"]);
        assert_eq!(user_job.retention, Some(RetentionPolicy::Count(3)));
        assert_eq!(user_job.prefix, Some(format!("v-{}", hostname().split('.').next().unwrap())));
        let sys_job = jobs.iter().find(|j| j.inv.name.contains("cron.d")).unwrap();
        assert_eq!(sys_job.inv.triggers, vec!["0 4 * * 0"]);
        assert_eq!(sys_job.inv.home, PathBuf::from("/srv/bob"));
    }

    #[test]
    fn crontab_home_override_moves_cwd_and_tilde() {
        let fx = Fixture::new();
        let data = fx.mkdir("data");
        let mut src = fx.sources();
        src.crontab = Some(format!("HOME={}\n0 3 * * * forever-ago --source . --dest-dir ~/b --prefix v --once\n", data.display()));
        let (jobs, _) = discover(&src);
        assert_eq!(jobs[0].source, data);
        assert_eq!(jobs[0].dest_dir, data.join("b"));
    }

    #[test]
    fn cron_skips_files_cron_itself_ignores() {
        assert!(cron_reads("forever-ago"));
        assert!(cron_reads("fa_nightly-2"));
        assert!(!cron_reads("forever-ago.disabled"));
        assert!(!cron_reads("fa.dpkg-old"));
        assert!(!cron_reads("fa~"));
    }

    #[test]
    fn pm2_dump_is_read_without_pm2() {
        let home = Path::new("/h");
        let dump = r#"[
            {"name":"n8n","pm_exec_path":"/usr/bin/n8n","args":["start"]},
            {"name":"vault-backup","pm_exec_path":"/h/.cargo/bin/forever-ago","pm_cwd":"/h/.openclaw",
             "args":["--source",".","--prefix","oc","--run-now"],"status":"online"},
            {"name":"old","script":"forever-ago","args":"--source /x --prefix old","status":"stopped"},
            {"name":"cron","script":"forever-ago","args":"--source /y --prefix y --once","status":"stopped","cron_restart":"0 3 * * *"}
        ]"#;
        let invs = parse_pm2_dump(dump, "dump.pm2", home).unwrap();
        assert_eq!(invs.len(), 3);
        assert_eq!(invs[0].cwd, PathBuf::from("/h/.openclaw"));
        assert_eq!(invs[0].enabled, Some(true));
        assert_eq!(invs[1].argv, vec!["forever-ago", "--source", "/x", "--prefix", "old"]);
        assert_eq!(invs[1].enabled, Some(false));
        // pm2 fires cron_restart even for a stopped app.
        assert_eq!(invs[2].enabled, Some(true));
        assert_eq!(invs[2].triggers, vec!["cron_restart 0 3 * * *"]);
    }

    #[test]
    fn running_daemon_attaches_to_its_scheduler_or_stands_alone() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        let other = fx.mkdir("home/me/other");
        fx.write("user-units/fa.service", "[Service]\nExecStart=forever-ago --source vault --prefix v\n");
        let proc_root = fx.mkdir("proc");
        for (pid, args) in [
            ("4242", "forever-ago\0--source\0vault\0--prefix\0v\0"),
            ("777", "forever-ago\0--prefix\0o\0"),
            // Someone else's daemon: cwd unreadable, source relative. Not a job we can place.
            ("13", "forever-ago\0--source\0.\0--prefix\0r\0"),
        ] {
            fx.write(&format!("proc/{pid}/cmdline"), args);
            fx.write(&format!("proc/{pid}/status"), "Name:\tforever-ago\nUid:\t1000\t1000\t1000\t1000\n");
        }
        symlink(&fx.home, proc_root.join("4242/cwd")).unwrap();
        symlink(&other, proc_root.join("777/cwd")).unwrap();
        let mut src = fx.sources();
        src.proc_root = Some(proc_root);

        let (jobs, _) = discover(&src);
        assert_eq!(jobs.len(), 2, "{jobs:#?}");
        let managed = jobs.iter().find(|j| j.source == vault).unwrap();
        assert_eq!(managed.inv.scheduler, Scheduler::SystemdUser);
        assert_eq!(managed.pids, vec![4242]);
        let lone = jobs.iter().find(|j| j.source == other).unwrap();
        assert_eq!(lone.inv.scheduler, Scheduler::Daemon);
    }

    // --- interpreting arguments ----------------------------------------------------

    #[test]
    fn unparseable_arguments_keep_what_they_can() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        fx.write(
            "user-units/fa.service",
            "[Service]\nExecStart=forever-ago --source=vault --prefix v --exclude node_modules --exclude=*.log --from-the-future\n",
        );
        let jobs = fx.jobs();
        assert_eq!(jobs[0].source, vault);
        assert!(jobs[0].problems[0].contains("cannot parse"), "{:?}", jobs[0].problems);
        let raw: Vec<&str> = jobs[0].excludes.iter().map(|(r, _)| r.as_str()).collect();
        assert_eq!(raw, vec!["node_modules", "*.log"]);
        // The caveat comes first, above the details it qualifies.
        let out = render_job(&jobs[0], &fx.home);
        assert!(out.lines().nth(1).unwrap().contains("problem    this build"), "{out}");
    }

    #[test]
    fn help_version_and_other_subcommands_are_not_jobs() {
        let fx = Fixture::new();
        fx.write("user-units/a.service", "[Service]\nExecStart=forever-ago --version\n");
        fx.write("user-units/b.service", "[Service]\nExecStart=forever-ago --help\n");
        fx.write("user-units/c.service", "[Service]\nExecStart=forever-ago prune --dry-run\n");
        fx.write("user-units/d.service", "[Service]\nExecStart=forever-ago jobs --all\n");
        assert!(fx.jobs().is_empty());
    }

    #[test]
    fn unreadable_exclude_file_is_not_called_fatal() {
        let fx = Fixture::new();
        fx.mkdir("home/me/vault");
        let ex = fx.write("home/me/ex", ".venv\n");
        fs::set_permissions(&ex, std::os::unix::fs::PermissionsExt::from_mode(0o000)).unwrap();
        let job = fx.job_with("vault", &["--exclude-from", ex.to_str().unwrap()]);
        fs::set_permissions(&ex, std::os::unix::fs::PermissionsExt::from_mode(0o644)).unwrap();
        if fs::read(&ex).is_ok() && job.problems.is_empty() {
            return; // running as root: nothing is unreadable
        }
        assert!(job.problems[0].contains("cannot read --exclude-from"), "{:?}", job.problems);
        assert!(!job.problems[0].contains("will fail"), "{:?}", job.problems);
    }

    // --- resolution and output ---------------------------------------------------

    #[test]
    fn resolution_climbs_to_the_nearest_ancestor_with_jobs() {
        let fx = Fixture::new();
        let deep = fx.mkdir("home/me/code/vault/Notes/2026");
        let jobs = vec![fx.job_with("code/vault", &[]), fx.job_with("code", &[]), fx.job_with("code", &[])];

        let (dir, hits) = covering(&jobs, &deep, &fx.home).found.unwrap();
        assert_eq!(dir, fx.home.join("code/vault"));
        assert_eq!(hits.len(), 1);

        let (dir, hits) = covering(&jobs, &fx.home.join("code/elsewhere"), &fx.home).found.unwrap();
        assert_eq!(dir, fx.home.join("code"));
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn resolution_stops_at_home() {
        let fx = Fixture::new();
        let cwd = fx.mkdir("home/me/projects/x");
        // A job on the parent of $HOME must not be reported for a directory inside it.
        let jobs = vec![fx.job_with(fx.root.join("home").to_str().unwrap(), &[])];
        assert!(covering(&jobs, &cwd, &fx.home).found.is_none());

        let report = report(&jobs, &cwd, &fx.home, false);
        assert!(report.contains("no scheduled forever-ago jobs cover ~/projects/x"), "{report}");
        assert!(report.contains("searched it and every parent up to ~"), "{report}");
        assert!(report.contains("1 job(s) back up other directories"), "{report}");
    }

    /// A job whose excludes leave the cwd out does not back it up, so the
    /// climb goes on to the job that does, and says why it skipped the first.
    #[test]
    fn resolution_skips_jobs_that_exclude_the_cwd() {
        let fx = Fixture::new();
        let cwd = fx.mkdir("home/me/vault/proj/node_modules/pkg");
        let jobs = vec![fx.job_with("vault", &["--exclude", "node_modules"]), fx.job_with(".", &[])];
        let report = report(&jobs, &cwd, &fx.home, false);
        assert!(report.starts_with("no jobs back up ~/vault/proj/node_modules/pkg itself; nearest ancestor with jobs: ~\n"), "{report}");
        assert!(report.contains("test-job (cron) backs up ~/vault but excludes ~/vault/proj/node_modules/pkg (`node_modules`)"), "{report}");
        assert!(report.contains("\n~  (1 job)"), "{report}");

        // Nothing else covers it: still say who skipped it.
        let only = vec![fx.job_with("vault", &["--exclude", "node_modules"])];
        let report = super::report(&only, &cwd, &fx.home, false);
        assert!(report.contains("no scheduled forever-ago jobs cover"), "{report}");
        assert!(report.contains("but excludes"), "{report}");
    }

    #[test]
    fn report_renders_a_job() {
        let fx = Fixture::new();
        let cwd = fx.mkdir("home/me/vault");
        let jobs = vec![fx.job_with("vault", &[])];
        let report = report(&jobs, &cwd, &fx.home, false);
        assert!(report.starts_with("~/vault  (1 job)\n"), "{report}");
        assert!(report.contains("NONE: a one-shot"), "{report}");
        assert!(report.contains("~/backups/p-YYYY-MM-DD.tar.gz"), "{report}");
    }

    #[test]
    fn report_all_groups_by_source() {
        let fx = Fixture::new();
        fx.mkdir("home/me/a");
        fx.mkdir("home/me/b");
        let jobs = vec![fx.job_with("a", &[]), fx.job_with("b", &[])];
        let report = report(&jobs, Path::new("/"), &fx.home, true);
        assert!(report.find("~/a  (1 job)").unwrap() < report.find("~/b  (1 job)").unwrap(), "{report}");
    }

    #[test]
    fn running_daemon_does_not_contradict_a_stopped_scheduler() {
        let fx = Fixture::new();
        fx.mkdir("home/me/vault");
        let mut job = test_job(&["forever-ago", "--source", "vault", "--prefix", "p"], &fx.home, &fx.home);
        job.inv.scheduler = Scheduler::Pm2;
        job.inv.enabled = Some(false);
        job.pids = vec![42];
        let out = render_job(&job, &fx.home);
        assert!(!out.contains("WARNING"), "{out}");
        assert!(out.contains("the running daemon still backs up daily at 03:00"), "{out}");
    }

    #[test]
    fn tilde_survives_a_symlinked_home() {
        let fx = Fixture::new();
        let real = fx.mkdir("data/me");
        fx.link(&real, "home/link");
        let link = fx.root.join("home/link");
        assert_eq!(tilde(&real.join("vault"), &link), "~/vault");
        assert_eq!(tilde(&link.join("vault"), &link), "~/vault");
        assert_eq!(tilde(Path::new("/elsewhere"), &link), "/elsewhere");
    }
}
