//! `forever-ago jobs` — which scheduled backups cover this directory?
//!
//! forever-ago keeps no registry of its own: a "job" is just an invocation
//! sitting in some scheduler's config. So this reads each scheduler's own
//! source of truth — systemd unit files, crontabs, the PM2 dump, and running
//! daemons — and re-parses every invocation with the real `Cli`, so defaults
//! and flag semantics are exactly what an actual run would use.

use crate::{
    Cli, ExcludePattern, RetentionPolicy, parse_backup_date, read_exclude_file, retention_policy,
};
use anyhow::{Result, anyhow};
use clap::Parser;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

const BIN: &str = "forever-ago";

/// Commands that run another command: `nice -n 10 forever-ago ...` is still a forever-ago job.
const WRAPPERS: &[&str] = &[
    "env", "nice", "ionice", "nohup", "timeout", "flock", "chrt", "taskset", "setsid", "stdbuf",
    "systemd-cat", "systemd-inhibit", "time", "sudo", "doas", "runuser", "exec", "command",
    "chronic", "cronic", "mise",
];
const SHELLS: &[&str] = &["sh", "bash", "zsh", "dash", "ksh", "fish"];

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
            let mut more: Vec<PathBuf> = rd.flatten().map(|e| e.path()).filter(|p| p.is_file()).collect();
            more.sort();
            system_crontabs.extend(more);
        }

        let pm2_home = std::env::var_os("PM2_HOME").map(PathBuf::from).unwrap_or_else(|| home.join(".pm2"));

        Ok(Self {
            user_unit_dirs: vec![
                config.join("systemd/user"),
                PathBuf::from("/etc/systemd/user"),
                data.join("systemd/user"),
                PathBuf::from("/usr/local/share/systemd/user"),
                PathBuf::from("/usr/share/systemd/user"),
                PathBuf::from("/usr/local/lib/systemd/user"),
                PathBuf::from("/usr/lib/systemd/user"),
                PathBuf::from("/lib/systemd/user"),
            ],
            system_unit_dirs: vec![
                PathBuf::from("/etc/systemd/system"),
                PathBuf::from("/run/systemd/system"),
                PathBuf::from("/usr/local/lib/systemd/system"),
                PathBuf::from("/usr/lib/systemd/system"),
                PathBuf::from("/lib/systemd/system"),
            ],
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
            .map(|(source, group)| render_group(source, &group, home, None))
            .collect();
        out.push_str(&blocks.join("\n"));
        return out;
    }

    let cwd = canonical_or_normalized(cwd);
    let stop = canonical_or_normalized(home);
    match covering(jobs, &cwd, &stop) {
        Some((dir, group)) => {
            if dir != cwd {
                out.push_str(&format!(
                    "no jobs back up {} itself; nearest ancestor with jobs: {}\n\n",
                    tilde(&cwd, home),
                    tilde(&dir, home)
                ));
            }
            out.push_str(&render_group(&dir, &group, home, Some(&cwd)));
        }
        None => {
            let limit = if cwd.starts_with(&stop) { tilde(&stop, home) } else { "/".to_string() };
            out.push_str(&format!(
                "no scheduled forever-ago jobs cover {} (searched it and every parent up to {limit})\n",
                tilde(&cwd, home)
            ));
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

/// Climb from `start` toward the root, stopping after `stop` (the home dir),
/// and return the first directory some job backs up.
pub(crate) fn covering<'a>(jobs: &'a [Job], start: &Path, stop: &Path) -> Option<(PathBuf, Vec<&'a Job>)> {
    climb(start, stop, |dir| {
        let hits: Vec<&Job> = jobs.iter().filter(|j| j.source == dir).collect();
        (!hits.is_empty()).then_some(hits)
    })
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
/// run at all (e.g. someone scheduled `forever-ago jobs`).
fn resolve(inv: Invocation) -> Option<Job> {
    let resolve_in = |p: &Path| resolve_path(p, &inv.cwd, &inv.home);
    match Cli::try_parse_from(&inv.argv) {
        Ok(cli) => {
            if cli.command.is_some() {
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
            let mut raw = cli.excludes.clone();
            if let Some(path) = &exclude_from {
                match read_exclude_file(path) {
                    Ok(more) => raw.extend(more),
                    Err(err) => problems.push(format!("{err:#}; the real run will fail here too")),
                }
            }
            let mut excludes = Vec::new();
            for r in raw {
                match ExcludePattern::parse(&r) {
                    Ok(p) => excludes.push((r, p)),
                    Err(err) => problems.push(format!("{err:#}; the real run will refuse to start")),
                }
            }
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
            // Flags this build does not know (an older or newer forever-ago).
            // Still pull out --source so the job can be matched to a directory.
            let reason = err.to_string();
            let reason = reason.lines().next().unwrap_or("").trim_start_matches("error: ").to_string();
            let source = flag_value(&inv.argv, "--source").unwrap_or_else(|| ".".into());
            let dest_dir = flag_value(&inv.argv, "--dest-dir")
                .map(|d| resolve_in(Path::new(&d)))
                .unwrap_or_else(|| inv.home.join("backups"));
            Some(Job {
                source: resolve_in(Path::new(&source)),
                dest_dir,
                prefix: flag_value(&inv.argv, "--prefix"),
                once: inv.argv.iter().any(|a| a == "--once"),
                run_now: false,
                at: flag_value(&inv.argv, "--at").unwrap_or_else(|| "03:00".into()),
                retention: None,
                exclude_from: None,
                excludes: Vec::new(),
                problems: vec![format!(
                    "this build of forever-ago cannot parse the job's arguments ({reason}); details below are partial"
                )],
                pids: Vec::new(),
                inv,
            })
        }
    }
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

/// `(section, key, value)` in file order, continuation lines joined.
type UnitEntries = Vec<(String, String, String)>;

struct UnitFile {
    name: String,
    path: PathBuf,
    entries: UnitEntries,
}

impl UnitFile {
    /// All values of a list-valued key, honouring systemd's "empty assignment resets the list".
    fn list(&self, section: &str, key: &str) -> Vec<String> {
        let mut out = Vec::new();
        for (s, k, v) in &self.entries {
            if s == section && k == key {
                if v.is_empty() {
                    out.clear();
                } else {
                    out.push(v.clone());
                }
            }
        }
        out
    }

    /// A single-valued key: the last assignment wins.
    fn last(&self, section: &str, key: &str) -> Option<String> {
        self.entries
            .iter()
            .rev()
            .find(|(s, k, _)| s == section && k == key)
            .map(|(_, _, v)| v.clone())
            .filter(|v| !v.is_empty())
    }
}

fn parse_unit_text(text: &str, out: &mut UnitEntries) {
    fn push(section: &str, line: &str, out: &mut UnitEntries) {
        if let Some((k, v)) = line.split_once('=') {
            out.push((section.to_string(), k.trim().to_string(), v.trim().to_string()));
        }
    }

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

/// Load every unit with `suffix` from a search path: earlier directories win,
/// masked units are skipped, drop-ins are applied in filename order.
fn load_units(dirs: &[PathBuf], suffix: &str) -> Vec<UnitFile> {
    let mut found: BTreeMap<String, PathBuf> = BTreeMap::new();
    for dir in dirs {
        let Ok(rd) = fs::read_dir(dir) else { continue };
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            // Templates (foo@.service) need an instance name to mean anything.
            if name.ends_with(suffix) && !name.contains('@') {
                found.entry(name).or_insert_with(|| entry.path());
            }
        }
    }

    let mut units = Vec::new();
    for (name, path) in found {
        if fs::canonicalize(&path).is_ok_and(|p| p == Path::new("/dev/null")) {
            continue; // masked
        }
        let Ok(text) = fs::read_to_string(&path) else { continue };
        if text.trim().is_empty() {
            continue;
        }
        let mut entries = Vec::new();
        parse_unit_text(&text, &mut entries);

        let mut dropins: BTreeMap<String, PathBuf> = BTreeMap::new();
        for dir in dirs {
            let Ok(rd) = fs::read_dir(dir.join(format!("{name}.d"))) else { continue };
            for entry in rd.flatten() {
                let fname = entry.file_name().to_string_lossy().into_owned();
                if fname.ends_with(".conf") {
                    dropins.entry(fname).or_insert_with(|| entry.path());
                }
            }
        }
        for dropin in dropins.values() {
            if let Ok(text) = fs::read_to_string(dropin) {
                parse_unit_text(&text, &mut entries);
            }
        }
        units.push(UnitFile { name, path, entries });
    }
    units
}

/// Is `unit` pulled in by some target (`*.wants/` or `*.requires/` link)?
fn unit_enabled_offline(dirs: &[PathBuf], unit: &str) -> bool {
    dirs.iter().any(|dir| {
        fs::read_dir(dir).is_ok_and(|rd| {
            rd.flatten().any(|e| {
                let n = e.file_name().to_string_lossy().into_owned();
                (n.ends_with(".wants") || n.ends_with(".requires")) && e.path().join(unit).exists()
            })
        })
    })
}

fn expand_specifiers(s: &str, unit: &str, home: &Path, user: &str) -> String {
    let stem = unit.rsplit_once('.').map_or(unit, |(stem, _)| stem);
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('h') => out.push_str(&home.to_string_lossy()),
            Some('u') => out.push_str(user),
            Some('n') => out.push_str(unit),
            Some('N') | Some('p') => out.push_str(stem),
            Some('%') => out.push('%'),
            Some(other) => {
                out.push('%');
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
}

fn systemd_invocations(src: &Sources, scheduler: Scheduler) -> Vec<Invocation> {
    let user_scope = scheduler == Scheduler::SystemdUser;
    let dirs = if user_scope { &src.user_unit_dirs } else { &src.system_unit_dirs };

    let services = load_units(dirs, ".service");
    if !services
        .iter()
        .any(|u| u.entries.iter().any(|(_, _, v)| v.contains(BIN)))
    {
        return Vec::new();
    }

    // timer -> the service it starts (Unit=, defaulting to the same stem).
    let mut timers_for: HashMap<String, Vec<UnitFile>> = HashMap::new();
    for timer in load_units(dirs, ".timer") {
        let target = timer.last("Timer", "Unit").unwrap_or_else(|| {
            format!("{}.service", timer.name.trim_end_matches(".timer"))
        });
        timers_for.entry(target).or_default().push(timer);
    }

    let me = std::env::var("USER").unwrap_or_default();
    let mut out = Vec::new();
    for svc in services {
        let user = svc.last("Service", "User");
        let (home, user_name) = if user_scope {
            (src.home.clone(), me.clone())
        } else {
            match &user {
                Some(u) => (src.home_of(u).unwrap_or_else(|| PathBuf::from("/")), u.clone()),
                None => (PathBuf::from("/root"), "root".to_string()),
            }
        };
        let spec = |s: &str| expand_specifiers(s, &svc.name, &home, &user_name);

        let mut vars: HashMap<String, String> = HashMap::new();
        for assignment in svc.list("Service", "Environment") {
            for word in tokenize(&spec(&assignment)) {
                if let Some((k, v)) = word.split_once('=') {
                    vars.insert(k.to_string(), v.to_string());
                }
            }
        }

        let base_cwd = match svc.last("Service", "WorkingDirectory") {
            Some(wd) => {
                let wd = spec(wd.trim_start_matches('-'));
                if wd == "~" { home.clone() } else { PathBuf::from(wd) }
            }
            // systemd's default: the user's home for user managers, / for the system one.
            None if user_scope => home.clone(),
            None => PathBuf::from("/"),
        };

        let starts: Vec<(Option<Vec<String>>, Vec<String>)> = svc
            .list("Service", "ExecStart")
            .iter()
            .filter_map(|line| find_invocation(&strip_exec_prefixes(tokenize(&spec(line))), 0))
            .collect();
        let count = starts.len();

        for (n, (cds, argv)) in starts.into_iter().enumerate() {
            let name = if count > 1 { format!("{} (ExecStart #{})", svc.name, n + 1) } else { svc.name.clone() };
            let argv: Vec<String> = argv.iter().map(|a| expand_vars(a, &vars)).collect();
            let cwd = apply_cds(&base_cwd, cds.as_deref(), &home, &vars);
            let mut inv = Invocation::new(scheduler, name, svc.path.display().to_string(), argv, cwd, home.clone());
            inv.units.push(svc.name.clone());

            let timers = timers_for.get(&svc.name).map(Vec::as_slice).unwrap_or(&[]);
            for timer in timers {
                inv.units.push(timer.name.clone());
                for key in [
                    "OnCalendar",
                    "OnBootSec",
                    "OnStartupSec",
                    "OnActiveSec",
                    "OnUnitActiveSec",
                    "OnUnitInactiveSec",
                ] {
                    for value in timer.list("Timer", key) {
                        inv.triggers.push(format!("{key}={value}"));
                    }
                }
                if timer.last("Timer", "Persistent").is_some_and(|v| is_truthy(&v)) {
                    inv.trigger_notes.push("catches up after downtime".into());
                }
                if let Some(delay) = timer.last("Timer", "RandomizedDelaySec") {
                    inv.trigger_notes.push(format!("random delay up to {delay}"));
                }
            }
            inv.enabled = Some(if timers.is_empty() {
                unit_enabled_offline(dirs, &svc.name)
            } else {
                timers.iter().any(|t| unit_enabled_offline(dirs, &t.name))
            });
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
fn strip_exec_prefixes(mut argv: Vec<String>) -> Vec<String> {
    let Some(first) = argv.first_mut() else { return argv };
    let n = first.chars().take_while(|c| "@-:+!|".contains(*c)).count();
    let had_at = first[..n].contains('@');
    first.replace_range(..n, "");
    if had_at && argv.len() > 1 {
        argv.remove(1);
    }
    argv
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
        let home = match user {
            Some(u) => src.home_of(u).unwrap_or_else(|| PathBuf::from("/")),
            None => src.home.clone(),
        };
        let mut vars = vars.clone();
        vars.entry("HOME".into()).or_insert_with(|| home.to_string_lossy().into_owned());

        let Some((cds, argv)) = find_invocation(&tokenize(command), 0) else { continue };
        let argv: Vec<String> = argv.iter().map(|a| expand_vars(a, &vars)).collect();
        // cron starts every command in the owner's home directory.
        let cwd = apply_cds(&home, cds.as_deref(), &home, &vars);
        let mut inv = Invocation::new(
            Scheduler::Cron,
            format!("{origin}, line {}", idx + 1),
            origin.to_string(),
            argv,
            cwd,
            home,
        );
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
        if let Some(cron) = get("cron_restart") {
            inv.triggers.push(format!("cron_restart {cron}"));
        }
        if let Some(status) = get("status") {
            inv.enabled = Some(status == "online");
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
        let cwd = fs::read_link(dir.join("cwd")).unwrap_or_else(|_| home.clone());
        let mut inv = Invocation::new(
            Scheduler::Daemon,
            format!("pid {pid}"),
            dir.join("cmdline").display().to_string(),
            argv,
            cwd,
            home,
        );
        inv.state.push(("pid".into(), pid.to_string()));
        out.push((pid, inv));
    }
    out
}

// ---------------------------------------------------------------------------
// Command-line archaeology
// ---------------------------------------------------------------------------

/// Split a command line into words: single and double quotes, backslash
/// escapes. Enough for unit files, crontabs, and `sh -c` strings.
fn tokenize(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_word = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_word = true;
                for c in chars.by_ref() {
                    if c == '\'' {
                        break;
                    }
                    cur.push(c);
                }
            }
            '"' => {
                in_word = true;
                while let Some(c) = chars.next() {
                    match c {
                        '"' => break,
                        '\\' if matches!(chars.peek(), Some('"' | '\\' | '$' | '`')) => {
                            cur.extend(chars.next());
                        }
                        _ => cur.push(c),
                    }
                }
            }
            '\\' => {
                in_word = true;
                cur.extend(chars.next());
            }
            // `2>&1` is a redirection, not a background `&`.
            '&' if cur.ends_with(['>', '<']) => cur.push(c),
            // Operators split words even when glued: `cd /x; forever-ago`.
            ';' | '&' | '|' => {
                if in_word {
                    out.push(std::mem::take(&mut cur));
                    in_word = false;
                }
                let mut op = c.to_string();
                if (c == '&' && chars.peek() == Some(&'&')) || (c == '|' && matches!(chars.peek(), Some('|' | '&'))) {
                    op.extend(chars.next());
                }
                out.push(op);
            }
            c if c.is_whitespace() => {
                if in_word {
                    out.push(std::mem::take(&mut cur));
                    in_word = false;
                }
            }
            _ => {
                in_word = true;
                cur.push(c);
            }
        }
    }
    if in_word {
        out.push(cur);
    }
    out
}

fn basename(word: &str) -> &str {
    word.rsplit('/').next().unwrap_or(word)
}

fn is_operator(word: &str) -> bool {
    matches!(word, "&&" | "||" | ";" | "|" | "&" | "|&")
}

fn is_redirect(word: &str) -> bool {
    let w = word.trim_start_matches(|c: char| c.is_ascii_digit());
    w.starts_with('>') || w.starts_with('<') || w.starts_with("&>")
}

fn is_assignment(word: &str) -> bool {
    word.split_once('=').is_some_and(|(k, _)| {
        !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

/// Find the forever-ago command inside a command line. Returns any `cd DIR`
/// that runs before it (so relative paths resolve correctly) and its argv,
/// cut off at the first shell operator or redirection.
///
/// Only a word in command position counts — first word, after an operator,
/// after `VAR=x` assignments, or handed to a wrapper like `nice`/`flock` — so
/// `restic backup ~/code/forever-ago` is not mistaken for a job.
fn find_invocation(words: &[String], depth: u8) -> Option<(Option<Vec<String>>, Vec<String>)> {
    let mut cds: Vec<String> = Vec::new();
    let mut command_position = true;
    let mut wrapped = false;
    let mut i = 0;
    while i < words.len() {
        let w = words[i].as_str();
        if is_operator(w) {
            command_position = true;
            wrapped = false;
            i += 1;
            continue;
        }
        if command_position && is_assignment(w) {
            i += 1;
            continue;
        }
        let name = basename(w);
        let runnable = name == BIN && !w.ends_with('/') && !Path::new(w).is_dir();
        if runnable && (command_position || (wrapped && !is_assignment(w))) {
            let argv: Vec<String> = words[i..]
                .iter()
                .take_while(|a| !is_operator(a) && !is_redirect(a))
                .cloned()
                .collect();
            return Some(((!cds.is_empty()).then_some(cds), argv));
        }
        if command_position {
            if SHELLS.contains(&name) && depth < 3 {
                let script = words[i + 1..]
                    .iter()
                    .position(|a| a == "-c" || (a.starts_with('-') && !a.starts_with("--") && a.ends_with('c')))
                    .and_then(|p| words.get(i + 1 + p + 1));
                if let Some(script) = script
                    && let Some((inner_cds, argv)) = find_invocation(&tokenize(script), depth + 1)
                {
                    cds.extend(inner_cds.unwrap_or_default());
                    return Some(((!cds.is_empty()).then_some(cds), argv));
                }
            }
            if name == "cd" {
                cds.push(words.get(i + 1).filter(|a| !is_operator(a)).cloned().unwrap_or_else(|| "~".into()));
            }
            wrapped = WRAPPERS.contains(&name);
            command_position = false;
        }
        i += 1;
    }
    None
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

fn apply_cds(base: &Path, cds: Option<&[String]>, home: &Path, vars: &HashMap<String, String>) -> PathBuf {
    let mut cwd = base.to_path_buf();
    for dir in cds.unwrap_or_default() {
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

pub(crate) fn tilde(path: &Path, home: &Path) -> String {
    match path.strip_prefix(home) {
        Ok(rest) if rest.as_os_str().is_empty() => "~".to_string(),
        Ok(rest) => format!("~/{}", rest.display()),
        Err(_) => path.display().to_string(),
    }
}

fn render_group(source: &Path, jobs: &[&Job], home: &Path, cwd: Option<&Path>) -> String {
    let mut out = format!(
        "{}  ({} job{})\n",
        tilde(source, home),
        jobs.len(),
        if jobs.len() == 1 { "" } else { "s" }
    );
    for job in jobs {
        out.push('\n');
        out.push_str(&render_job(job, home, cwd));
    }
    out
}

fn render_job(job: &Job, home: &Path, cwd: Option<&Path>) -> String {
    let inv = &job.inv;
    let mut rows: Vec<(String, String)> = Vec::new();

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
        schedule.push("NONE: a one-shot (--once) with no timer; runs only when started by hand".into());
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
        rows.push(("WARNING".into(), format!("will not fire: {why}")));
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

    if let Some(cwd) = cwd
        && let Some(pattern) = excluded_by(job, cwd)
    {
        rows.push((
            "NOTE".into(),
            format!("{} is excluded by `{pattern}` and is NOT in these backups", tilde(cwd, home)),
        ));
    }
    for p in &job.problems {
        rows.push(("problem".into(), p.clone()));
    }

    let mut out = format!("  {}  [{}]\n", inv.name, inv.scheduler.label());
    for (label, value) in rows {
        out.push_str(&format!("    {label:<11}{value}\n"));
    }
    out
}

/// If `cwd` (inside the job's source) is pruned by an exclude, which pattern did it.
fn excluded_by<'a>(job: &'a Job, cwd: &Path) -> Option<&'a str> {
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
    let Ok(rd) = fs::read_dir(dest_dir) else {
        return "nothing yet (destination does not exist)".to_string();
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
            "{} backup{}, newest {name} ({:.1} MB)",
            backups.len(),
            if backups.len() == 1 { "" } else { "s" },
            *size as f64 / 1_048_576.0
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

    fn w(s: &str) -> Vec<String> {
        tokenize(s)
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
                live: false,
            }
        }
    }

    #[test]
    fn tokenize_handles_quotes_and_escapes() {
        assert_eq!(
            w(r#"forever-ago --source "/a b/c" --prefix 'x y' --dest-dir /d\ e"#),
            vec!["forever-ago", "--source", "/a b/c", "--prefix", "x y", "--dest-dir", "/d e"]
        );
        assert_eq!(w(r#"sh -c "echo \"hi\"""#), vec!["sh", "-c", "echo \"hi\""]);
        assert_eq!(w("cd /x;forever-ago&&b||c 2>&1 'a;b'"), vec!["cd", "/x", ";", "forever-ago", "&&", "b", "||", "c", "2>&1", "a;b"]);
    }

    #[test]
    fn unit_parser_joins_continuations_and_skips_comments_inside_them() {
        let mut e = Vec::new();
        parse_unit_text(
            "[Service]\n# c\nExecStart=/bin/forever-ago \\\n    --prefix v \\\n# ignored\n    --once\nUser=bob\n",
            &mut e,
        );
        assert_eq!(e[0], ("Service".into(), "ExecStart".into(), "/bin/forever-ago --prefix v --once".into()));
        assert_eq!(e[1], ("Service".into(), "User".into(), "bob".into()));
    }

    #[test]
    fn finds_invocation_only_in_command_position() {
        let found = |s: &str| find_invocation(&w(s), 0).map(|(_, argv)| argv);
        assert_eq!(found("forever-ago --prefix v --once").unwrap()[0], "forever-ago");
        assert_eq!(found("FOO=1 nice -n 10 /opt/forever-ago --prefix v").unwrap()[0], "/opt/forever-ago");
        assert_eq!(
            found("cd /x && forever-ago --prefix v >> /var/log/fa.log 2>&1").unwrap(),
            vec!["forever-ago", "--prefix", "v"]
        );
        assert_eq!(found("/bin/sh -lc 'cd /x; forever-ago --prefix v'").unwrap()[0], "forever-ago");
        assert!(found("notify-send 'forever-ago failed'").is_none());
        assert!(found("restic backup /home/me/code/forever-ago/").is_none());
        assert!(found("env FOO=/x/forever-ago restic backup").is_none());
    }

    #[test]
    fn cd_before_invocation_sets_cwd() {
        let (cds, _) = find_invocation(&w("cd $HOME/vault && forever-ago --prefix v"), 0).unwrap();
        let vars = HashMap::from([("HOME".to_string(), "/h".to_string())]);
        assert_eq!(apply_cds(Path::new("/h"), cds.as_deref(), Path::new("/h"), &vars), PathBuf::from("/h/vault"));
    }

    #[test]
    fn systemd_service_with_timer_is_discovered() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        fx.write(
            "user-units/fa-vault.service",
            "[Service]\nType=oneshot\nEnvironment=\"DEST=%h/backups/v\"\n\
             ExecStart=-/opt/bin/forever-ago \\\n  --source %h/vault \\\n  --dest-dir ${DEST} --prefix vault \\\n  --keep-daily 7 --once\n",
        );
        fx.write(
            "user-units/fa-vault.timer",
            "[Timer]\nOnCalendar=*-*-* 03:00:00\nPersistent=true\nRandomizedDelaySec=15m\n",
        );
        fx.mkdir("user-units/timers.target.wants");
        symlink(fx.root.join("user-units/fa-vault.timer"), fx.root.join("user-units/timers.target.wants/fa-vault.timer")).unwrap();
        // Mentions forever-ago, but does not run it.
        fx.write("user-units/fa-alert.service", "[Service]\nExecStart=/usr/bin/notify-send \"forever-ago failed\"\n");

        let (jobs, warnings) = discover(&fx.sources());
        assert!(warnings.is_empty());
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
    fn earlier_unit_dir_wins_and_dropins_can_reset_execstart() {
        let fx = Fixture::new();
        fx.mkdir("home/me/vault");
        // The vendor copy is shadowed by the user's own file of the same name.
        fx.write("vendor-user-units/a.service", "[Service]\nExecStart=/bin/true\n");
        fx.write("user-units/a.service", "[Service]\nExecStart=forever-ago --source vault --prefix a --once\n");
        // b runs forever-ago in its main file, but a drop-in replaces the command.
        fx.write("user-units/b.service", "[Service]\nExecStart=forever-ago --source vault --prefix b --once\n");
        fx.write("user-units/b.service.d/override.conf", "[Service]\nExecStart=\nExecStart=/bin/true\n");
        // c is masked.
        fx.mkdir("user-units");
        symlink("/dev/null", fx.root.join("user-units/c.service")).unwrap();

        let (jobs, _) = discover(&fx.sources());
        let names: Vec<&str> = jobs.iter().map(|j| j.inv.name.as_str()).collect();
        assert_eq!(names, vec!["a.service"]);
        assert_eq!(jobs[0].inv.enabled, Some(false), "no timer and not wanted by any target");
    }

    #[test]
    fn system_unit_resolves_home_from_user_field() {
        let fx = Fixture::new();
        fx.write(
            "system-units/fa.service",
            "[Service]\nUser=bob\nWorkingDirectory=~\nExecStart=/usr/bin/forever-ago --source ~/data --prefix d\n",
        );
        let (jobs, _) = discover(&fx.sources());
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].inv.scheduler, Scheduler::SystemdSystem);
        assert_eq!(jobs[0].source, PathBuf::from("/srv/bob/data"));
        assert_eq!(jobs[0].dest_dir, PathBuf::from("/srv/bob/backups"));
        assert!(!jobs[0].once, "no --once means forever-ago's own daemon loop");
    }

    #[test]
    fn crontab_lines_become_jobs() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        let mut src = fx.sources();
        src.crontab = Some(
            "MAILTO=\"\"\n# nightly\n15 3 * * * cd $HOME/vault && forever-ago --prefix v --retain 3 --once >> ~/fa.log 2>&1\n\
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
        let sys_job = jobs.iter().find(|j| j.inv.name.contains("cron.d")).unwrap();
        assert_eq!(sys_job.inv.triggers, vec!["0 4 * * 0"]);
        assert_eq!(sys_job.inv.home, PathBuf::from("/srv/bob"));
    }

    #[test]
    fn pm2_dump_is_read_without_pm2() {
        let home = Path::new("/h");
        let dump = r#"[
            {"name":"n8n","pm_exec_path":"/usr/bin/n8n","args":["start"]},
            {"name":"vault-backup","pm_exec_path":"/h/.cargo/bin/forever-ago","pm_cwd":"/h/.openclaw",
             "args":["--source",".","--prefix","oc","--run-now"],"status":"online"},
            {"name":"old","script":"forever-ago","args":"--source /x --prefix old","status":"stopped"}
        ]"#;
        let invs = parse_pm2_dump(dump, "dump.pm2", home).unwrap();
        assert_eq!(invs.len(), 2);
        assert_eq!(invs[0].cwd, PathBuf::from("/h/.openclaw"));
        assert_eq!(invs[0].enabled, Some(true));
        assert_eq!(invs[1].argv, vec!["forever-ago", "--source", "/x", "--prefix", "old"]);
        assert_eq!(invs[1].enabled, Some(false));
    }

    #[test]
    fn running_daemon_attaches_to_its_scheduler_or_stands_alone() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        let other = fx.mkdir("home/me/other");
        fx.write("user-units/fa.service", "[Service]\nExecStart=forever-ago --source vault --prefix v\n");
        let proc_root = fx.mkdir("proc");
        for (pid, args) in [("4242", "forever-ago\0--source\0vault\0--prefix\0v\0"), ("777", "forever-ago\0--prefix\0o\0")] {
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

    #[test]
    fn unparseable_arguments_still_match_by_source() {
        let fx = Fixture::new();
        let vault = fx.mkdir("home/me/vault");
        fx.write("user-units/fa.service", "[Service]\nExecStart=forever-ago --source=vault --prefix v --from-the-future\n");
        let (jobs, _) = discover(&fx.sources());
        assert_eq!(jobs[0].source, vault);
        assert!(jobs[0].problems[0].contains("cannot parse"), "{:?}", jobs[0].problems);
    }

    fn job_for(fx: &Fixture, source: &str, excludes: &[&str]) -> Job {
        let mut argv = vec!["forever-ago".to_string(), "--source".into(), source.into(), "--prefix".into(), "p".into(), "--once".into()];
        for e in excludes {
            argv.push("--exclude".into());
            argv.push(e.to_string());
        }
        let inv = Invocation::new(Scheduler::Cron, source.into(), "test".into(), argv, fx.home.clone(), fx.home.clone());
        resolve(inv).unwrap()
    }

    #[test]
    fn resolution_climbs_to_the_nearest_ancestor_with_jobs() {
        let fx = Fixture::new();
        let deep = fx.mkdir("home/me/code/vault/Notes/2026");
        let jobs = vec![job_for(&fx, "code/vault", &[]), job_for(&fx, "code", &[]), job_for(&fx, "code", &[])];

        let (dir, hits) = covering(&jobs, &deep, &fx.home).unwrap();
        assert_eq!(dir, fx.home.join("code/vault"));
        assert_eq!(hits.len(), 1);

        let (dir, hits) = covering(&jobs, &fx.home.join("code/elsewhere"), &fx.home).unwrap();
        assert_eq!(dir, fx.home.join("code"));
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn resolution_stops_at_home() {
        let fx = Fixture::new();
        let cwd = fx.mkdir("home/me/projects/x");
        // A job on the parent of $HOME must not be reported for a directory inside it.
        let jobs = vec![job_for(&fx, fx.root.join("home").to_str().unwrap(), &[])];
        assert!(covering(&jobs, &cwd, &fx.home).is_none());

        let report = report(&jobs, &cwd, &fx.home, false);
        assert!(report.contains("no scheduled forever-ago jobs cover ~/projects/x"), "{report}");
        assert!(report.contains("searched it and every parent up to ~"), "{report}");
        assert!(report.contains("1 job(s) back up other directories"), "{report}");
    }

    #[test]
    fn report_explains_ancestor_resolution_and_exclusions() {
        let fx = Fixture::new();
        let cwd = fx.mkdir("home/me/vault/proj/node_modules/pkg");
        let jobs = vec![job_for(&fx, "vault", &["node_modules"])];
        let report = report(&jobs, &cwd, &fx.home, false);
        assert!(report.contains("no jobs back up ~/vault/proj/node_modules/pkg itself; nearest ancestor with jobs: ~/vault"), "{report}");
        assert!(report.contains("~/vault  (1 job)"), "{report}");
        assert!(report.contains("excluded by `node_modules`"), "{report}");
        assert!(report.contains("NONE: a one-shot"), "{report}");
        assert!(report.contains("~/backups/p-YYYY-MM-DD.tar.gz"), "{report}");
    }

    #[test]
    fn report_all_groups_by_source() {
        let fx = Fixture::new();
        fx.mkdir("home/me/a");
        fx.mkdir("home/me/b");
        let jobs = vec![job_for(&fx, "a", &[]), job_for(&fx, "b", &[])];
        let report = report(&jobs, Path::new("/"), &fx.home, true);
        assert!(report.find("~/a  (1 job)").unwrap() < report.find("~/b  (1 job)").unwrap(), "{report}");
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
}
