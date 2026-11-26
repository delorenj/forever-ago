//! Emma: project-declared backups on Forever Ago (EMMA-DESIGN.md, slice 1).
//!
//! A project declares what it wants backed up in its own `.emma.toml`.
//! Forever Ago validates it, records it in a derived local registry, and
//! plans it. Slice 1 is observe-only: nothing here captures, prunes,
//! restores, schedules or calls the legacy backup path.
//!
//! Owners consumed, never copied: Candybar (when, how it runs, receipts),
//! pjangler (`pj info --json`, project identity), Holocene (reads
//! `projects --json` the way it reads `wax catalog --json`).

pub mod adopt;
pub mod candybar;
pub mod envelope;
pub mod identity;
pub mod manifest;
pub mod plan;
pub mod register;
pub mod registry;
pub mod retention;
pub mod strategies;

#[cfg(test)]
mod tests;

use crate::jobs::Job;
use chrono::{DateTime, Utc};
use envelope::{emit, exit, Finding};
use registry::{EmmaRow, Registry};
use serde_json::{json, Value as Json};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Everything the Emma commands read from outside, injectable for tests.
pub struct Env {
    pub home: PathBuf,
    pub state_dir: PathBuf,
    pub pj: String,
    pub candybar: String,
    pub jobs: Option<Vec<Job>>,
}

impl Env {
    pub fn from_host() -> Result<Env, Finding> {
        let home = dirs::home_dir().ok_or_else(|| Finding::of("E_HOME", "could not resolve $HOME"))?;
        Ok(Env {
            state_dir: registry::default_state_dir(&home),
            home,
            pj: identity::pj_bin(),
            candybar: candybar::candybar_bin(),
            jobs: None,
        })
    }
}

pub struct Bounded {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// Run an owner's CLI with a hard timeout and no retries (the pattern
/// `jobs` uses for systemctl, plus a deadline). No shell.
pub fn run_bounded(bin: &str, args: &[&str], cwd: Option<&Path>, timeout: Duration) -> Result<Bounded, String> {
    let mut cmd = Command::new(bin);
    cmd.args(args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    if let Some(d) = cwd {
        cmd.current_dir(d);
    }
    let mut child = cmd.spawn().map_err(|e| format!("cannot run {bin}: {e}"))?;
    let mut so = child.stdout.take().ok_or("no stdout")?;
    let mut se = child.stderr.take().ok_or("no stderr")?;
    let t_out = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = so.read_to_string(&mut s);
        s
    });
    let t_err = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = se.read_to_string(&mut s);
        s
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(st)) => break st,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("{bin} timed out after {}s", timeout.as_secs()));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(15)),
            Err(e) => return Err(format!("waiting for {bin}: {e}")),
        }
    };
    Ok(Bounded { code: status.code(), stdout: t_out.join().unwrap_or_default(), stderr: t_err.join().unwrap_or_default() })
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(clap::Args, Debug)]
pub struct RegisterArgs {
    /// A directory (searched upward to the git toplevel) or a .emma.toml. Default: the current directory.
    pub path: Option<PathBuf>,
    /// Every pjangler project's `<repo_path>/.emma.toml` (no home scan).
    #[arg(long, conflicts_with = "path")]
    pub all: bool,
    /// Show what would change; write nothing, not even the state directory.
    #[arg(long)]
    pub dry_run: bool,
    /// Apply drift, moves and identity changes.
    #[arg(long)]
    pub update: bool,
    /// Mark orphans retired (never deletes rows or history).
    #[arg(long)]
    pub prune: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args, Debug)]
pub struct AdoptArgs {
    /// Create `<repo_path>/.emma.toml` when absent. An existing file is never edited.
    #[arg(long)]
    pub write: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args, Debug)]
pub struct ProjectsArgs {
    /// Only this project's Emmas.
    #[arg(long)]
    pub project: Option<String>,
    /// Skip the Candybar read (schedule shows `unknown`).
    #[arg(long)]
    pub offline: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args, Debug)]
pub struct PlanArgs {
    /// `project/emma`, or `emma` when unambiguous.
    pub emma: String,
    /// Bounded read-only walk of a files source (entries and bytes).
    #[arg(long)]
    pub measure: bool,
    /// Entry limit for --measure.
    #[arg(long, default_value_t = 200_000)]
    pub max_entries: usize,
    #[arg(long)]
    pub offline: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args, Debug)]
pub struct RetentionArgs {
    #[command(subcommand)]
    pub command: RetentionCommand,
}

#[derive(clap::Subcommand, Debug)]
pub enum RetentionCommand {
    /// Which restore points the Emma's retention would keep and forget. Read-only.
    ///
    /// There is no `retention apply` in this version.
    Plan(RetentionPlanArgs),
}

#[derive(clap::Args, Debug)]
pub struct RetentionPlanArgs {
    /// `project/emma`, or `emma` when unambiguous.
    pub emma: String,
    /// Reference time (RFC 3339). Default: now, recorded in the plan.
    #[arg(long)]
    pub as_of: Option<String>,
    /// What-if: one snapshot per day for N days ending at --as-of (06:00 UTC).
    #[arg(long, conflicts_with = "inventory_json")]
    pub simulate_daily: Option<u32>,
    /// What-if: plan over a supplied JSON list of {id, captured_at, status?, hold?, generation?, fidelity?}.
    #[arg(long)]
    pub inventory_json: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

fn env_or_exit(command: &str, json_out: bool) -> Result<Env, i32> {
    Env::from_host().map_err(|f| emit(command, json_out, exit::FAILURE, Json::Null, "", &[f]))
}

pub fn run_register(a: &RegisterArgs) -> i32 {
    let Ok(mut env) = env_or_exit("register", a.json) else { return exit::FAILURE };
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let opts = register::RegisterOpts { path: a.path.clone(), all: a.all, dry_run: a.dry_run, update: a.update, prune: a.prune };
    let o = register::register(&opts, &mut env, &cwd);
    emit("register", a.json, o.exit, o.data, &o.human, &o.findings)
}

pub fn run_adopt(a: &AdoptArgs) -> i32 {
    let Ok(mut env) = env_or_exit("adopt", a.json) else { return exit::FAILURE };
    let o = adopt::adopt(a.write, &mut env);
    emit("adopt", a.json, o.exit, o.data, &o.human, &o.findings)
}

fn open_rows(env: &Env) -> Result<(Vec<EmmaRow>, Vec<registry::ManifestRow>), Finding> {
    match Registry::open_read(&env.state_dir)? {
        None => Ok((Vec::new(), Vec::new())),
        Some(r) => {
            let rows = r.emmas().map_err(|e| Finding::of("E_REGISTRY", e.to_string()))?;
            let ms = r.manifests().map_err(|e| Finding::of("E_REGISTRY", e.to_string()))?;
            Ok((rows, ms))
        }
    }
}

/// `delodocs/vault`, or `vault` when exactly one non-retired Emma has that id.
pub fn resolve_emma<'a>(rows: &'a [EmmaRow], spec: &str) -> Result<&'a EmmaRow, (i32, Finding)> {
    let live = |r: &&EmmaRow| r.sync_state != "retired";
    let found: Vec<&EmmaRow> = match spec.split_once('/') {
        Some((p, e)) => rows.iter().filter(|r| r.project_key == p && r.emma_id == e).collect(),
        None => {
            let all: Vec<&EmmaRow> = rows.iter().filter(|r| r.emma_id == spec).collect();
            let alive: Vec<&EmmaRow> = all.iter().copied().filter(live).collect();
            if alive.is_empty() { all } else { alive }
        }
    };
    match found.as_slice() {
        [one] => Ok(one),
        [] => Err((exit::USAGE, Finding::of("E_UNKNOWN_EMMA", format!("no registered Emma {spec:?}; run `forever-ago projects`")))),
        many => Err((
            exit::USAGE,
            Finding::of(
                "E_AMBIGUOUS_EMMA",
                format!("{spec:?} is ambiguous: {}", many.iter().map(|r| r.label()).collect::<Vec<_>>().join(", ")),
            ),
        )),
    }
}

pub fn projects(env: &mut Env, project: Option<&str>, offline: bool) -> (i32, Json, String, Vec<Finding>) {
    let (rows, manifests) = match open_rows(env) {
        Ok(x) => x,
        Err(f) => return (exit::FAILURE, Json::Null, String::new(), vec![f]),
    };
    let mut ctx = plan::Ctx {
        home: env.home.clone(),
        candybar: candybar::Cache::new(env.candybar.clone(), offline),
        jobs: env.jobs.take(),
        manifests,
    };
    let mut findings = Vec::new();
    let mut out = Vec::new();
    for r in rows.iter().filter(|r| project.is_none_or(|p| r.project_key == p)) {
        let (e, f) = plan::evaluate(r, &mut ctx, None);
        findings.extend(f);
        out.push(e);
    }
    env.jobs = ctx.jobs.take();
    let human = plan::render_projects(&out, &env.home);
    let data = json!({
        "registry": env.state_dir.join(registry::DB_NAME).display().to_string(),
        "offline": offline,
        "identity": if offline { "unverified" } else { "registered" },
        "emmas": out,
    });
    (exit::OK, data, human, findings)
}

pub fn run_projects(a: &ProjectsArgs) -> i32 {
    let Ok(mut env) = env_or_exit("projects", a.json) else { return exit::FAILURE };
    let (code, data, human, findings) = projects(&mut env, a.project.as_deref(), a.offline);
    emit("projects", a.json, code, data, &human, &findings)
}

pub fn plan_one(env: &mut Env, spec: &str, measure: Option<usize>, offline: bool) -> (i32, Json, String, Vec<Finding>) {
    let (rows, manifests) = match open_rows(env) {
        Ok(x) => x,
        Err(f) => return (exit::FAILURE, Json::Null, String::new(), vec![f]),
    };
    let row = match resolve_emma(&rows, spec) {
        Ok(r) => r.clone(),
        Err((code, f)) => return (code, Json::Null, String::new(), vec![f]),
    };
    let mut ctx = plan::Ctx {
        home: env.home.clone(),
        candybar: candybar::Cache::new(env.candybar.clone(), offline),
        jobs: env.jobs.take(),
        manifests,
    };
    let (e, findings) = plan::evaluate(&row, &mut ctx, measure);
    env.jobs = ctx.jobs.take();
    let human = plan::render_plan(&e, &env.home);
    (exit::OK, e, human, findings)
}

pub fn run_plan(a: &PlanArgs) -> i32 {
    let Ok(mut env) = env_or_exit("plan", a.json) else { return exit::FAILURE };
    let (code, data, human, findings) = plan_one(&mut env, &a.emma, a.measure.then_some(a.max_entries), a.offline);
    emit("plan", a.json, code, data, &human, &findings)
}

fn parse_time(s: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(s).map(|t| t.with_timezone(&Utc)).map_err(|e| format!("{s:?} is not RFC 3339: {e}"))
}

/// The live inventory of a `tar-gz-dir@1` Emma: one snapshot per archive,
/// captured_at = file mtime (UTC), the filename date when mtime is unusable.
pub fn tar_inventory(dest: &Path, prefix: &str) -> Vec<retention::Snap> {
    let Ok(rd) = std::fs::read_dir(dest) else { return Vec::new() };
    let mut out = Vec::new();
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(date) = crate::parse_backup_date(prefix, &name) else { continue };
        if !e.file_type().is_ok_and(|t| t.is_file()) {
            continue;
        }
        let (captured_at, source) = match e.metadata().ok().and_then(|m| m.modified().ok()) {
            Some(t) => (DateTime::<Utc>::from(t), "mtime"),
            None => (date.and_hms_opt(0, 0, 0).expect("midnight").and_utc(), "filename"),
        };
        let mut s = retention::Snap::new(name.clone(), captured_at);
        s.date = date;
        s.time_source = source;
        if !dest.join(format!("{name}.sha256")).is_file() {
            s.status = retention::SnapStatus::Incomplete;
        }
        out.push(s);
    }
    out
}

fn inventory_from_json(path: &Path) -> Result<Vec<retention::Snap>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let v: Json = serde_json::from_str(&text).map_err(|e| format!("{} is not JSON: {e}", path.display()))?;
    let items = v.as_array().or_else(|| v.get("snapshots").and_then(Json::as_array)).ok_or("expected a JSON array of snapshots")?;
    items
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let id = s.get("id").and_then(Json::as_str).ok_or(format!("snapshot {i}: missing id"))?;
            let at = s.get("captured_at").and_then(Json::as_str).ok_or(format!("snapshot {i}: missing captured_at"))?;
            let mut snap = retention::Snap::new(id, parse_time(at)?);
            if let Some(st) = s.get("status").and_then(Json::as_str) {
                snap.status = retention::SnapStatus::parse(st).ok_or(format!("snapshot {i}: unknown status {st:?}"))?;
            }
            snap.hold = s.get("hold").and_then(Json::as_str).map(str::to_string);
            snap.generation = s.get("generation").and_then(Json::as_i64).unwrap_or(1);
            if let Some(f) = s.get("fidelity").and_then(Json::as_str) {
                snap.fidelity = f.to_string();
            }
            snap.time_source = "supplied";
            Ok(snap)
        })
        .collect()
}

pub fn retention_plan(env: &Env, a: &RetentionPlanArgs) -> (i32, Json, String, Vec<Finding>) {
    let fail = |code: i32, f: Finding| (code, Json::Null, String::new(), vec![f]);
    let (rows, _) = match open_rows(env) {
        Ok(x) => x,
        Err(f) => return fail(exit::FAILURE, f),
    };
    let row = match resolve_emma(&rows, &a.emma) {
        Ok(r) => r.clone(),
        Err((code, f)) => return fail(code, f),
    };
    let as_of = match a.as_of.as_deref().map(parse_time).transpose() {
        Ok(t) => t.unwrap_or_else(Utc::now),
        Err(e) => return fail(exit::USAGE, Finding::of("E_USAGE", e)),
    };
    let (strategy, params) = row.strategy("retention");
    if row.sole_copy && !matches!(strategy.as_str(), "keep-all@1" | "observe-only@1") {
        return fail(exit::REFUSED, Finding::of("E_SOLE_COPY_RETENTION", format!("{} is a sole copy; {strategy} would thin it", row.label())));
    }
    let mut findings = Vec::new();
    let (snaps, what_if, source) = if let Some(n) = a.simulate_daily {
        (retention::simulate_daily(n, as_of), true, format!("simulated: one snapshot per day for {n} days ending {}", as_of.format("%Y-%m-%d")))
    } else if let Some(p) = &a.inventory_json {
        match inventory_from_json(p) {
            Ok(s) => (s, true, format!("supplied: {}", p.display())),
            Err(e) => return fail(exit::USAGE, Finding::of("E_USAGE", e)),
        }
    } else {
        let (backend, bp) = row.strategy("backend");
        match (backend.as_str(), &row.dest_canonical, bp["prefix"].as_str()) {
            ("tar-gz-dir@1", Some(d), Some(prefix)) => (tar_inventory(Path::new(d), prefix), false, format!("live: {d}/{prefix}-*.tar.gz")),
            _ => {
                findings.push(Finding::of(
                    "I_NO_INVENTORY",
                    format!("{backend} has no readable inventory in this version (nothing has been captured); try --simulate-daily N"),
                ));
                (Vec::new(), false, "live: none".to_string())
            }
        }
    };
    let p = match retention::plan(&strategy, &params, &snaps, as_of) {
        Ok(p) => p,
        Err(e) => return fail(exit::REFUSED, Finding::of("E_UNKNOWN_STRATEGY", e)),
    };
    let mut data = retention::to_json(&p, what_if);
    data["emma"] = json!(row.label());
    data["inventory"] = json!({"source": source, "snapshots": snaps.len()});
    let human = render_retention(&row, &p, what_if, &source);
    (exit::OK, data, human, findings)
}

fn render_retention(row: &EmmaRow, p: &retention::Plan, what_if: bool, source: &str) -> String {
    let mut o = format!(
        "{}  retention {}  as of {}{}\n  inventory {source}\n  executed by {}; plan_digest {}\n",
        row.label(),
        p.strategy,
        p.as_of.format("%Y-%m-%dT%H:%M:%SZ"),
        if what_if { "  (WHAT-IF: no apply authority)" } else { "  (no `retention apply` exists in this version)" },
        p.executed_by,
        &p.digest[..16]
    );
    for n in &p.notes {
        o.push_str(&format!("  note: {n}\n"));
    }
    for part in &p.partitions {
        let kept = part.decisions.iter().filter(|d| d.keep).count();
        o.push_str(&format!(
            "  generation {}: keep {kept} of {}{}; longest gap between kept points {}\n",
            part.generation,
            part.decisions.len(),
            if part.frozen { " (FROZEN)" } else { "" },
            part.max_gap_s.map(|g| format!("{} d", retention::days(g))).unwrap_or_else(|| "-".into())
        ));
        if part.bands.len() == 4 {
            o.push_str("    band         range        members  kept  forget  max gap\n");
            for (i, b) in part.bands.iter().enumerate() {
                let (name, range) = retention::BANDS[i];
                o.push_str(&format!(
                    "    {name:<12} {range:<12} {:>7}  {:>4}  {:>6}  {}\n",
                    b.members,
                    b.kept,
                    b.forgotten,
                    b.max_gap_s.map(|g| format!("{} d", retention::days(g))).unwrap_or_else(|| "-".into())
                ));
            }
        }
        let shown = if part.decisions.len() > 60 { part.decisions.iter().filter(|d| d.keep).collect::<Vec<_>>() } else { part.decisions.iter().collect() };
        if part.decisions.len() > 60 {
            o.push_str(&format!("    kept points (forgotten ones omitted; --json lists all {}):\n", part.decisions.len()));
        }
        for d in shown {
            o.push_str(&format!(
                "    {:<6} {}  {}  {}\n",
                if d.keep { "keep" } else { "forget" },
                d.captured_at.format("%Y-%m-%d %H:%MZ"),
                d.id,
                d.reasons.join(" ")
            ));
        }
    }
    for (id, why) in &p.untouched {
        o.push_str(&format!("    untouched {id}  {why}\n"));
    }
    o
}

pub fn run_retention(a: &RetentionArgs) -> i32 {
    match &a.command {
        RetentionCommand::Plan(p) => {
            let Ok(env) = env_or_exit("retention plan", p.json) else { return exit::FAILURE };
            let (code, data, human, findings) = retention_plan(&env, p);
            emit("retention plan", p.json, code, data, &human, &findings)
        }
    }
}
