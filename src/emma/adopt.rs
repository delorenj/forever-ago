//! `forever-ago adopt`: describe each live legacy job as a `.emma.toml` entry.
//!
//! Reads the jobs `forever-ago jobs` finds, binds each to its pjangler
//! project, and prints the manifest that describes it exactly. With
//! `--write` it creates `<repo_path>/.emma.toml` only if absent. It never
//! touches a unit file, Candybar or the migration ledger.

use super::candybar::{Cache, Show};
use super::envelope::{exit, Finding};
use super::identity::{self, Identity};
use super::manifest::{self, slug_ok, MANIFEST_NAME};
use super::register::Outcome;
use super::Env;
use crate::jobs::{self, Job};
use crate::RetentionPolicy;
use serde_json::{json, Value as Json};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};

fn q(s: &str) -> String {
    toml::Value::String(s.to_string()).to_string()
}

/// `voxxy-voices` in project `voxxy` is the Emma `voices`.
pub fn emma_id_for(prefix: &str, project_id: &str) -> String {
    let base = prefix.strip_prefix(&format!("{project_id}-")).filter(|r| !r.is_empty()).unwrap_or(prefix);
    let mut id: String = base
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect();
    while id.contains("--") {
        id = id.replace("--", "-");
    }
    let id = id.trim_matches('-').chars().take(48).collect::<String>().trim_end_matches('-').to_string();
    if slug_ok(&id) { id } else { "backup".into() }
}

/// A path as the manifest should spell it: relative to the project root when
/// inside it, `~/...` under home, else absolute.
fn spell(path: &Path, root: &Path, home: &Path) -> String {
    if let Ok(rest) = path.strip_prefix(root) {
        return if rest.as_os_str().is_empty() { ".".into() } else { rest.display().to_string() };
    }
    jobs::tilde(path, home)
}

pub struct Proposal {
    pub project_id: String,
    pub repo_path: PathBuf,
    pub entries: Vec<(String, String)>, // (emma id, TOML block)
    pub sources: Vec<String>,
}

impl Proposal {
    pub fn render(&self, today: &str) -> String {
        let mut s = format!(
            "# Written by `forever-ago adopt` on {today} from {}.\n\
             # mode = \"adopted\": the legacy job captures and prunes by its own flags. Emma records\n\
             # and checks it and never runs, prunes or reschedules it. Candybar owns when.\n\
             schema_version = 1\n",
            self.sources.join(", ")
        );
        for (_, block) in &self.entries {
            s.push('\n');
            s.push_str(block);
        }
        s
    }
}

fn block(id: &str, description: &str, job: &Job, root: &Path, home: &Path, candybar_ref: Option<&str>) -> Option<String> {
    let mut b = String::new();
    b.push_str("[[emmas]]\n");
    b.push_str(&format!("id          = {}\n", q(id)));
    b.push_str(&format!("description = {}\n", q(description)));
    b.push_str("mode        = \"adopted\"\n\n");
    b.push_str("[emmas.source]\nkind = \"files\"\n");
    b.push_str(&format!("path = {}\n", q(&spell(&job.source, root, home))));
    let ex = job.cli_excludes();
    if !ex.is_empty() {
        b.push_str(&format!("exclude = [{}]\n", ex.iter().map(|e| q(e)).collect::<Vec<_>>().join(", ")));
    }
    if let Some(f) = job.exclude_from() {
        b.push_str(&format!("exclude_from = {}\n", q(&spell(f, root, home))));
    }
    b.push_str("\n[emmas.capture]\nstrategy = \"files@1\"\n\n");
    b.push_str("[emmas.backend]\nstrategy = \"tar-gz-dir@1\"\n");
    b.push_str(&format!("dir      = {}\n", q(&jobs::tilde(&job.dest_dir, home))));
    b.push_str(&format!("prefix   = {}\n\n", q(job.prefix.as_deref()?)));
    match job.retention()? {
        RetentionPolicy::Count(n) => b.push_str(&format!("[emmas.retention]\nstrategy = \"legacy-count@1\"\nkeep     = {n}\n\n")),
        RetentionPolicy::Gfs { daily, weekly, monthly } => b.push_str(&format!(
            "[emmas.retention]\nstrategy = \"legacy-gfs@1\"\ndaily    = {daily}\nweekly   = {weekly}\nmonthly  = {monthly}\n\n"
        )),
    }
    b.push_str("[emmas.fidelity]\nstrategy = \"full@1\"\n");
    if let Some(r) = candybar_ref {
        b.push_str(&format!("\n[emmas.candybar]\nref = {}\n", q(r)));
    }
    Some(b)
}

fn clean_description(s: &str) -> String {
    let s: String = s.chars().filter(|c| !c.is_control()).collect();
    if s.chars().count() <= 200 { s } else { s.chars().take(197).collect::<String>() + "..." }
}

pub fn adopt(write: bool, env: &mut Env) -> Outcome {
    let home = env.home.clone();
    let mut findings = Vec::new();
    if env.jobs.is_none() {
        match jobs::discover_host(false) {
            Ok(d) => env.jobs = Some(d.jobs),
            Err(e) => return Outcome::fail(exit::FAILURE, Finding::of("E_DISCOVERY", format!("{e:#}"))),
        }
    }
    let mut cb = Cache::new(env.candybar.clone(), false);
    let mut by_project: BTreeMap<String, Proposal> = BTreeMap::new();
    let all_jobs = env.jobs.take().unwrap_or_default();
    for job in &all_jobs {
        let what = job.describe();
        let Some(prefix) = job.prefix.clone() else { continue };
        if job.retention().is_none() {
            findings.push(Finding::of("W_ADOPT_SKIPPED", format!("{what}: its retention flags do not parse; fix the unit first")));
            continue;
        }
        if !job.once() {
            findings.push(Finding::of(
                "W_LEGACY_DAEMON_CLOCK",
                format!("{what} runs forever-ago's own daemon loop, a second clock; adopt it only after Candybar starts it with --once"),
            ));
            continue;
        }
        let dir = if job.source.is_dir() { job.source.clone() } else { job.source.parent().map(Path::to_path_buf).unwrap_or_default() };
        let (project_id, repo) = match identity::resolve(&env.pj, &dir, &home) {
            Ok(Identity::Pjangler { project_id, repo_path }) => (project_id, repo_path),
            Ok(Identity::Unbound { .. }) => {
                findings.push(Finding::of("W_ADOPT_NO_PROJECT", format!("{what}: {} is in no pjangler project; nothing to adopt into", dir.display())));
                continue;
            }
            Err(f) => {
                findings.push(f);
                continue;
            }
        };
        let source = std::fs::canonicalize(&job.source).unwrap_or(job.source.clone());
        if !source.starts_with(&repo) {
            findings.push(Finding::of("W_ADOPT_NO_PROJECT", format!("{what}: source {} is outside {}", source.display(), repo.display())));
            continue;
        }

        // The Candybar ref is the unit stem, kept only when Candybar confirms it starts this unit.
        let mut candybar_ref = None;
        let mut description = format!("Adopted from {}", job.unit_name());
        if job.is_systemd()
            && let Some(stem) = job.unit_name().strip_suffix(".service")
            && slug_ok(stem)
        {
            match cb.show(stem) {
                Show::Found(v) => {
                    let unit = v["runner"]["unit"].as_str();
                    if v["runner"]["kind"].as_str() == Some("systemd") && unit == Some(job.unit_name()) {
                        candybar_ref = Some(stem.to_string());
                        if let Some(d) = v["description"].as_str().filter(|d| !d.trim().is_empty()) {
                            description = clean_description(d);
                        }
                    } else {
                        findings.push(Finding::of(
                            "W_CANDYBAR_REF_UNCONFIRMED",
                            format!("candybar {stem:?} exists but does not start {} (runner {}); ref dropped", job.unit_name(), v["runner"]),
                        ));
                    }
                }
                Show::Missing => findings.push(Finding::of(
                    "W_CANDYBAR_REF_UNCONFIRMED",
                    format!("no candybar named {stem:?} starts {}; ref dropped (the Emma shows `unscheduled`)", job.unit_name()),
                )),
                Show::Unknown(why) => findings.push(Finding::of("W_CANDYBAR_REF_UNCONFIRMED", format!("candybar {stem:?} not checked ({why}); ref dropped"))),
            }
        }

        let p = by_project.entry(project_id.clone()).or_insert_with(|| Proposal {
            project_id: project_id.clone(),
            repo_path: repo.clone(),
            entries: Vec::new(),
            sources: Vec::new(),
        });
        let mut id = emma_id_for(&prefix, &project_id);
        let base = id.clone();
        let mut n = 2;
        while p.entries.iter().any(|(e, _)| *e == id) {
            id = format!("{base}-{n}");
            n += 1;
        }
        let Some(b) = block(&id, &description, job, &repo, &home, candybar_ref.as_deref()) else { continue };
        p.entries.push((id, b));
        p.sources.push(job.unit_name().to_string());
    }
    env.jobs = Some(all_jobs);

    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let mut code = exit::OK;
    let mut human = String::new();
    let mut out = Vec::new();
    for p in by_project.values() {
        let text = p.render(&today);
        let target = p.repo_path.join(MANIFEST_NAME);
        // The proposal must itself validate, or it is not printed as one.
        let (parsed, problems) = manifest::parse_str(&text, &p.repo_path, &home, true);
        let valid = !problems.iter().any(Finding::is_error) && parsed.len() == p.entries.len();
        if !valid {
            findings.extend(problems.into_iter().filter(Finding::is_error).map(|f| f.at(target.display().to_string())));
        }
        let mut written = false;
        if write && valid {
            match OpenOptions::new().write(true).create_new(true).open(&target) {
                Ok(mut f) => {
                    if let Err(e) = f.write_all(text.as_bytes()) {
                        findings.push(Finding::of("E_WRITE", format!("cannot write {}: {e}", target.display())));
                        code = exit::FAILURE;
                    } else {
                        written = true;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    code = code.max(exit::REFUSED);
                    findings.push(Finding::of(
                        "E_MANIFEST_EXISTS",
                        format!("{} exists and is never edited by adopt; merge the printed entries by hand", target.display()),
                    ));
                }
                Err(e) => {
                    findings.push(Finding::of("E_WRITE", format!("cannot create {}: {e}", target.display())));
                    code = exit::FAILURE;
                }
            }
        }
        human.push_str(&format!(
            "# --- {} ({}) {}\n{}\n",
            p.project_id,
            jobs::tilde(&target, &home),
            if written { "WRITTEN" } else if write { "not written" } else { "proposal" },
            text
        ));
        out.push(json!({
            "project": p.project_id, "repo_path": p.repo_path.display().to_string(), "manifest": target.display().to_string(),
            "emmas": p.entries.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(), "valid": valid,
            "written": written, "toml": text,
        }));
    }
    if by_project.is_empty() {
        human.push_str("no legacy forever-ago job inside a pjangler project\n");
    }
    if code == exit::OK && findings.iter().any(Finding::is_error) {
        code = exit::USAGE;
    }
    Outcome { exit: code, data: Json::Array(out), human, findings }
}
