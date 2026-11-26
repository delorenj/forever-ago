//! `forever-ago register`: the only writer of the registry.
//!
//! Validate every manifest in the run, resolve identity through pjangler,
//! then apply everything in one transaction, or nothing. Vocabulary follows
//! Candybar's `register`: new, unchanged, drift:<fields>, moved, orphan,
//! conflict:<project>. Registration enables nothing and changes no clock.

use super::candybar::{Cache, Show};
use super::envelope::{exit, has_errors, Finding};
use super::identity::{self, in_linked_worktree, Identity};
use super::manifest::{self, EmmaDef, Loaded, Mode, MANIFEST_NAME};
use super::plan::{compare_legacy, matching_job};
use super::registry::{EmmaRow, EmmaWrite, ManifestRow, Registry};
use super::Env;
use crate::jobs::{self, canonical_or_normalized};
use serde_json::{json, Value as Json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct RegisterOpts {
    pub path: Option<PathBuf>,
    pub all: bool,
    pub dry_run: bool,
    pub update: bool,
    pub prune: bool,
}

pub struct Outcome {
    pub exit: i32,
    pub data: Json,
    pub human: String,
    pub findings: Vec<Finding>,
}

impl Outcome {
    pub fn fail(code: i32, f: Finding) -> Self {
        Outcome { exit: code, data: Json::Null, human: String::new(), findings: vec![f] }
    }
}

/// The nearest `.emma.toml` from `dir` upward, stopping at the git toplevel
/// (or at $HOME outside a repository).
pub fn find_manifest(dir: &Path, home: &Path) -> Option<PathBuf> {
    let top = identity::git_root(dir);
    let stop = if top.join(".git").exists() { Some(top) } else { None };
    let mut cur = Some(dir);
    while let Some(d) = cur {
        let m = d.join(MANIFEST_NAME);
        if m.is_file() {
            return Some(m);
        }
        if stop.as_deref() == Some(d) || d == home {
            return None;
        }
        cur = d.parent();
    }
    None
}

#[derive(Clone, Debug, PartialEq)]
enum State {
    New { revive: bool },
    Unchanged,
    Drift(Vec<String>),
    Moved { from: String },
    IdentityChanged { old_key: String },
    Conflict(String),
}

impl State {
    fn label(&self) -> String {
        match self {
            State::New { .. } => "new".into(),
            State::Unchanged => "unchanged".into(),
            State::Drift(f) => format!("drift:{}", f.join(",")),
            State::Moved { .. } => "moved".into(),
            State::IdentityChanged { .. } => "identity-changed".into(),
            State::Conflict(p) => format!("conflict:{p}"),
        }
    }
}

struct Item<'a> {
    manifest: &'a Loaded,
    key: String,
    emma: &'a EmmaDef,
    state: State,
    old: Option<EmmaRow>,
}

#[allow(clippy::too_many_arguments)]
fn write_of<'a>(item: &'a Item, def_json: &'a Json, def_sha: &'a str, contract: &'a str, decl: &'a str, src: &'a Option<String>, dst: &'a Option<String>, mpath: &'a str) -> EmmaWrite<'a> {
    EmmaWrite {
        project_key: &item.key,
        emma_id: &item.emma.id,
        manifest_path: mpath,
        mode: item.emma.mode.label(),
        sole_copy: item.emma.sole_copy,
        definition: def_json,
        definition_sha: def_sha,
        contract_sha: contract,
        source_declared: decl,
        source_canonical: src.as_deref(),
        dest_canonical: dst.as_deref(),
        candybar_ref: item.emma.candybar_ref.as_deref(),
    }
}

fn path_str(p: &Option<PathBuf>) -> Option<String> {
    p.as_ref().map(|p| p.display().to_string())
}

/// Definition plus the canonical columns, compared field by field.
fn changes(row: &EmmaRow, e: &EmmaDef) -> Vec<String> {
    let mut f = manifest::drift_fields(&row.definition, &e.definition_json());
    if row.source_canonical != path_str(&e.source_canonical) {
        f.push("source_canonical".into());
    }
    if row.dest_canonical != path_str(&e.dest_canonical) {
        f.push("dest_canonical".into());
    }
    f
}

fn rel(path: &Path, home: &Path) -> String {
    jobs::tilde(path, home)
}

pub fn register(opts: &RegisterOpts, env: &mut Env, cwd: &Path) -> Outcome {
    let home = env.home.clone();
    let mut findings: Vec<Finding> = Vec::new();

    // 1. Which manifests.
    let mut targets: Vec<PathBuf> = Vec::new();
    if opts.all {
        match identity::list_projects(&env.pj, &home) {
            Ok(projects) => {
                for (_, repo) in projects {
                    let m = repo.join(MANIFEST_NAME);
                    if m.is_file() {
                        targets.push(m);
                    }
                }
            }
            Err(f) => return Outcome::fail(exit::FAILURE, f),
        }
    } else {
        let start = opts.path.clone().unwrap_or_else(|| cwd.to_path_buf());
        let start = if start.is_absolute() { start } else { cwd.join(start) };
        if start.is_file() {
            targets.push(start);
        } else if start.is_dir() {
            match find_manifest(&canonical_or_normalized(&start), &home) {
                Some(m) => targets.push(m),
                None => {
                    return Outcome::fail(
                        exit::USAGE,
                        Finding::of("E_NO_MANIFEST", format!("no {MANIFEST_NAME} in {} or its parents up to the git toplevel", start.display())),
                    );
                }
            }
        } else {
            return Outcome::fail(exit::USAGE, Finding::of("E_NO_MANIFEST", format!("{} does not exist", start.display())));
        }
    }
    let mut seen = HashSet::new();
    targets.retain(|t| seen.insert(canonical_or_normalized(t)));

    // 2. Load and validate every manifest (all-or-nothing per file).
    let loaded: Vec<Loaded> = targets.iter().map(|t| manifest::load(t, &home, true)).collect();
    for l in &loaded {
        findings.extend(l.findings.iter().cloned());
    }
    let mut refused = false;
    let mut identity_failed = false;

    // 3. Identity and ownership, per valid manifest.
    let mut idents: HashMap<PathBuf, Identity> = HashMap::new();
    for l in loaded.iter().filter(|l| l.ok()) {
        let label = l.path.display().to_string();
        match identity::resolve(&env.pj, &l.dir, &home) {
            Err(f) => {
                identity_failed = true;
                findings.push(f.at(&label));
            }
            Ok(id) => {
                let repo = id.repo_path().to_path_buf();
                let under = l.dir == repo || l.dir.starts_with(&repo);
                if !under || in_linked_worktree(&l.dir) {
                    refused = true;
                    findings.push(
                        Finding::of(
                            "E_NOT_OWNER",
                            format!(
                                "{MANIFEST_NAME} must sit in the project's canonical checkout ({}); {} is {}",
                                repo.display(),
                                l.dir.display(),
                                if under { "a linked git worktree" } else { "outside it" }
                            ),
                        )
                        .at(&label),
                    );
                    continue;
                }
                if let Identity::Unbound { root } = &id {
                    findings.push(
                        Finding::of("W_UNBOUND", format!("pjangler does not know {}; keyed by path (run `pj init` to bind it)", root.display())).at(&label),
                    );
                }
                idents.insert(l.path.clone(), id);
            }
        }
    }

    // 4. Adopted Emmas must describe a live legacy job.
    let needs_jobs = loaded.iter().filter(|l| l.ok()).flat_map(|l| &l.emmas).any(|e| e.mode == Mode::Adopted);
    if needs_jobs && env.jobs.is_none() {
        env.jobs = Some(jobs::discover_host(false).map(|d| d.jobs).unwrap_or_default());
    }
    for l in loaded.iter().filter(|l| l.ok()) {
        for e in l.emmas.iter().filter(|e| e.mode == Mode::Adopted) {
            let jobs = env.jobs.as_deref().unwrap_or(&[]);
            let (Some(src), Some(dst)) = (&e.source_canonical, &e.dest_canonical) else { continue };
            let prefix = e.backend.str_param("prefix").unwrap_or("");
            match matching_job(jobs, src, dst, prefix) {
                None => findings.push(
                    Finding::of(
                        "E_ADOPT_NO_JOB",
                        format!("mode = \"adopted\" but no legacy forever-ago job backs up {} to {}/{prefix}-*", src.display(), dst.display()),
                    )
                    .emma(&e.id)
                    .at(l.path.display().to_string()),
                ),
                Some(job) => {
                    let drift = compare_legacy(job, &e.definition_json(), &l.dir, &home);
                    if !drift.is_empty() {
                        findings.push(
                            Finding::of("W_LEGACY_DRIFT", format!("drift:legacy-job ({}) against {}", drift.join(","), job.unit_name()))
                                .emma(&e.id)
                                .at(l.path.display().to_string()),
                        );
                    }
                }
            }
        }
    }

    // 5. Candybar refs: existence is information, never a register error.
    let mut cb = Cache::new(env.candybar.clone(), false);
    for l in loaded.iter().filter(|l| l.ok()) {
        for e in &l.emmas {
            if let Some(r) = &e.candybar_ref {
                match cb.show(r) {
                    Show::Found(_) => {}
                    Show::Missing => findings.push(Finding::of("W_CANDYBAR_MISSING", format!("candybar {r:?} does not exist (yet)")).emma(&e.id)),
                    Show::Unknown(why) => findings.push(Finding::of("I_CANDYBAR_UNKNOWN", format!("candybar {r:?} not checked: {why}")).emma(&e.id)),
                }
            }
        }
    }

    // 6. The registry: locked for writing, or read-only for --dry-run.
    let mut reg_w: Option<Registry> = None;
    let (rows, manifests): (Vec<EmmaRow>, Vec<ManifestRow>) = if opts.dry_run {
        match Registry::open_read(&env.state_dir) {
            Ok(Some(r)) => (r.emmas().unwrap_or_default(), r.manifests().unwrap_or_default()),
            Ok(None) => (Vec::new(), Vec::new()),
            Err(f) => return Outcome::fail(exit::FAILURE, f),
        }
    } else {
        match Registry::open_write(&env.state_dir) {
            Ok(r) => {
                let rows = r.emmas().unwrap_or_default();
                let ms = r.manifests().unwrap_or_default();
                reg_w = Some(r);
                (rows, ms)
            }
            Err((code, f)) => return Outcome::fail(code, f),
        }
    };

    // 7. Classify every Emma against the registry.
    let run_paths: HashSet<String> = loaded.iter().map(|l| l.path.display().to_string()).collect();
    let mut items: Vec<Item> = Vec::new();
    let mut orphans: Vec<(EmmaRow, &Loaded)> = Vec::new();
    let mut owners: HashMap<(String, String), String> = HashMap::new();
    for l in loaded.iter().filter(|l| l.ok()) {
        let Some(id) = idents.get(&l.path) else { continue };
        let key = id.project_key();
        let mpath = l.path.display().to_string();
        let old_key = manifests.iter().find(|m| m.manifest_path == mpath && m.project_key != key).map(|m| m.project_key.clone());
        let lookup_key = old_key.clone().unwrap_or_else(|| key.clone());
        for e in &l.emmas {
            if let Some(other) = owners.insert((key.clone(), e.id.clone()), mpath.clone()) {
                refused = true;
                findings.push(
                    Finding::of("E_CONFLICT", format!("conflict:duplicate-owner: {other} and {mpath} both declare {key}/{}", e.id)).emma(&e.id),
                );
            }
            let row = rows.iter().find(|r| r.project_key == lookup_key && r.emma_id == e.id).cloned();
            let state = match (&row, &old_key) {
                (_, Some(ok)) => State::IdentityChanged { old_key: ok.clone() },
                (None, None) => State::New { revive: false },
                (Some(r), None) if r.sync_state == "retired" => State::New { revive: true },
                (Some(r), None) if r.manifest_path != mpath => {
                    let other = Path::new(&r.manifest_path);
                    let still_claims = !run_paths.contains(&r.manifest_path)
                        && other.is_file()
                        && manifest::load(other, &home, false).emmas.iter().any(|x| x.id == e.id);
                    if still_claims {
                        refused = true;
                        findings.push(
                            Finding::of(
                                "E_CONFLICT",
                                format!("conflict:duplicate-owner: {} still declares {key}/{}; remove one before registering", r.manifest_path, e.id),
                            )
                            .emma(&e.id)
                            .at(&mpath),
                        );
                        State::Conflict(key.clone())
                    } else {
                        State::Moved { from: r.manifest_path.clone() }
                    }
                }
                (Some(r), None) => {
                    let f = changes(r, e);
                    if f.is_empty() { State::Unchanged } else { State::Drift(f) }
                }
            };
            items.push(Item { manifest: l, key: key.clone(), emma: e, state, old: row });
        }
        let declared: HashSet<&str> = l.emmas.iter().map(|e| e.id.as_str()).collect();
        for r in rows.iter().filter(|r| r.manifest_path == mpath && r.project_key == lookup_key) {
            if !declared.contains(r.emma_id.as_str()) && r.sync_state != "retired" {
                orphans.push((r.clone(), l));
            }
        }
    }

    // Destination claims after this run: E_DEST_COLLISION.
    let in_run: HashSet<(String, String)> = items.iter().map(|i| (i.key.clone(), i.emma.id.clone())).collect();
    let mut claims: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for r in rows.iter().filter(|r| r.sync_state != "retired" && !in_run.contains(&(r.project_key.clone(), r.emma_id.clone()))) {
        if let Some(d) = &r.dest_canonical {
            let prefix = r.definition["backend"]["prefix"].as_str().unwrap_or("").to_string();
            claims.entry((d.clone(), prefix)).or_default().push(r.label());
        }
    }
    for i in &items {
        if let Some(c) = i.emma.dest_claim() {
            claims.entry(c).or_default().push(format!("{}/{}", i.key, i.emma.id));
        }
    }
    for ((dest, prefix), who) in &claims {
        if who.len() > 1 {
            refused = true;
            findings.push(Finding::of(
                "E_DEST_COLLISION",
                format!("{} all write {dest}{}; one destination has one owner", who.join(", "), if prefix.is_empty() { String::new() } else { format!(" prefix {prefix}") }),
            ));
        }
    }

    // --all: registered manifests this run did not visit.
    let mut vanished: Vec<&ManifestRow> = Vec::new();
    if opts.all {
        for m in manifests.iter().filter(|m| !run_paths.contains(&m.manifest_path) && m.state != "moved") {
            if !Path::new(&m.manifest_path).exists() {
                vanished.push(m);
            } else {
                findings.push(Finding::of("W_MANIFEST_NOT_VISITED", "registered manifest is not under any pjangler project's repo_path; left as is").at(&m.manifest_path));
            }
        }
    }

    // 8. Report rows.
    let invalid: Vec<&Loaded> = loaded.iter().filter(|l| !l.ok()).collect();
    let mut report: Vec<Json> = Vec::new();
    let mut human = String::new();
    let mut needs_update = false;
    for i in &items {
        let applied = match &i.state {
            State::New { .. } | State::Unchanged => true,
            State::Drift(_) | State::Moved { .. } | State::IdentityChanged { .. } => opts.update,
            State::Conflict(_) => false,
        };
        if matches!(i.state, State::Drift(_) | State::Moved { .. } | State::IdentityChanged { .. }) && !opts.update {
            needs_update = true;
        }
        let mpath = i.manifest.path.display().to_string();
        report.push(json!({
            "project": i.key, "emma": i.emma.id, "manifest": mpath, "state": i.state.label(),
            "moved_from": if let State::Moved { from } = &i.state { Some(from.clone()) } else { None },
            "applied": applied && !opts.dry_run,
        }));
        human.push_str(&format!("  {:<18} {}/{}  ({})\n", i.state.label(), i.key, i.emma.id, rel(&i.manifest.path, &home)));
    }
    for (r, l) in &orphans {
        let state = if opts.prune { "retired" } else { "orphan" };
        report.push(json!({"project": r.project_key, "emma": r.emma_id, "manifest": l.path.display().to_string(), "state": state, "applied": !opts.dry_run}));
        human.push_str(&format!("  {:<18} {}  (no longer in {})\n", state, r.label(), rel(&l.path, &home)));
        findings.push(Finding::of("W_ORPHAN", format!("{} is registered but no longer declared; `--prune` marks it retired (nothing is deleted)", r.label())));
    }
    for m in &vanished {
        human.push_str(&format!("  {:<18} {}\n", "missing", m.manifest_path));
    }
    for l in &invalid {
        human.push_str(&format!("  {:<18} {}\n", "invalid", rel(&l.path, &home)));
    }

    let code = if refused {
        exit::REFUSED
    } else if !invalid.is_empty() || has_errors(&findings) && !identity_failed {
        exit::USAGE
    } else if identity_failed {
        exit::FAILURE
    } else if needs_update {
        exit::DRIFT
    } else {
        exit::OK
    };

    let data = |applied: bool| {
        json!({
            "registry": env.state_dir.join(super::registry::DB_NAME).display().to_string(),
            "dry_run": opts.dry_run,
            "applied": applied,
            "manifests": loaded.iter().map(|l| json!({"path": l.path.display().to_string(), "sha256": l.sha256, "valid": l.ok()})).collect::<Vec<_>>(),
            "emmas": report,
        })
    };

    // 9. Apply: one transaction, or only the observations when the run is refused.
    let Some(mut reg) = reg_w else {
        human.push_str("dry run: nothing written\n");
        return Outcome { exit: code, data: data(false), human, findings };
    };
    let result = (|| -> rusqlite::Result<bool> {
        let tx = reg.begin()?;
        let findings_json = Json::Array(findings.iter().map(Finding::to_json).collect());
        for l in &invalid {
            let p = l.path.display().to_string();
            if manifests.iter().any(|m| m.manifest_path == p) {
                tx.mark_manifest(&p, "invalid", Some(&findings_json.to_string()))?;
            }
            tx.log("register", Some(&p), "invalid", &findings_json)?;
        }
        if code != exit::OK && code != exit::DRIFT {
            for l in loaded.iter().filter(|l| l.ok()) {
                tx.log("register", Some(&l.path.display().to_string()), "refused", &findings_json)?;
            }
            tx.commit()?;
            return Ok(false);
        }
        for l in loaded.iter().filter(|l| l.ok()) {
            let Some(id) = idents.get(&l.path) else { continue };
            let key = id.project_key();
            tx.upsert_project(&key, id.kind(), id.project_id(), &id.repo_path().display().to_string())?;
            tx.upsert_manifest(&l.path.display().to_string(), &key, &l.sha256, "ok", None)?;
        }
        for i in &items {
            let def = i.emma.definition_json();
            let def_sha = i.emma.definition_sha();
            let contract = i.emma.contract_sha();
            let (src, dst) = (path_str(&i.emma.source_canonical), path_str(&i.emma.dest_canonical));
            let mpath = i.manifest.path.display().to_string();
            let decl = i.emma.source_declared();
            let w = write_of(i, &def, &def_sha, &contract, &decl, &src, &dst, &mpath);
            match (&i.state, &i.old) {
                (State::New { revive: false }, _) => tx.insert_emma(&w, "registered")?,
                (State::New { revive: true }, Some(old)) => {
                    tx.update_emma(&w, old, "revived")?;
                }
                (State::Unchanged, Some(old)) if old.sync_state != "ok" => tx.set_sync_state(&i.key, &i.emma.id, "ok")?,
                (State::Drift(fields), Some(old)) => {
                    if opts.update {
                        tx.update_emma(&w, old, &format!("contract-changed:{}", fields.join(",")))?;
                    } else {
                        tx.set_sync_state(&i.key, &i.emma.id, "drift")?;
                    }
                }
                (State::Moved { from }, Some(old)) if opts.update => {
                    tx.mark_manifest(from, "moved", None)?;
                    tx.update_emma(&w, old, "moved")?;
                }
                (State::IdentityChanged { old_key }, Some(old)) if opts.update => {
                    tx.rekey(old_key, &i.key, &mpath)?;
                    let rekeyed = EmmaRow { project_key: i.key.clone(), ..old.clone() };
                    tx.update_emma(&w, &rekeyed, "identity-changed")?;
                }
                (State::IdentityChanged { .. }, None) if opts.update => tx.insert_emma(&w, "registered")?,
                _ => {}
            }
        }
        for (r, _) in &orphans {
            tx.set_sync_state(&r.project_key, &r.emma_id, if opts.prune { "retired" } else { "orphan" })?;
        }
        for m in &vanished {
            tx.mark_manifest(&m.manifest_path, "missing", None)?;
            for r in rows.iter().filter(|r| r.manifest_path == m.manifest_path && r.sync_state != "retired") {
                tx.set_sync_state(&r.project_key, &r.emma_id, if opts.prune { "retired" } else { "orphan" })?;
            }
        }
        for l in loaded.iter().filter(|l| l.ok()) {
            let p = l.path.display().to_string();
            let outcome = items
                .iter()
                .filter(|i| i.manifest.path == l.path)
                .map(|i| format!("{}={}", i.emma.id, i.state.label()))
                .collect::<Vec<_>>()
                .join(" ");
            tx.log("register", Some(&p), &outcome, &findings_json)?;
        }
        tx.commit()?;
        Ok(true)
    })();
    match result {
        Ok(applied) => {
            human.push_str(&format!(
                "registry {}{}\n",
                rel(&env.state_dir.join(super::registry::DB_NAME), &home),
                if applied { "" } else { " (refused: no definition changed)" }
            ));
            if code == exit::DRIFT {
                human.push_str("drift or moves found: rerun with --update to apply them\n");
            }
            Outcome { exit: code, data: data(applied), human, findings }
        }
        Err(e) => {
            findings.push(Finding::of("E_REGISTRY", format!("registry transaction failed, nothing changed: {e}")));
            Outcome { exit: exit::FAILURE, data: data(false), human, findings }
        }
    }
}
