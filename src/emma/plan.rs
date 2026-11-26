//! `plan` and `projects`: read-only evaluation of registered Emmas.
//!
//! Nothing here writes: not the registry, not a source, not a destination,
//! not Candybar. The coverage verdict is computed on demand and never stored,
//! and no code path here can produce a green verdict (slice 1 has no
//! independent failure domain, no verified integrity and no restore proof).

use super::candybar::{self, Cache};
use super::envelope::Finding;
use super::manifest;
use super::registry::{EmmaRow, ManifestRow};
use super::strategies::{self, resolve_path, Status};
use crate::jobs::{self, canonical_or_normalized, Job};
use crate::{human_size, is_excluded, list, read_exclude_file, ExcludePattern, RetentionPolicy};
use chrono::{DateTime, Local, SecondsFormat, Utc};
use serde_json::{json, Value as Json};
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// Every verdict slice 1 can emit. None of them is green.
#[cfg(test)]
pub const VERDICTS: [&str; 4] = ["broken:source-missing", "none", "local-only", "unknown"];

pub struct Ctx {
    pub home: PathBuf,
    pub candybar: Cache,
    /// Legacy jobs, discovered on first need (tests inject them).
    pub jobs: Option<Vec<Job>>,
    pub manifests: Vec<ManifestRow>,
}

impl Ctx {
    pub fn jobs(&mut self) -> &[Job] {
        if self.jobs.is_none() {
            self.jobs = Some(jobs::discover_host(false).map(|d| d.jobs).unwrap_or_default());
        }
        self.jobs.as_deref().unwrap_or(&[])
    }
}

fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn nearest_existing(path: &Path) -> Option<PathBuf> {
    let mut cur = Some(path);
    while let Some(p) = cur {
        if p.exists() {
            return Some(p.to_path_buf());
        }
        cur = p.parent();
    }
    None
}

fn device(path: &Path) -> Option<u64> {
    fs::metadata(path).ok().map(|m| m.dev())
}

fn dev_label(dev: u64) -> String {
    // Linux encoding of st_dev: major in bits 8..20 (+ high bits), minor in 0..8 (+ 20..).
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
    let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
    format!("{major}:{minor}")
}

fn mtime_utc(m: &fs::Metadata) -> Option<DateTime<Utc>> {
    m.modified().ok().map(DateTime::<Utc>::from)
}

/// A shallow look at the source: top-level entries only, nothing recursive.
fn shallow_summary(path: &Path) -> Json {
    let Ok(meta) = fs::metadata(path) else { return json!({"readable": false}) };
    if meta.is_file() {
        return json!({"readable": true, "type": "file", "bytes": meta.len(), "newest": mtime_utc(&meta).map(ts)});
    }
    let Ok(rd) = fs::read_dir(path) else { return json!({"readable": false, "type": "dir"}) };
    let (mut entries, mut files, mut dirs, mut bytes) = (0u64, 0u64, 0u64, 0u64);
    let mut newest: Option<DateTime<Utc>> = None;
    for e in rd.flatten() {
        entries += 1;
        let Ok(m) = e.metadata() else { continue };
        if m.is_dir() {
            dirs += 1;
        } else if m.is_file() {
            files += 1;
            bytes += m.len();
            if let Some(t) = mtime_utc(&m) {
                newest = Some(newest.map_or(t, |n| n.max(t)));
            }
        }
    }
    json!({
        "readable": true, "type": "dir", "scope": "top-level only",
        "entries": entries, "files": files, "dirs": dirs, "bytes": bytes, "bytes_human": human_size(bytes),
        "newest": newest.map(ts),
    })
}

/// `--measure`: a bounded, read-only walk honouring the declared excludes.
fn measure(root: &Path, excludes: &[ExcludePattern], max_entries: usize) -> Json {
    let (mut entries, mut bytes, mut excluded) = (0usize, 0u64, 0usize);
    let mut truncated = false;
    let mut stack = vec![root.to_path_buf()];
    'walk: while let Some(dir) = stack.pop() {
        let Ok(rd) = fs::read_dir(&dir) else { continue };
        for e in rd.flatten() {
            let path = e.path();
            let Ok(ft) = e.file_type() else { continue };
            let rel = path.strip_prefix(root).unwrap_or(&path).to_path_buf();
            if is_excluded(&rel, ft.is_dir(), excludes) {
                excluded += 1;
                continue;
            }
            entries += 1;
            if entries >= max_entries {
                truncated = true;
                break 'walk;
            }
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                bytes += e.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    json!({"entries": entries, "bytes": bytes, "bytes_human": human_size(bytes), "excluded": excluded, "truncated": truncated, "max_entries": max_entries})
}

fn str_set(v: &Json) -> BTreeSet<String> {
    v.as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

/// The legacy job an adopted Emma describes: same canonical source, destination and prefix.
pub fn matching_job<'a>(jobs: &'a [Job], source: &Path, dest: &Path, prefix: &str) -> Option<&'a Job> {
    let candidates: Vec<&Job> = jobs
        .iter()
        .filter(|j| {
            canonical_or_normalized(&j.source) == source
                && canonical_or_normalized(&j.dest_dir) == dest
                && j.prefix.as_deref() == Some(prefix)
        })
        .collect();
    candidates.iter().find(|j| j.is_systemd()).or(candidates.first()).copied()
}

/// Fields where the live legacy job no longer matches the manifest.
pub fn compare_legacy(job: &Job, def: &Json, manifest_dir: &Path, home: &Path) -> Vec<String> {
    let mut drift = Vec::new();
    let retention = &def["retention"];
    let want = match retention["strategy"].as_str() {
        Some("legacy-count@1") => retention["keep"].as_u64().map(|n| RetentionPolicy::Count(n as usize)),
        Some("legacy-gfs@1") => Some(RetentionPolicy::Gfs {
            daily: retention["daily"].as_u64().unwrap_or(7) as usize,
            weekly: retention["weekly"].as_u64().unwrap_or(4) as usize,
            monthly: retention["monthly"].as_u64().unwrap_or(4) as usize,
        }),
        _ => None,
    };
    if want.as_ref() != job.retention() {
        drift.push("retention".into());
    }
    let declared: BTreeSet<String> = str_set(&def["source"]["exclude"]);
    let live: BTreeSet<String> = job.cli_excludes().into_iter().collect();
    if declared != live {
        drift.push("exclude".into());
    }
    let declared_from = def["source"]["exclude_from"]
        .as_str()
        .and_then(|f| resolve_path(f, manifest_dir, home).ok())
        .map(|p| canonical_or_normalized(&p));
    let live_from = job.exclude_from().map(canonical_or_normalized);
    if declared_from != live_from {
        drift.push("exclude_from".into());
    }
    if !job.once() {
        drift.push("once".into());
    }
    drift
}

/// A unit that executes a binary inside a cargo build directory stops
/// backing up the moment someone runs `cargo clean`.
pub fn exec_in_build_dir(job: &Job) -> Option<String> {
    let exe = job.argv().first()?;
    (exe.contains("/target/release/") || exe.contains("/target/debug/")).then(|| exe.clone())
}

fn live_manifest(row: &EmmaRow, ctx: &Ctx) -> Json {
    let path = Path::new(&row.manifest_path);
    let registered = ctx.manifests.iter().find(|m| m.manifest_path == row.manifest_path).map(|m| m.state.clone());
    if !path.exists() {
        return json!({"path": row.manifest_path, "registered_state": registered, "live": "missing"});
    }
    let loaded = manifest::load(path, &ctx.home, false);
    if !loaded.ok() {
        return json!({"path": row.manifest_path, "registered_state": registered, "live": "invalid"});
    }
    let live = match loaded.emmas.iter().find(|e| e.id == row.emma_id) {
        None => "orphan".to_string(),
        Some(e) if e.definition_sha() == row.definition_sha => "ok".to_string(),
        Some(e) => format!("drift:{}", manifest::drift_fields(&row.definition, &e.definition_json()).join(",")),
    };
    json!({"path": row.manifest_path, "registered_state": registered, "live": live})
}

/// Evaluate one registered Emma. `measure` = Some(max entries) for `--measure`.
pub fn evaluate(row: &EmmaRow, ctx: &mut Ctx, measure_max: Option<usize>) -> (Json, Vec<Finding>) {
    let mut findings = Vec::new();
    let label = row.label();
    let def = row.definition.clone();
    let manifest_dir = Path::new(&row.manifest_path).parent().unwrap_or(Path::new("/")).to_path_buf();
    let home = ctx.home.clone();
    let kind = def["source"]["kind"].as_str().unwrap_or("").to_string();

    // --- source ---
    let source_path = row.source_canonical.as_ref().map(PathBuf::from);
    let source_missing = kind == "files" && !source_path.as_ref().is_some_and(|p| p.exists());
    let source_dev = source_path.as_deref().and_then(device);
    let source = if kind == "files" {
        let mut s = json!({
            "kind": "files",
            "declared": row.source_declared,
            "canonical": row.source_canonical,
            "exists": !source_missing,
            "device": source_dev.map(dev_label),
            "exclude": def["source"]["exclude"],
            "exclude_from": def["source"]["exclude_from"],
        });
        if let Some(p) = &source_path
            && !source_missing
        {
            s["summary"] = shallow_summary(p);
            if let Some(max) = measure_max {
                let mut pats: Vec<String> = str_set(&def["source"]["exclude"]).into_iter().collect();
                if let Some(f) = def["source"]["exclude_from"].as_str()
                    && let Ok(p) = resolve_path(f, &manifest_dir, &home)
                    && let Ok(lines) = read_exclude_file(&p)
                {
                    pats.extend(lines);
                }
                let excludes: Vec<ExcludePattern> = pats.iter().filter_map(|p| ExcludePattern::parse(p).ok()).collect();
                s["measure"] = measure(p, &excludes, max);
            }
        }
        s
    } else {
        json!({
            "kind": "export",
            "command": def["source"]["command"],
            "contract": def["source"]["contract"],
            "note": "an owner export verb; this version never runs it",
        })
    };
    if source_missing {
        findings.push(Finding::of("E_SOURCE_MISSING", format!("source {} no longer exists", row.source_declared)).emma(&label));
    }

    // --- capture ---
    let (cap_name, cap) = row.strategy("capture");
    let cap_status = strategies::lookup(&cap_name).map(|d| d.status).unwrap_or(Status::Reserved);
    let mut capture = json!({"strategy": cap_name, "status": cap_status.label()});
    if cap_name == "command-export@1" {
        let staging = cap["staging"].as_str().and_then(|s| resolve_path(s, &manifest_dir, &home).ok());
        let mut argv: Vec<String> =
            def["source"]["command"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect()).unwrap_or_default();
        if let Some(s) = &staging {
            argv.extend(["--out".to_string(), s.display().to_string()]);
        }
        if let Some(c) = def["source"]["contract"].as_str() {
            argv.extend(["--format".to_string(), c.to_string()]);
        }
        capture["argv"] = json!(argv);
        capture["staging"] = json!(staging.as_ref().map(|p| p.display().to_string()));
        capture["staging_exists"] = json!(staging.as_ref().is_some_and(|p| p.exists()));
    }

    // --- backend / destination ---
    let (back_name, back) = row.strategy("backend");
    let back_status = strategies::lookup(&back_name).map(|d| d.status).unwrap_or(Status::Reserved);
    let dest = row.dest_canonical.as_ref().map(PathBuf::from);
    let dest_exists = dest.as_ref().is_some_and(|p| p.exists());
    let dest_anchor = dest.as_deref().and_then(nearest_existing);
    let dest_dev = dest_anchor.as_deref().and_then(device);
    let free = dest_anchor.as_deref().and_then(|p| fs2::available_space(p).ok());
    let backend = json!({
        "strategy": back_name,
        "status": back_status.label(),
        "dest": row.dest_canonical,
        "prefix": back["prefix"],
        "exists": dest_exists,
        "device": dest_dev.map(dev_label),
        "free_bytes": free,
        "free_human": free.map(human_size),
        "password_ref": back.get("password_ref").map(|_| "set (op:// reference, not resolved)"),
    });

    // --- snapshot inventory ---
    let mut snapshot_count: Option<usize> = None;
    let mut inventory = json!({});
    match back_name.as_str() {
        "tar-gz-dir@1" => {
            let prefix = back["prefix"].as_str().unwrap_or("");
            match &dest {
                Some(d) if d.exists() => {
                    if fs::read_dir(d).is_ok() {
                        let snaps = list::snapshots_in(d, prefix);
                        let bytes: u64 = snaps.iter().map(|s| s.size).sum();
                        snapshot_count = Some(snaps.len());
                        inventory = json!({
                            "snapshots": snaps.len(),
                            "newest": snaps.first().map(|s| s.date.to_string()),
                            "oldest": snaps.last().map(|s| s.date.to_string()),
                            "bytes": bytes,
                            "bytes_human": human_size(bytes),
                            "missing_checksum": snaps.iter().filter(|s| !s.has_checksum).count(),
                            "dates": snaps.iter().map(|s| s.date.to_string()).collect::<Vec<_>>(),
                        });
                    } else {
                        inventory = json!({"snapshots": null, "note": "destination is not readable"});
                    }
                }
                _ => {
                    snapshot_count = Some(0);
                    inventory = json!({"snapshots": 0, "note": "destination directory does not exist"});
                }
            }
        }
        "restic-local@1" => {
            if dest_exists {
                inventory = json!({"snapshots": null, "note": "a restic repository exists, but this version cannot read it"});
            } else {
                snapshot_count = Some(0);
                inventory = json!({"snapshots": 0, "note": "repository not created (nothing has run)"});
            }
        }
        _ => {}
    }

    // --- independence ---
    let independence = match (source_dev, dest_dev) {
        (Some(a), Some(b)) if a == b => "same-device",
        _ => "same-host",
    };
    if row.sole_copy && independence == "same-device" {
        findings.push(
            Finding::of(
                "W_SOLE_COPY_SAME_DEVICE",
                "sole_copy: the only copy of this data and its declared backup share one device; nothing here survives that disk",
            )
            .emma(&label),
        );
    }

    // --- adopted: the legacy job ---
    let mut legacy = Json::Null;
    let mut expect_unit = None;
    if row.mode == "adopted" {
        let prefix = back["prefix"].as_str().unwrap_or("").to_string();
        let src = source_path.clone().unwrap_or_default();
        let dst = dest.clone().unwrap_or_default();
        let jobs = ctx.jobs();
        match matching_job(jobs, &src, &dst, &prefix) {
            None => {
                legacy = json!({"found": false});
                findings.push(Finding::of("E_ADOPT_NO_JOB", "no legacy forever-ago job writes this source, destination and prefix any more").emma(&label));
            }
            Some(job) => {
                let drift = compare_legacy(job, &def, &manifest_dir, &home);
                let build = exec_in_build_dir(job);
                if let Some(exe) = &build {
                    findings.push(
                        Finding::of(
                            "W_EXEC_IN_BUILD_DIR",
                            format!("{} executes {exe}, inside a cargo build directory: `cargo clean` would silently end this backup", job.unit_name()),
                        )
                        .emma(&label),
                    );
                }
                if !drift.is_empty() {
                    findings.push(
                        Finding::of("W_LEGACY_DRIFT", format!("drift:legacy-job ({}): the unit's flags no longer match the manifest", drift.join(",")))
                            .emma(&label),
                    );
                }
                if job.is_systemd() {
                    expect_unit = Some(job.unit_name().to_string());
                }
                legacy = json!({
                    "found": true,
                    "unit": job.unit_name(),
                    "scheduler": job.scheduler_label(),
                    "defined_in": jobs::tilde(Path::new(job.defined_in()), &home),
                    "exec": job.argv().first(),
                    "comparison": if drift.is_empty() { "clean".to_string() } else { format!("drift:legacy-job:{}", drift.join(",")) },
                    "problems": job.problems(),
                    "exec_in_build_dir": build.is_some(),
                });
            }
        }
    }

    // --- the Candybar join ---
    let cb = candybar::join(&mut ctx.candybar, row.candybar_ref.as_deref(), expect_unit.as_deref());
    match cb["status"].as_str() {
        Some("missing") => findings.push(Finding::of("W_CANDYBAR_MISSING", format!("candybar {:?} does not exist", row.candybar_ref)).emma(&label)),
        Some("runner-mismatch") => findings.push(
            Finding::of("W_CANDYBAR_RUNNER_MISMATCH", format!("candybar {:?} does not start {:?}", row.candybar_ref, expect_unit)).emma(&label),
        ),
        _ => {}
    }

    // --- coverage verdict (never green) ---
    let coverage = if source_missing {
        "broken:source-missing"
    } else {
        match snapshot_count {
            None => "unknown",
            Some(0) => "none",
            Some(_) => "local-only",
        }
    };

    // --- retention semantics ---
    let (ret_name, ret) = row.strategy("retention");
    let retention = json!({
        "strategy": ret_name,
        "params": ret,
        "meaning": strategies::lookup(&ret_name).map(|d| d.meaning),
        "executed_by": if ret_name.starts_with("legacy-") { "the legacy job; Emma will not act" } else { "nothing: there is no retention apply in this version" },
    });
    let (fid_name, _) = row.strategy("fidelity");

    // --- what a future `run` would still need ---
    let mut blockers: Vec<Json> = Vec::new();
    let mut block = |code: &str, msg: String| blockers.push(json!({"code": code, "message": msg}));
    if row.mode == "adopted" {
        // Adopted Emmas are executed by their legacy job, never by Emma.
    } else {
        if cap_status != Status::Available {
            block("capture-not-implemented", format!("{cap_name} is {}: not implemented in this version", cap_status.label()));
        }
        if back_status != Status::Available {
            block("backend-not-implemented", format!("{back_name} is {}: not implemented in this version", back_status.label()));
        }
        if back_name == "restic-local@1" {
            if !dest_exists {
                block("backend-repo-missing", "no restic repository exists (creating one is a separate approved step)".into());
            }
            if back.get("password_ref").is_none() {
                block("password-ref-missing", "restic-local@1 needs `password_ref = \"op://...\"` before capture".into());
            }
        }
        if kind == "export" {
            block(
                "owner-export-verb-missing",
                format!(
                    "owner export verb missing: no `{}` export under contract {} has ever been produced or recorded, and this version never calls it",
                    def["source"]["command"].as_array().map(|a| a.iter().filter_map(Json::as_str).collect::<Vec<_>>().join(" ")).unwrap_or_default(),
                    def["source"]["contract"].as_str().unwrap_or("?")
                ),
            );
        }
        if def["budget"]["stored_bytes_max"].is_null() {
            block("budget-missing", "no [emmas.budget] stored_bytes_max: capture stays blocked".into());
        }
        if row.candybar_ref.is_none() {
            block("unscheduled", "no [emmas.candybar] ref (adding a candybar is a clock change)".into());
        }
        if !def["activation"]["capture"].as_bool().unwrap_or(false) {
            block("activation-off", "activation.capture is false (the only legal value in this version)".into());
        }
    }

    let out = json!({
        "project": row.project_key,
        "emma": row.emma_id,
        "label": label,
        "mode": row.mode,
        "sole_copy": row.sole_copy,
        "sync_state": row.sync_state,
        "generation": row.generation,
        "manifest": live_manifest(row, ctx),
        "description": def["description"],
        "source": source,
        "capture": capture,
        "backend": backend,
        "retention": retention,
        "fidelity": {"strategy": fid_name},
        "candybar": cb,
        "legacy_job": legacy,
        "inventory": inventory,
        "independence": independence,
        "coverage": coverage,
        "blockers": blockers,
    });
    (out, findings)
}

fn tilde(p: &str, home: &Path) -> String {
    jobs::tilde(Path::new(p), home)
}

fn schedule_line(cb: &Json) -> String {
    match cb["status"].as_str().unwrap_or("unknown") {
        "unscheduled" => "unscheduled (no candybar ref)".into(),
        status => {
            let mut s = format!("{} {}", cb["ref"].as_str().unwrap_or("?"), status);
            if let Some(sched) = cb["schedule"].as_str() {
                s.push_str(&format!(", `{sched}` {}", cb["timezone"].as_str().unwrap_or("")));
            }
            if let Some(last) = cb["last_invocation"].as_object() {
                s.push_str(&format!(
                    "; last {} at {}",
                    last.get("result").and_then(Json::as_str).unwrap_or("?"),
                    last.get("started_at").and_then(Json::as_str).unwrap_or("?")
                ));
            }
            s
        }
    }
}

pub fn render_plan(e: &Json, home: &Path) -> String {
    let mut o = String::new();
    let flags = [e["mode"].as_str().unwrap_or("?").to_string()]
        .into_iter()
        .chain(e["sole_copy"].as_bool().unwrap_or(false).then(|| "SOLE COPY".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    o.push_str(&format!("{}  ({flags}, generation {}, sync {})\n", e["label"].as_str().unwrap_or("?"), e["generation"], e["sync_state"].as_str().unwrap_or("?")));
    if let Some(d) = e["description"].as_str() {
        o.push_str(&format!("  {d}\n"));
    }
    let m = &e["manifest"];
    o.push_str(&format!(
        "  manifest     {}  (registered {}, live {})\n",
        tilde(m["path"].as_str().unwrap_or("?"), home),
        m["registered_state"].as_str().unwrap_or("?"),
        m["live"].as_str().unwrap_or("?")
    ));
    let s = &e["source"];
    if s["kind"] == "files" {
        let mut line = format!("  source       files {}", s["declared"].as_str().unwrap_or("?"));
        if let Some(c) = s["canonical"].as_str() {
            line.push_str(&format!(" -> {}", tilde(c, home)));
        }
        if s["exists"].as_bool() != Some(true) {
            line.push_str("  MISSING");
        } else if let Some(sum) = s["summary"].as_object() {
            if sum.get("type").and_then(Json::as_str) == Some("dir") {
                line.push_str(&format!(
                    "  ({} top-level files, {}, newest {})",
                    sum.get("files").and_then(Json::as_u64).unwrap_or(0),
                    sum.get("bytes_human").and_then(Json::as_str).unwrap_or("?"),
                    sum.get("newest").and_then(Json::as_str).unwrap_or("-")
                ));
            }
        }
        o.push_str(&line);
        o.push('\n');
        if let Some(mz) = s["measure"].as_object() {
            o.push_str(&format!(
                "  measured     {} entries, {}{} ({} excluded)\n",
                mz.get("entries").and_then(Json::as_u64).unwrap_or(0),
                mz.get("bytes_human").and_then(Json::as_str).unwrap_or("?"),
                if mz.get("truncated").and_then(Json::as_bool) == Some(true) { " (truncated)" } else { "" },
                mz.get("excluded").and_then(Json::as_u64).unwrap_or(0)
            ));
        }
    } else {
        o.push_str(&format!(
            "  source       export {}  contract {}\n",
            s["command"].as_array().map(|a| a.iter().filter_map(Json::as_str).collect::<Vec<_>>().join(" ")).unwrap_or_default(),
            s["contract"].as_str().unwrap_or("?")
        ));
    }
    let c = &e["capture"];
    o.push_str(&format!("  capture      {}  ({})\n", c["strategy"].as_str().unwrap_or("?"), c["status"].as_str().unwrap_or("?")));
    if let Some(argv) = c["argv"].as_array() {
        o.push_str(&format!("  capture argv {}\n", argv.iter().filter_map(Json::as_str).collect::<Vec<_>>().join(" ")));
    }
    let b = &e["backend"];
    o.push_str(&format!(
        "  backend      {}  {}{}  ({}, device {}, {} free)\n",
        b["strategy"].as_str().unwrap_or("?"),
        b["dest"].as_str().map(|d| tilde(d, home)).unwrap_or_default(),
        b["prefix"].as_str().map(|p| format!("/{p}-YYYY-MM-DD.tar.gz")).unwrap_or_default(),
        if b["exists"].as_bool() == Some(true) { "exists" } else { "absent" },
        b["device"].as_str().unwrap_or("?"),
        b["free_human"].as_str().unwrap_or("?")
    ));
    o.push_str(&format!(
        "  retention    {}  (executed by {})\n",
        e["retention"]["strategy"].as_str().unwrap_or("?"),
        e["retention"]["executed_by"].as_str().unwrap_or("?")
    ));
    o.push_str(&format!("  fidelity     {}\n", e["fidelity"]["strategy"].as_str().unwrap_or("?")));
    o.push_str(&format!("  candybar     {}\n", schedule_line(&e["candybar"])));
    let l = &e["legacy_job"];
    if l.is_object() {
        if l["found"].as_bool() == Some(true) {
            o.push_str(&format!(
                "  legacy job   {} [{}] {}  exec {}\n",
                l["unit"].as_str().unwrap_or("?"),
                l["scheduler"].as_str().unwrap_or("?"),
                l["comparison"].as_str().unwrap_or("?"),
                l["exec"].as_str().unwrap_or("?")
            ));
        } else {
            o.push_str("  legacy job   NOT FOUND\n");
        }
    }
    let inv = &e["inventory"];
    match inv["snapshots"].as_u64() {
        Some(0) => o.push_str(&format!("  snapshots    0  ({})\n", inv["note"].as_str().unwrap_or("none found"))),
        Some(n) => o.push_str(&format!(
            "  snapshots    {n}, newest {}, oldest {}, {}{}\n",
            inv["newest"].as_str().unwrap_or("?"),
            inv["oldest"].as_str().unwrap_or("?"),
            inv["bytes_human"].as_str().unwrap_or("?"),
            match inv["missing_checksum"].as_u64() {
                Some(0) | None => String::new(),
                Some(k) => format!(", {k} without .sha256"),
            }
        )),
        None => o.push_str(&format!("  snapshots    unknown  ({})\n", inv["note"].as_str().unwrap_or(""))),
    }
    o.push_str(&format!(
        "  coverage     {}  (independence: {})\n",
        e["coverage"].as_str().unwrap_or("?"),
        e["independence"].as_str().unwrap_or("?")
    ));
    if let Some(bl) = e["blockers"].as_array()
        && !bl.is_empty()
    {
        o.push_str("  blockers for a future run:\n");
        for x in bl {
            o.push_str(&format!("    - {}: {}\n", x["code"].as_str().unwrap_or("?"), x["message"].as_str().unwrap_or("")));
        }
    }
    o
}

pub fn render_projects(rows: &[Json], home: &Path) -> String {
    if rows.is_empty() {
        return "no Emmas registered (write a .emma.toml, then `forever-ago register`)\n".into();
    }
    let mut o = String::new();
    let today = Local::now().date_naive();
    for e in rows {
        let inv = &e["inventory"];
        let snaps = match inv["snapshots"].as_u64() {
            Some(0) => "0 snapshots".to_string(),
            Some(n) => format!(
                "{n} snapshot{}, newest {}{}, {}",
                if n == 1 { "" } else { "s" },
                inv["newest"].as_str().unwrap_or("?"),
                inv["newest"]
                    .as_str()
                    .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
                    .map(|d| format!(" ({}d ago)", (today - d).num_days()))
                    .unwrap_or_default(),
                inv["bytes_human"].as_str().unwrap_or("?")
            ),
            None => "snapshots unknown".to_string(),
        };
        let dest = e["backend"]["dest"].as_str().map(|d| tilde(d, home)).unwrap_or_default();
        o.push_str(&format!(
            "{}  [{}{}]  sync {}  coverage {} ({})\n",
            e["label"].as_str().unwrap_or("?"),
            e["mode"].as_str().unwrap_or("?"),
            if e["sole_copy"].as_bool() == Some(true) { ", SOLE COPY" } else { "" },
            e["sync_state"].as_str().unwrap_or("?"),
            e["coverage"].as_str().unwrap_or("?"),
            e["independence"].as_str().unwrap_or("?"),
        ));
        let source = if e["source"]["kind"] == "files" {
            e["source"]["canonical"].as_str().map(|s| tilde(s, home)).unwrap_or_default()
        } else {
            format!(
                "export {}",
                e["source"]["command"].as_array().map(|a| a.iter().filter_map(Json::as_str).collect::<Vec<_>>().join(" ")).unwrap_or_default()
            )
        };
        o.push_str(&format!("    {source} -> {dest} ({})\n", e["backend"]["strategy"].as_str().unwrap_or("?")));
        o.push_str(&format!("    {}; {}\n", schedule_line(&e["candybar"]), snaps));
    }
    o
}
