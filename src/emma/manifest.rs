//! `.emma.toml` v1: parsing and validation (EMMA-DESIGN 3.1, 3.2).
//!
//! One file at the project root. Every level is closed: a key not in the
//! grammar is an error, never ignored. Validation is all-or-nothing per file.
//! Rules follow Candybar's manifest convention (strict keys, relative paths
//! against the project, duplicate check) implemented here in Rust.

use super::envelope::{has_errors, Finding};
use super::strategies::{self, parse_axis, resolve_path, Axis, Status, StrategyRef};
use crate::jobs::canonical_or_normalized;
use crate::{read_exclude_file, ExcludePattern};
use serde_json::{json, Value as Json};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use toml::{Table, Value};

pub const MANIFEST_NAME: &str = ".emma.toml";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Declared,
    Adopted,
}

impl Mode {
    pub fn label(self) -> &'static str {
        match self {
            Mode::Declared => "declared",
            Mode::Adopted => "adopted",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Source {
    Files { path: String, exclude: Vec<String>, exclude_from: Option<String> },
    Export { command: Vec<String>, contract: String },
}

#[derive(Clone, Debug, PartialEq)]
pub struct EmmaDef {
    pub id: String,
    pub description: Option<String>,
    pub mode: Mode,
    pub sole_copy: bool,
    pub source: Source,
    pub capture: StrategyRef,
    pub backend: StrategyRef,
    pub retention: StrategyRef,
    pub fidelity: StrategyRef,
    pub candybar_ref: Option<String>,
    pub activation_capture: bool,
    pub activation_prune: bool,
    pub budget: BTreeMap<String, i64>,
    /// `files`: the source as resolved (absolute, symlinks kept).
    pub source_resolved: Option<PathBuf>,
    /// `files`: symlinks resolved, for overlap and destination checks.
    pub source_canonical: Option<PathBuf>,
    pub exclude_from_resolved: Option<PathBuf>,
    /// Backend `dir` or `repo`, resolved and canonicalised (the nearest existing ancestor).
    pub dest_resolved: Option<PathBuf>,
    pub dest_canonical: Option<PathBuf>,
}

impl EmmaDef {
    pub fn source_json(&self) -> Json {
        match &self.source {
            Source::Files { path, exclude, exclude_from } => {
                json!({"kind": "files", "path": path, "exclude": exclude, "exclude_from": exclude_from})
            }
            Source::Export { command, contract } => json!({"kind": "export", "command": command, "contract": contract}),
        }
    }

    pub fn source_declared(&self) -> String {
        match &self.source {
            Source::Files { path, .. } => path.clone(),
            Source::Export { command, .. } => command.join(" "),
        }
    }

    /// The validated definition with keys sorted (serde_json maps are ordered).
    /// Declared values only, so a repo moved on disk keeps the same definition.
    pub fn definition_json(&self) -> Json {
        json!({
            "id": self.id,
            "description": self.description,
            "mode": self.mode.label(),
            "sole_copy": self.sole_copy,
            "source": self.source_json(),
            "capture": self.capture.to_json(),
            "backend": self.backend.to_json(),
            "retention": self.retention.to_json(),
            "fidelity": self.fidelity.to_json(),
            "candybar": self.candybar_ref.as_ref().map(|r| json!({"ref": r})),
            "activation": {"capture": self.activation_capture, "prune": self.activation_prune},
            "budget": self.budget,
        })
    }

    pub fn definition_sha(&self) -> String {
        sha256_hex(self.definition_json().to_string().as_bytes())
    }

    /// Source + capture + backend + fidelity only: retention, candybar and
    /// budget edits never bump the generation.
    pub fn contract_sha(&self) -> String {
        let c = json!({
            "source": self.source_json(),
            "capture": self.capture.to_json(),
            "backend": self.backend.to_json(),
            "fidelity": self.fidelity.to_json(),
        });
        sha256_hex(c.to_string().as_bytes())
    }

    /// `(canonical dest, prefix)`: the claim checked for E_DEST_COLLISION.
    pub fn dest_claim(&self) -> Option<(String, String)> {
        let dest = self.dest_canonical.as_ref()?;
        Some((dest.display().to_string(), self.backend.str_param("prefix").unwrap_or("").to_string()))
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The nearest existing ancestor, canonicalised, with the rest appended:
/// a not-yet-created restic repo under a symlinked directory still compares
/// equal to its real location.
pub fn canonical_lenient(path: &Path) -> PathBuf {
    if let Ok(p) = fs::canonicalize(path) {
        return p;
    }
    let mut rest = Vec::new();
    let mut cur = path.to_path_buf();
    while let Some(parent) = cur.parent().map(Path::to_path_buf) {
        if let Some(name) = cur.file_name() {
            rest.push(name.to_os_string());
        }
        if let Ok(p) = fs::canonicalize(&parent) {
            let mut out = p;
            for n in rest.iter().rev() {
                out.push(n);
            }
            return out;
        }
        cur = parent;
    }
    canonical_or_normalized(path)
}

pub struct Loaded {
    /// Canonical absolute path of the manifest file.
    pub path: PathBuf,
    /// The directory holding it (the project root).
    pub dir: PathBuf,
    pub sha256: String,
    pub emmas: Vec<EmmaDef>,
    pub findings: Vec<Finding>,
}

impl Loaded {
    pub fn ok(&self) -> bool {
        !has_errors(&self.findings)
    }
}

/// Read and validate a manifest file. A read failure is an E_PARSE finding.
pub fn load(path: &Path, home: &Path, check_fs: bool) -> Loaded {
    let canonical = canonical_or_normalized(path);
    let dir = canonical.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from("/"));
    let label = canonical.display().to_string();
    match fs::read(&canonical) {
        Err(err) => Loaded {
            sha256: String::new(),
            emmas: Vec::new(),
            findings: vec![Finding::of("E_PARSE", format!("cannot read {MANIFEST_NAME}: {err}")).at(&label)],
            path: canonical,
            dir,
        },
        Ok(bytes) => {
            let sha256 = sha256_hex(&bytes);
            let (emmas, findings) = match String::from_utf8(bytes) {
                Ok(text) => parse_str(&text, &dir, home, check_fs),
                Err(_) => (Vec::new(), vec![Finding::of("E_PARSE", "not valid UTF-8")]),
            };
            let findings = findings.into_iter().map(|f| if f.path.is_none() { f.at(&label) } else { f }).collect();
            Loaded { path: canonical, dir, sha256, emmas, findings }
        }
    }
}

const CLOCK_KEYS: [&str; 6] = ["schedule", "cron", "timer", "interval", "every", "at"];
const IDENTITY_KEYS: [&str; 4] = ["project", "project_id", "uuid", "slug"];

fn secret_shaped(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    if k == "password_ref" {
        return false;
    }
    ["password", "passwd", "token", "secret", "key", "apikey", "credential", "credentials"].contains(&k.as_str())
        || ["_password", "_token", "_secret", "_key", "_apikey", "_credential"].iter().any(|s| k.ends_with(s))
}

/// Keys rejected with a pointer to their owner instead of a generic unknown-key error.
fn forbidden(key: &str) -> Option<Finding> {
    if CLOCK_KEYS.contains(&key) {
        return Some(Finding::of(
            "E_CLOCK_KEY",
            format!("`{key}`: Candybar owns when; set `[emmas.candybar] ref = \"<candybar name>\"` and schedule it in Candybar"),
        ));
    }
    if IDENTITY_KEYS.contains(&key) {
        return Some(Finding::of(
            "E_IDENTITY_KEY",
            format!("`{key}`: project identity comes from pjangler, never the manifest; run `pj init` in the project"),
        ));
    }
    if secret_shaped(key) {
        return Some(Finding::of(
            "E_SECRET_KEY",
            format!("`{key}`: a manifest never holds a credential; use `password_ref = \"op://<vault>/<item>/<field>\"`"),
        ));
    }
    None
}

/// Walk every table (including arrays of tables) for forbidden keys.
fn scan_forbidden(t: &Table, path: &str, out: &mut Vec<Finding>) {
    for (k, v) in t {
        let here = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
        if let Some(f) = forbidden(k) {
            out.push(Finding { message: format!("{} (at `{here}`)", f.message), ..f });
        }
        match v {
            Value::Table(sub) => scan_forbidden(sub, &here, out),
            Value::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    if let Value::Table(sub) = item {
                        scan_forbidden(sub, &format!("{here}[{i}]"), out);
                    }
                }
            }
            _ => {}
        }
    }
}

fn is_forbidden(k: &str) -> bool {
    forbidden(k).is_some()
}

fn unknown_keys(t: &Table, allowed: &[&str], where_: &str, emma: Option<&str>, out: &mut Vec<Finding>) {
    for k in t.keys() {
        if !allowed.contains(&k.as_str()) && !is_forbidden(k) {
            let mut f = Finding::of("E_UNKNOWN_KEY", format!("unknown key `{k}` in {where_}; allowed: {}", allowed.join(", ")));
            if let Some(e) = emma {
                f = f.emma(e);
            }
            out.push(f);
        }
    }
}

pub fn id_ok(id: &str) -> bool {
    slug_ok(id) && id.len() <= 48
}

/// Candybar's own SLUG_RE: `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`.
pub fn slug_ok(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
        && b[0] != b'-'
        && b[b.len() - 1] != b'-'
}

fn contract_ok(s: &str) -> bool {
    let Some((n, v)) = s.split_once('@') else { return false };
    !n.is_empty()
        && n.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !v.is_empty()
        && v.chars().all(|c| c.is_ascii_digit())
}

/// Parse and validate manifest text. `check_fs` adds the checks that need
/// the filesystem (source exists, destination placement, exclude file).
pub fn parse_str(text: &str, manifest_dir: &Path, home: &Path, check_fs: bool) -> (Vec<EmmaDef>, Vec<Finding>) {
    let mut findings = Vec::new();
    let table: Table = match toml::from_str(text) {
        Ok(t) => t,
        Err(err) => {
            let msg = err.to_string().lines().filter(|l| !l.trim().is_empty()).collect::<Vec<_>>().join(" | ");
            return (Vec::new(), vec![Finding::of("E_PARSE", format!("invalid TOML: {msg}"))]);
        }
    };

    match table.get("schema_version") {
        Some(Value::Integer(1)) => {}
        Some(other) => {
            return (
                Vec::new(),
                vec![Finding::of("E_SCHEMA_VERSION", format!("schema_version must be 1 (got {other}); this forever-ago reads v1 only"))],
            );
        }
        None => return (Vec::new(), vec![Finding::of("E_SCHEMA_VERSION", "schema_version = 1 is required")]),
    }

    scan_forbidden(&table, "", &mut findings);
    unknown_keys(&table, &["schema_version", "emmas"], "the top level", None, &mut findings);

    let items = match table.get("emmas") {
        Some(Value::Array(items)) if !items.is_empty() => items,
        Some(Value::Array(_)) | None => {
            findings.push(Finding::of("E_MISSING_KEY", "at least one [[emmas]] entry is required"));
            return (Vec::new(), findings);
        }
        Some(_) => {
            findings.push(Finding::of("E_PARSE", "`emmas` must be an array of tables ([[emmas]])"));
            return (Vec::new(), findings);
        }
    };

    let mut emmas = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let Value::Table(t) = item else {
            findings.push(Finding::of("E_PARSE", format!("emmas[{i}] is not a table")));
            continue;
        };
        if let Some(def) = parse_emma(t, i, manifest_dir, home, check_fs, &mut findings) {
            emmas.push(def);
        }
    }

    let mut seen = HashSet::new();
    for e in &emmas {
        if !seen.insert(e.id.clone()) {
            findings.push(Finding::of("E_DUPLICATE_ID", format!("id {:?} is declared more than once", e.id)).emma(&e.id));
        }
    }

    for (i, a) in emmas.iter().enumerate() {
        for b in emmas.iter().skip(i + 1) {
            if let (Some(sa), Some(sb)) = (&a.source_canonical, &b.source_canonical)
                && (sa.starts_with(sb) || sb.starts_with(sa))
            {
                findings.push(
                    Finding::of("W_SOURCE_OVERLAP", format!("sources of {:?} and {:?} overlap ({} / {})", a.id, b.id, sa.display(), sb.display()))
                        .emma(&a.id),
                );
            }
        }
    }

    if has_errors(&findings) {
        return (Vec::new(), findings);
    }
    (emmas, findings)
}

fn parse_emma(t: &Table, index: usize, manifest_dir: &Path, home: &Path, check_fs: bool, out: &mut Vec<Finding>) -> Option<EmmaDef> {
    let start = out.len();
    let id = match t.get("id") {
        Some(Value::String(s)) if id_ok(s) => s.clone(),
        Some(Value::String(s)) => {
            out.push(Finding::of("E_BAD_ID", format!("id {s:?} must match ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ and be at most 48 characters")));
            return None;
        }
        Some(_) => {
            out.push(Finding::of("E_BAD_ID", format!("emmas[{index}].id must be a string")));
            return None;
        }
        None => {
            out.push(Finding::of("E_MISSING_KEY", format!("emmas[{index}] needs an `id`")));
            return None;
        }
    };
    let err = |code: &str, msg: String| Finding::of(code, msg).emma(&id);

    unknown_keys(
        t,
        &["id", "description", "mode", "sole_copy", "source", "capture", "backend", "retention", "fidelity", "candybar", "activation", "budget"],
        &format!("[[emmas]] {id:?}"),
        Some(&id),
        out,
    );

    let description = match t.get("description") {
        None => None,
        Some(Value::String(s)) if s.chars().count() <= 200 && !s.chars().any(char::is_control) => Some(s.clone()),
        Some(Value::String(_)) => {
            out.push(err("E_BAD_PARAM", "description must be at most 200 characters with no control characters".into()));
            None
        }
        Some(_) => {
            out.push(err("E_BAD_PARAM", "description must be a string".into()));
            None
        }
    };

    let mode = match t.get("mode") {
        None => Mode::Declared,
        Some(Value::String(s)) if s == "declared" => Mode::Declared,
        Some(Value::String(s)) if s == "adopted" => Mode::Adopted,
        Some(Value::String(s)) if s == "managed" => {
            out.push(err("E_RESERVED_MODE", "mode \"managed\" is reserved and not accepted in v1".into()));
            Mode::Declared
        }
        Some(other) => {
            out.push(err("E_BAD_PARAM", format!("mode must be \"declared\" or \"adopted\" (got {other})")));
            Mode::Declared
        }
    };

    let sole_copy = match t.get("sole_copy") {
        None => false,
        Some(Value::Boolean(b)) => *b,
        Some(_) => {
            out.push(err("E_BAD_PARAM", "sole_copy must be true or false".into()));
            false
        }
    };

    // --- source ---
    let mut source_resolved = None;
    let mut source_canonical = None;
    let mut exclude_from_resolved = None;
    let source = match t.get("source") {
        Some(Value::Table(s)) => {
            let kind = s.get("kind").and_then(Value::as_str);
            match kind {
                Some("files") => {
                    unknown_keys(s, &["kind", "path", "exclude", "exclude_from"], "[emmas.source] (kind = \"files\")", Some(&id), out);
                    let path = match s.get("path") {
                        Some(Value::String(p)) => Some(p.clone()),
                        Some(_) => {
                            out.push(err("E_BAD_PARAM", "source.path must be a string".into()));
                            None
                        }
                        None => {
                            out.push(err("E_MISSING_KEY", "a files source needs `path`".into()));
                            None
                        }
                    };
                    let exclude = match s.get("exclude") {
                        None => Vec::new(),
                        Some(Value::Array(a)) => {
                            let mut v = Vec::new();
                            for x in a {
                                match x.as_str() {
                                    Some(p) => match ExcludePattern::parse(p) {
                                        Ok(_) => v.push(p.to_string()),
                                        Err(e) => out.push(err("E_BAD_PARAM", format!("source.exclude: {e:#}"))),
                                    },
                                    None => out.push(err("E_BAD_PARAM", "source.exclude entries must be strings".into())),
                                }
                            }
                            v
                        }
                        Some(_) => {
                            out.push(err("E_BAD_PARAM", "source.exclude must be an array of patterns".into()));
                            Vec::new()
                        }
                    };
                    let exclude_from = match s.get("exclude_from") {
                        None => None,
                        Some(Value::String(f)) => match resolve_path(f, manifest_dir, home) {
                            Ok(p) => {
                                if check_fs {
                                    match read_exclude_file(&p) {
                                        Ok(lines) => {
                                            for l in lines {
                                                if let Err(e) = ExcludePattern::parse(&l) {
                                                    out.push(err("E_BAD_PARAM", format!("source.exclude_from {}: {e:#}", p.display())));
                                                }
                                            }
                                        }
                                        Err(e) => out.push(err("E_BAD_PARAM", format!("source.exclude_from: {e:#}"))),
                                    }
                                }
                                exclude_from_resolved = Some(p);
                                Some(f.clone())
                            }
                            Err(e) => {
                                out.push(err("E_BAD_PARAM", format!("source.exclude_from: {e}")));
                                None
                            }
                        },
                        Some(_) => {
                            out.push(err("E_BAD_PARAM", "source.exclude_from must be a path string".into()));
                            None
                        }
                    };
                    if let Some(p) = &path {
                        match resolve_path(p, manifest_dir, home) {
                            Ok(resolved) => {
                                if check_fs {
                                    match fs::canonicalize(&resolved) {
                                        Ok(c) => {
                                            let ft = fs::metadata(&c).map(|m| m.file_type());
                                            if !ft.as_ref().is_ok_and(|t| t.is_dir() || t.is_file()) {
                                                out.push(err("E_SOURCE_MISSING", format!("source {} is not a directory or regular file", c.display())));
                                            }
                                            source_canonical = Some(c);
                                        }
                                        Err(e) => out.push(err("E_SOURCE_MISSING", format!("source {} does not exist ({e})", resolved.display()))),
                                    }
                                } else {
                                    source_canonical = Some(canonical_or_normalized(&resolved));
                                }
                                source_resolved = Some(resolved);
                            }
                            Err(e) => out.push(err("E_BAD_PARAM", format!("source.path: {e}"))),
                        }
                    }
                    path.map(|path| Source::Files { path, exclude, exclude_from })
                }
                Some("export") => {
                    unknown_keys(s, &["kind", "command", "contract"], "[emmas.source] (kind = \"export\")", Some(&id), out);
                    let command = match s.get("command") {
                        Some(Value::Array(a)) if !a.is_empty() && a.iter().all(|x| x.as_str().is_some_and(|s| !s.is_empty())) => {
                            Some(a.iter().map(|x| x.as_str().unwrap().to_string()).collect::<Vec<_>>())
                        }
                        Some(_) => {
                            out.push(err("E_BAD_PARAM", "source.command must be a non-empty argv array of strings (no shell)".into()));
                            None
                        }
                        None => {
                            out.push(err("E_MISSING_KEY", "an export source needs `command`".into()));
                            None
                        }
                    };
                    let contract = match s.get("contract") {
                        Some(Value::String(c)) if contract_ok(c) => Some(c.clone()),
                        Some(_) => {
                            out.push(err("E_BAD_PARAM", "source.contract must be the owner's export contract id, `<name>@<int>`".into()));
                            None
                        }
                        None => {
                            out.push(err("E_MISSING_KEY", "an export source needs `contract`".into()));
                            None
                        }
                    };
                    match (command, contract) {
                        (Some(command), Some(contract)) => Some(Source::Export { command, contract }),
                        _ => None,
                    }
                }
                Some(other) => {
                    out.push(err("E_BAD_PARAM", format!("source.kind must be \"files\" or \"export\" (got {other:?})")));
                    None
                }
                None => {
                    out.push(err("E_MISSING_KEY", "[emmas.source] needs `kind`".into()));
                    None
                }
            }
        }
        Some(_) => {
            out.push(err("E_BAD_PARAM", "`source` must be a table".into()));
            None
        }
        None => {
            out.push(err("E_MISSING_KEY", "[emmas.source] is required".into()));
            None
        }
    };

    // --- strategy axes ---
    let axis = |a: Axis, out: &mut Vec<Finding>| -> Option<StrategyRef> {
        match t.get(a.key()) {
            None => {
                out.push(err("E_MISSING_KEY", format!("[emmas.{}] is required", a.key())));
                None
            }
            Some(v) => match parse_axis(a, v, manifest_dir, home, &id) {
                Ok(r) => Some(r),
                Err(errs) => {
                    out.extend(errs);
                    None
                }
            },
        }
    };
    let capture = axis(Axis::Capture, out);
    let backend = axis(Axis::Backend, out);
    let retention = axis(Axis::Retention, out);
    let fidelity = axis(Axis::Fidelity, out);

    // --- candybar ---
    let candybar_ref = match t.get("candybar") {
        None => None,
        Some(Value::Table(c)) => {
            unknown_keys(c, &["ref"], "[emmas.candybar]", Some(&id), out);
            match c.get("ref") {
                Some(Value::String(r)) if slug_ok(r) => Some(r.clone()),
                Some(Value::String(r)) => {
                    out.push(err("E_BAD_CANDYBAR_REF", format!("candybar.ref {r:?} is not a Candybar name (lowercase slug); ids never appear in a manifest")));
                    None
                }
                Some(_) => {
                    out.push(err("E_BAD_CANDYBAR_REF", "candybar.ref must be a Candybar name string".into()));
                    None
                }
                None => {
                    out.push(err("E_BAD_CANDYBAR_REF", "[emmas.candybar] needs `ref`".into()));
                    None
                }
            }
        }
        Some(_) => {
            out.push(err("E_BAD_CANDYBAR_REF", "`candybar` must be a table with `ref`".into()));
            None
        }
    };

    // --- activation (fail closed) ---
    let (mut activation_capture, mut activation_prune) = (false, false);
    match t.get("activation") {
        None => {}
        Some(Value::Table(a)) => {
            unknown_keys(a, &["capture", "prune"], "[emmas.activation]", Some(&id), out);
            for (k, slot) in [("capture", &mut activation_capture), ("prune", &mut activation_prune)] {
                match a.get(k) {
                    None => {}
                    Some(Value::Boolean(false)) => {}
                    Some(Value::Boolean(true)) => {
                        *slot = true;
                        out.push(err("E_NOT_IMPLEMENTED", format!("activation.{k} = true: this version only registers and plans; nothing can be enabled")));
                    }
                    Some(_) => out.push(err("E_BAD_PARAM", format!("activation.{k} must be true or false"))),
                }
            }
        }
        Some(_) => out.push(err("E_BAD_PARAM", "`activation` must be a table".into())),
    }

    // --- budget ---
    let mut budget = BTreeMap::new();
    match t.get("budget") {
        None => {}
        Some(Value::Table(b)) => {
            unknown_keys(b, &["stored_bytes_max", "scratch_bytes_max", "duration_s_max"], "[emmas.budget]", Some(&id), out);
            for k in ["stored_bytes_max", "scratch_bytes_max", "duration_s_max"] {
                match b.get(k) {
                    None => {}
                    Some(Value::Integer(n)) if *n > 0 => {
                        budget.insert(k.to_string(), *n);
                    }
                    Some(_) => out.push(err("E_BAD_PARAM", format!("budget.{k} must be an integer > 0"))),
                }
            }
        }
        Some(_) => out.push(err("E_BAD_PARAM", "`budget` must be a table".into())),
    }

    // --- cross-field rules ---
    if let (Some(src), Some(cap)) = (&source, &capture) {
        let want = match src {
            Source::Files { .. } => "files@1",
            Source::Export { .. } => "command-export@1",
        };
        if cap.name != want {
            out.push(err("E_BAD_PARAM", format!("capture {} does not fit a {} source; use {want}", cap.name, if want == "files@1" { "files" } else { "export" })));
        }
    }
    if sole_copy && let Some(r) = &retention
        && !matches!(r.name.as_str(), "keep-all@1" | "observe-only@1")
    {
        out.push(err(
            "E_SOLE_COPY_RETENTION",
            format!("sole_copy = true forbids thinning: retention must be keep-all@1 or observe-only@1, not {}", r.name),
        ));
    }
    if mode == Mode::Adopted {
        let backend_ok = backend.as_ref().is_some_and(|b| b.name == "tar-gz-dir@1");
        let retention_ok = retention.as_ref().is_some_and(|r| r.name.starts_with("legacy-"));
        let files_ok = matches!(source, Some(Source::Files { .. }));
        if !(backend_ok && retention_ok && files_ok) {
            out.push(err(
                "E_ADOPT_SHAPE",
                "mode = \"adopted\" describes a legacy forever-ago job: it needs a files source, backend tar-gz-dir@1 and a legacy-* retention".into(),
            ));
        }
    }

    // --- destination placement ---
    let mut dest_resolved = None;
    let mut dest_canonical = None;
    if let Some(b) = &backend {
        let key = if b.name == "tar-gz-dir@1" { "dir" } else { "repo" };
        if let Some(raw) = b.str_param(key)
            && let Ok(p) = resolve_path(raw, manifest_dir, home)
        {
            let canon = if check_fs { canonical_lenient(&p) } else { canonical_or_normalized(&p) };
            if let Some(src) = &source_canonical {
                if canon.starts_with(src) {
                    out.push(err(
                        "E_DEST_INSIDE_SOURCE",
                        format!("backend {key} {} is inside the source {}; the backup would include itself", canon.display(), src.display()),
                    ));
                } else if src.starts_with(&canon) {
                    out.push(err(
                        "W_SOURCE_INSIDE_DEST",
                        format!("source {} sits inside the backend {key} {}", src.display(), canon.display()),
                    ));
                }
            }
            dest_resolved = Some(p);
            dest_canonical = Some(canon);
        }
    }
    if out[start..].iter().any(|f| f.is_error()) {
        return None;
    }
    Some(EmmaDef {
        id: id.clone(),
        description,
        mode,
        sole_copy,
        source: source?,
        capture: capture?,
        backend: backend?,
        retention: retention?,
        fidelity: fidelity?,
        candybar_ref,
        activation_capture,
        activation_prune,
        budget,
        source_resolved,
        source_canonical,
        exclude_from_resolved,
        dest_resolved,
        dest_canonical,
    })
}

/// Compare two definitions by top-level field; the names are what
/// `drift:<fields>` reports.
pub fn drift_fields(old: &Json, new: &Json) -> Vec<String> {
    let mut out = Vec::new();
    let (Some(o), Some(n)) = (old.as_object(), new.as_object()) else {
        return vec!["definition".into()];
    };
    let mut keys: Vec<&String> = o.keys().chain(n.keys()).collect();
    keys.sort();
    keys.dedup();
    for k in keys {
        if o.get(k) != n.get(k) {
            out.push(k.clone());
        }
    }
    out
}

#[allow(dead_code)]
pub fn strategy_status(r: &StrategyRef) -> Status {
    strategies::lookup(&r.name).map(|d| d.status).unwrap_or(Status::Reserved)
}
