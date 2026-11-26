//! The compiled-in strategy catalog (EMMA-DESIGN 3.3).
//!
//! A strategy ref is `<name>@<int>`. There is no `latest`, no remote
//! registry and no plugin runtime: a ref this table does not know is refused.

use super::envelope::Finding;
use serde_json::{Map, Value as Json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use toml::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Axis {
    Capture,
    Backend,
    Retention,
    Fidelity,
}

impl Axis {
    pub fn key(self) -> &'static str {
        match self {
            Axis::Capture => "capture",
            Axis::Backend => "backend",
            Axis::Retention => "retention",
            Axis::Fidelity => "fidelity",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Accepted and fully planable in this version.
    Available,
    /// Accepted; `plan` reports it as not implemented and coverage stays `none`.
    Declared,
    /// Refused by `register`.
    Reserved,
}

impl Status {
    pub fn label(self) -> &'static str {
        match self {
            Status::Available => "available",
            Status::Declared => "declared",
            Status::Reserved => "reserved",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A filesystem path: `~/` expands to $HOME, relative resolves against the manifest directory.
    Path,
    /// A file-name prefix: non-empty, no `/`.
    Prefix,
    PositiveInt,
    NonNegativeInt,
    /// `op://vault/item/field`, shape-checked only, never resolved.
    SecretRef,
}

pub struct Param {
    pub key: &'static str,
    pub kind: Kind,
    pub required: bool,
}

pub struct Def {
    pub name: &'static str,
    pub axis: Axis,
    pub status: Status,
    pub params: &'static [Param],
    pub meaning: &'static str,
}

const fn p(key: &'static str, kind: Kind, required: bool) -> Param {
    Param { key, kind, required }
}

pub static CATALOG: &[Def] = &[
    Def { name: "files@1", axis: Axis::Capture, status: Status::Declared, params: &[], meaning: "walk source.path honouring excludes" },
    Def {
        name: "command-export@1",
        axis: Axis::Capture,
        status: Status::Declared,
        params: &[p("staging", Kind::Path, true)],
        meaning: "run the owner's read-only export argv into a same-filesystem staging dir",
    },
    Def { name: "postgres-logical@1", axis: Axis::Capture, status: Status::Reserved, params: &[], meaning: "logical Postgres dump; needs a bound owner source" },
    Def {
        name: "tar-gz-dir@1",
        axis: Axis::Backend,
        status: Status::Available,
        params: &[p("dir", Kind::Path, true), p("prefix", Kind::Prefix, true)],
        meaning: "<prefix>-YYYY-MM-DD.tar.gz plus .sha256 in dir (today's forever-ago writer)",
    },
    Def {
        name: "restic-local@1",
        axis: Axis::Backend,
        status: Status::Declared,
        params: &[p("repo", Kind::Path, true), p("password_ref", Kind::SecretRef, false)],
        meaning: "local encrypted restic repository",
    },
    Def { name: "observe-only@1", axis: Axis::Retention, status: Status::Available, params: &[], meaning: "inventory only; never selects anything for deletion" },
    Def { name: "keep-all@1", axis: Axis::Retention, status: Status::Available, params: &[], meaning: "keep everything, and assert it" },
    Def {
        name: "legacy-count@1",
        axis: Axis::Retention,
        status: Status::Available,
        params: &[p("keep", Kind::PositiveInt, true)],
        meaning: "the legacy --retain N: keep the newest N",
    },
    Def {
        name: "legacy-gfs@1",
        axis: Axis::Retention,
        status: Status::Available,
        params: &[p("daily", Kind::NonNegativeInt, false), p("weekly", Kind::NonNegativeInt, false), p("monthly", Kind::NonNegativeInt, false)],
        meaning: "the legacy --keep-daily/--keep-weekly/--keep-monthly GFS ladder",
    },
    Def {
        name: "age-buckets@1",
        axis: Axis::Retention,
        status: Status::Available,
        params: &[],
        meaning: "all [0,14d), newest per 3-day bucket [14d,90d), per ISO week [90d,365d), per month >=365d",
    },
    Def { name: "full@1", axis: Axis::Fidelity, status: Status::Available, params: &[], meaning: "original content within the declared source scope" },
    Def {
        name: "conversation-prompts@1",
        axis: Axis::Fidelity,
        status: Status::Reserved,
        params: &[],
        meaning: "lossy derived tier (prompts only); separate Emma, never replaces a restore point",
    },
];

pub fn lookup(name: &str) -> Option<&'static Def> {
    CATALOG.iter().find(|d| d.name == name)
}

/// A validated strategy reference with its parameters, as canonical JSON.
#[derive(Clone, Debug, PartialEq)]
pub struct StrategyRef {
    pub name: String,
    pub params: BTreeMap<String, Json>,
}

impl StrategyRef {
    pub fn to_json(&self) -> Json {
        let mut m = Map::new();
        m.insert("strategy".into(), Json::String(self.name.clone()));
        for (k, v) in &self.params {
            m.insert(k.clone(), v.clone());
        }
        Json::Object(m)
    }

    pub fn str_param(&self, key: &str) -> Option<&str> {
        self.params.get(key).and_then(Json::as_str)
    }

    #[cfg(test)]
    pub fn int_param(&self, key: &str) -> Option<i64> {
        self.params.get(key).and_then(Json::as_i64)
    }
}

fn shape_ok(name: &str) -> bool {
    let Some((n, v)) = name.split_once('@') else { return false };
    !n.is_empty()
        && n.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !v.is_empty()
        && v.chars().all(|c| c.is_ascii_digit())
}

pub fn secret_ref_ok(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("op://") else { return false };
    let parts: Vec<&str> = rest.split('/').collect();
    parts.len() == 3 && parts.iter().all(|p| !p.is_empty())
}

/// Resolve a manifest path value: `~/` is $HOME, relative is against the
/// manifest directory (never the cwd), `$VAR` is not expanded and so refused.
pub fn resolve_path(raw: &str, manifest_dir: &Path, home: &Path) -> Result<PathBuf, String> {
    if raw.is_empty() {
        return Err("empty path".into());
    }
    if raw.contains('$') {
        return Err(format!("{raw:?}: $VAR is not expanded in .emma.toml; write the path out"));
    }
    if raw.contains('\0') {
        return Err("path contains NUL".into());
    }
    if raw == "~" {
        return Ok(home.to_path_buf());
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return Ok(home.join(rest));
    }
    if raw.starts_with('~') {
        return Err(format!("{raw:?}: only `~/` (your own home) is expanded"));
    }
    let p = Path::new(raw);
    Ok(if p.is_absolute() { p.to_path_buf() } else { manifest_dir.join(p) })
}

/// Parse one axis table (or its bare-string shorthand).
pub fn parse_axis(
    axis: Axis,
    value: &Value,
    manifest_dir: &Path,
    home: &Path,
    emma: &str,
) -> Result<StrategyRef, Vec<Finding>> {
    let at = |code: &str, msg: String| Finding::of(code, msg).emma(emma);
    let (name, table) = match value {
        Value::String(s) => (s.clone(), None),
        Value::Table(t) => match t.get("strategy") {
            Some(Value::String(s)) => (s.clone(), Some(t)),
            Some(_) => return Err(vec![at("E_BAD_PARAM", format!("[emmas.{}] strategy must be a string", axis.key()))]),
            None => return Err(vec![at("E_MISSING_KEY", format!("[emmas.{}] needs `strategy = \"<name>@<n>\"`", axis.key()))]),
        },
        _ => {
            return Err(vec![at(
                "E_BAD_PARAM",
                format!("`{}` must be a strategy string or a table with `strategy`", axis.key()),
            )]);
        }
    };
    if !shape_ok(&name) {
        return Err(vec![at(
            "E_UNKNOWN_STRATEGY",
            format!("{} strategy {name:?} is not a `<name>@<int>` ref (there is no `latest`)", axis.key()),
        )]);
    }
    let Some(def) = lookup(&name) else {
        let known: Vec<&str> = CATALOG.iter().filter(|d| d.axis == axis && d.status != Status::Reserved).map(|d| d.name).collect();
        return Err(vec![at(
            "E_UNKNOWN_STRATEGY",
            format!("unknown {} strategy {name:?}; this version knows: {}", axis.key(), known.join(", ")),
        )]);
    };
    if def.axis != axis {
        return Err(vec![at(
            "E_UNKNOWN_STRATEGY",
            format!("{name} is a {} strategy, not a {} one", def.axis.key(), axis.key()),
        )]);
    }
    if def.status == Status::Reserved {
        return Err(vec![at(
            "E_RESERVED_STRATEGY",
            format!("{name} is reserved: not accepted until a reviewed contract and a fixture-proven implementation exist"),
        )]);
    }

    let mut errs = Vec::new();
    let mut params = BTreeMap::new();
    if let Some(t) = table {
        for (k, v) in t {
            if k == "strategy" {
                continue;
            }
            let Some(param) = def.params.iter().find(|p| p.key == k) else {
                errs.push(at("E_BAD_PARAM", format!("{name} takes no parameter `{k}`")));
                continue;
            };
            match check_param(param, v, manifest_dir, home) {
                Ok(j) => {
                    params.insert(k.clone(), j);
                }
                Err(msg) => errs.push(at(
                    if param.kind == Kind::SecretRef { "E_BAD_SECRET_REF" } else { "E_BAD_PARAM" },
                    format!("{name} `{k}`: {msg}"),
                )),
            }
        }
    }
    for param in def.params.iter().filter(|p| p.required) {
        if !params.contains_key(param.key) && !errs.iter().any(|e| e.message.contains(&format!("`{}`", param.key))) {
            errs.push(at("E_BAD_PARAM", format!("{name} requires `{}`", param.key)));
        }
    }
    if name == "legacy-gfs@1" && errs.is_empty() {
        // Same defaults and the same refusal as the legacy CLI's retention_policy.
        for (k, d) in [("daily", 7), ("weekly", 4), ("monthly", 4)] {
            params.entry(k.to_string()).or_insert(Json::from(d));
        }
        if ["daily", "weekly", "monthly"].iter().all(|k| params[*k].as_i64() == Some(0)) {
            errs.push(at("E_BAD_PARAM", "legacy-gfs@1 daily, weekly and monthly are all 0, which would delete every backup".into()));
        }
    }
    if errs.is_empty() { Ok(StrategyRef { name, params }) } else { Err(errs) }
}

fn check_param(param: &Param, v: &Value, manifest_dir: &Path, home: &Path) -> Result<Json, String> {
    match param.kind {
        Kind::Path => {
            let s = v.as_str().ok_or("must be a path string")?;
            resolve_path(s, manifest_dir, home)?;
            Ok(Json::String(s.to_string()))
        }
        Kind::Prefix => {
            let s = v.as_str().ok_or("must be a string")?;
            if s.is_empty() || s.contains('/') || s.contains('\0') {
                return Err("must be a non-empty file prefix without `/`".into());
            }
            Ok(Json::String(s.to_string()))
        }
        Kind::PositiveInt | Kind::NonNegativeInt => {
            let n = v.as_integer().ok_or("must be an integer")?;
            let min = if param.kind == Kind::PositiveInt { 1 } else { 0 };
            if n < min {
                return Err(if min == 1 {
                    format!("must be >= 1 (got {n}); keeping 0 would delete every backup")
                } else {
                    format!("must be >= 0 (got {n})")
                });
            }
            Ok(Json::from(n))
        }
        Kind::SecretRef => {
            let s = v.as_str().ok_or("must be an op:// reference string")?;
            if !secret_ref_ok(s) {
                // Never echo the value: a raw credential typed here must not reach a log.
                return Err("is not an `op://<vault>/<item>/<field>` reference (value not shown)".into());
            }
            Ok(Json::String(s.to_string()))
        }
    }
}
