//! Project identity: consumed from pjangler (atom `project.identity`).
//!
//! Emma never mints an id. `pj info --json` run in the manifest's directory
//! answers; a directory pjangler does not know is `unbound`, keyed by its
//! canonical root. When pjangler's `GET /v1/resolve?cwd=` lands, `resolve`
//! is the one function to swap.

use super::envelope::Finding;
use super::run_bounded;
use serde_json::Value as Json;
use std::path::{Path, PathBuf};
use std::time::Duration;

const PJ_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Identity {
    Pjangler { project_id: String, repo_path: PathBuf },
    Unbound { root: PathBuf },
}

impl Identity {
    pub fn project_key(&self) -> String {
        match self {
            Identity::Pjangler { project_id, .. } => project_id.clone(),
            Identity::Unbound { root } => format!("path:{}", root.display()),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Identity::Pjangler { .. } => "pjangler",
            Identity::Unbound { .. } => "unbound",
        }
    }

    pub fn project_id(&self) -> Option<&str> {
        match self {
            Identity::Pjangler { project_id, .. } => Some(project_id),
            Identity::Unbound { .. } => None,
        }
    }

    pub fn repo_path(&self) -> &Path {
        match self {
            Identity::Pjangler { repo_path, .. } => repo_path,
            Identity::Unbound { root } => root,
        }
    }
}

/// `pj`, or `$FOREVER_AGO_PJ_BIN` (tests, Holocene, a pinned install).
pub fn pj_bin() -> String {
    std::env::var("FOREVER_AGO_PJ_BIN").unwrap_or_else(|_| "pj".into())
}

fn expand_home(p: &str, home: &Path) -> PathBuf {
    match p.strip_prefix("~/") {
        Some(rest) => home.join(rest),
        None if p == "~" => home.to_path_buf(),
        None => PathBuf::from(p),
    }
}

/// The git toplevel above `dir` (the nearest ancestor holding `.git`), or `dir`.
pub fn git_root(dir: &Path) -> PathBuf {
    let mut cur = Some(dir);
    while let Some(d) = cur {
        if d.join(".git").exists() {
            return d.to_path_buf();
        }
        cur = d.parent();
    }
    dir.to_path_buf()
}

/// True when `dir` is inside a linked git worktree (its toplevel's `.git` is a
/// file pointing into another repository's `worktrees/`).
pub fn in_linked_worktree(dir: &Path) -> bool {
    let root = git_root(dir);
    let dotgit = root.join(".git");
    if !dotgit.is_file() {
        return false;
    }
    std::fs::read_to_string(&dotgit)
        .ok()
        .and_then(|s| s.lines().find_map(|l| l.strip_prefix("gitdir:").map(|g| g.trim().to_string())))
        .is_some_and(|g| g.contains("/worktrees/"))
}

/// Resolve the project that owns `dir` through `pj info --json`.
pub fn resolve(pj: &str, dir: &Path, home: &Path) -> Result<Identity, Finding> {
    let out = run_bounded(pj, &["info", "--json"], Some(dir), PJ_TIMEOUT)
        .map_err(|e| Finding::of("E_IDENTITY_UNAVAILABLE", format!("pj info --json failed in {}: {e}", dir.display())))?;
    let v: Json = serde_json::from_str(&out.stdout).map_err(|_| {
        Finding::of(
            "E_IDENTITY_UNAVAILABLE",
            format!("pj info --json in {} did not return JSON (exit {:?})", dir.display(), out.code),
        )
    })?;
    if v.get("ok").and_then(Json::as_bool) == Some(true) {
        let id = v.get("project_id").and_then(Json::as_str);
        let repo = v.get("repo_path").and_then(Json::as_str);
        return match (id, repo) {
            (Some(id), Some(repo)) if !id.is_empty() => {
                let repo = expand_home(repo, home);
                Ok(Identity::Pjangler {
                    project_id: id.to_string(),
                    repo_path: std::fs::canonicalize(&repo).unwrap_or(repo),
                })
            }
            _ => Err(Finding::of("E_IDENTITY_UNAVAILABLE", "pj info --json returned ok without project_id/repo_path")),
        };
    }
    let msg = v.get("error").and_then(Json::as_str).unwrap_or("");
    if msg.contains("Not inside a project") || msg.contains("no .project.json") {
        let root = git_root(dir);
        return Ok(Identity::Unbound { root: std::fs::canonicalize(&root).unwrap_or(root) });
    }
    Err(Finding::of("E_IDENTITY_UNAVAILABLE", format!("pj info --json in {}: {msg}", dir.display())))
}

/// Every registered project's `(project_id, repo_path)` from `pj list --json`.
pub fn list_projects(pj: &str, home: &Path) -> Result<Vec<(String, PathBuf)>, Finding> {
    let out = run_bounded(pj, &["list", "--json"], None, PJ_TIMEOUT)
        .map_err(|e| Finding::of("E_IDENTITY_UNAVAILABLE", format!("pj list --json failed: {e}")))?;
    let v: Json = serde_json::from_str(&out.stdout)
        .map_err(|_| Finding::of("E_IDENTITY_UNAVAILABLE", "pj list --json did not return JSON"))?;
    let Some(projects) = v.get("projects").and_then(Json::as_object) else {
        return Err(Finding::of("E_IDENTITY_UNAVAILABLE", "pj list --json has no `projects` map"));
    };
    let mut out: Vec<(String, PathBuf)> = projects
        .iter()
        .filter_map(|(key, p)| {
            let id = p.get("project_id").and_then(Json::as_str).unwrap_or(key);
            let repo = p.get("repo_path").and_then(Json::as_str)?;
            Some((id.to_string(), expand_home(repo, home)))
        })
        .collect();
    out.sort();
    Ok(out)
}
