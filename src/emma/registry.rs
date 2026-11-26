//! The Emma registry: derived state in one local SQLite file.
//!
//! `$FOREVER_AGO_STATE_DIR` or `$XDG_STATE_HOME/forever-ago/registry.sqlite3`
//! (default `~/.local/state/forever-ago/`). Directory 0700, file 0600, WAL,
//! `user_version = 1`, foreign keys on. Only `register` writes it, under an
//! flock on `registry.lock` and inside one `BEGIN IMMEDIATE` transaction.
//! Every row except `generations` and `register_log` is recomputable from
//! the manifests plus `pj info`.

use super::envelope::{exit, Finding};
use chrono::{SecondsFormat, Utc};
use fs2::FileExt as _;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde_json::Value as Json;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

pub const DB_NAME: &str = "registry.sqlite3";
pub const LOCK_NAME: &str = "registry.lock";
pub const SCHEMA_VERSION: i64 = 1;

const SCHEMA_V1: &str = r#"
CREATE TABLE projects (
  project_key   TEXT PRIMARY KEY,
  identity      TEXT NOT NULL CHECK (identity IN ('pjangler','unbound')),
  project_id    TEXT,
  repo_path     TEXT NOT NULL,
  last_seen_at  TEXT NOT NULL
);
CREATE TABLE manifests (
  manifest_path TEXT PRIMARY KEY,
  project_key   TEXT NOT NULL REFERENCES projects(project_key),
  sha256        TEXT NOT NULL,
  state         TEXT NOT NULL CHECK (state IN ('ok','missing','invalid','moved')),
  last_error    TEXT,
  registered_at TEXT NOT NULL,
  last_seen_at  TEXT NOT NULL
);
CREATE TABLE emmas (
  project_key     TEXT NOT NULL REFERENCES projects(project_key),
  emma_id         TEXT NOT NULL,
  manifest_path   TEXT NOT NULL REFERENCES manifests(manifest_path),
  mode            TEXT NOT NULL CHECK (mode IN ('declared','adopted')),
  sole_copy       INTEGER NOT NULL DEFAULT 0,
  definition_json TEXT NOT NULL,
  definition_sha  TEXT NOT NULL,
  contract_sha    TEXT NOT NULL,
  generation      INTEGER NOT NULL,
  source_declared TEXT NOT NULL,
  source_canonical TEXT,
  dest_canonical  TEXT,
  candybar_ref    TEXT,
  sync_state      TEXT NOT NULL CHECK (sync_state IN ('ok','drift','orphan','retired')),
  first_registered_at TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  PRIMARY KEY (project_key, emma_id)
);
CREATE UNIQUE INDEX emmas_dest ON emmas(dest_canonical, COALESCE(json_extract(definition_json,'$.backend.prefix'),''))
  WHERE dest_canonical IS NOT NULL AND sync_state != 'retired';
CREATE TABLE generations (
  project_key TEXT NOT NULL, emma_id TEXT NOT NULL, generation INTEGER NOT NULL,
  contract_sha TEXT NOT NULL, started_at TEXT NOT NULL, reason TEXT NOT NULL,
  PRIMARY KEY (project_key, emma_id, generation)
);
CREATE TABLE register_log (
  id INTEGER PRIMARY KEY, at TEXT NOT NULL, verb TEXT NOT NULL,
  manifest_path TEXT, outcome TEXT NOT NULL, findings_json TEXT NOT NULL
);
"#;

pub fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Where the registry lives. `FOREVER_AGO_STATE_DIR` wins (tests), then
/// `$XDG_STATE_HOME/forever-ago`, then `~/.local/state/forever-ago`.
pub fn default_state_dir(home: &Path) -> PathBuf {
    if let Some(d) = std::env::var_os("FOREVER_AGO_STATE_DIR").map(PathBuf::from).filter(|p| p.is_absolute()) {
        return d;
    }
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .unwrap_or_else(|| home.join(".local/state"))
        .join("forever-ago")
}

#[derive(Clone, Debug, PartialEq)]
pub struct EmmaRow {
    pub project_key: String,
    pub emma_id: String,
    pub manifest_path: String,
    pub mode: String,
    pub sole_copy: bool,
    pub definition: Json,
    pub definition_sha: String,
    pub contract_sha: String,
    pub generation: i64,
    pub source_declared: String,
    pub source_canonical: Option<String>,
    pub dest_canonical: Option<String>,
    pub candybar_ref: Option<String>,
    pub sync_state: String,
    pub first_registered_at: String,
    pub updated_at: String,
}

impl EmmaRow {
    pub fn label(&self) -> String {
        format!("{}/{}", self.project_key, self.emma_id)
    }

    /// `(strategy name, params object)` for one axis of the stored definition.
    pub fn strategy(&self, axis: &str) -> (String, Json) {
        let v = self.definition.get(axis).cloned().unwrap_or(Json::Null);
        (v.get("strategy").and_then(Json::as_str).unwrap_or("").to_string(), v)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestRow {
    pub manifest_path: String,
    pub project_key: String,
    pub sha256: String,
    pub state: String,
    pub last_error: Option<String>,
    pub registered_at: String,
}

pub struct Registry {
    pub conn: Connection,
    _lock: Option<File>,
}

fn io_finding(code: &str, what: &str, err: impl std::fmt::Display) -> Finding {
    Finding::of(code, format!("{what}: {err}"))
}

impl Registry {
    /// Open for writing: create the 0700 directory and 0600 file, take the
    /// lock (busy is exit 5), migrate. Errors carry their exit code.
    pub fn open_write(dir: &Path) -> Result<Registry, (i32, Finding)> {
        let fail = |e: Finding| (exit::FAILURE, e);
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(|e| fail(io_finding("E_REGISTRY", &format!("cannot create {}", dir.display()), e)))?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
            .map_err(|e| fail(io_finding("E_REGISTRY", &format!("cannot chmod {}", dir.display()), e)))?;

        let lock_path = dir.join(LOCK_NAME);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&lock_path)
            .map_err(|e| fail(io_finding("E_REGISTRY", &format!("cannot open {}", lock_path.display()), e)))?;
        if lock.try_lock_exclusive().is_err() {
            return Err((
                exit::LOCK_BUSY,
                Finding::of("E_LOCK_BUSY", format!("another `forever-ago register` holds {}", lock_path.display())),
            ));
        }

        let path = dir.join(DB_NAME);
        if !path.exists() {
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&path)
                .map_err(|e| fail(io_finding("E_REGISTRY", &format!("cannot create {}", path.display()), e)))?;
        }
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|e| fail(io_finding("E_REGISTRY", &format!("cannot chmod {}", path.display()), e)))?;
        let conn = Connection::open(&path).map_err(|e| fail(io_finding("E_REGISTRY", "cannot open the registry", e)))?;
        let mut reg = Registry { conn, _lock: Some(lock) };
        reg.migrate().map_err(fail)?;
        Ok(reg)
    }

    /// Open read-only. `Ok(None)` when no registry exists yet. Never creates
    /// the directory or the file, never migrates.
    pub fn open_read(dir: &Path) -> Result<Option<Registry>, Finding> {
        let path = dir.join(DB_NAME);
        if !path.exists() {
            return Ok(None);
        }
        let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)
            .map_err(|e| io_finding("E_REGISTRY", &format!("cannot open {} read-only", path.display()), e))?;
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(|e| io_finding("E_REGISTRY", "cannot read the registry version", e))?;
        if version != SCHEMA_VERSION {
            return Err(Finding::of(
                "E_REGISTRY",
                format!("registry schema is v{version}; this forever-ago reads v{SCHEMA_VERSION} (run `forever-ago register` to migrate)"),
            ));
        }
        Ok(Some(Registry { conn, _lock: None }))
    }

    fn migrate(&mut self) -> Result<(), Finding> {
        let e = |err: rusqlite::Error| io_finding("E_REGISTRY", "registry migration failed", err);
        let mode: String = self.conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0)).map_err(e)?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Finding::of("E_REGISTRY", format!("could not enable WAL (journal_mode = {mode})")));
        }
        self.conn.execute_batch("PRAGMA foreign_keys = ON;").map_err(e)?;
        let version: i64 = self.conn.query_row("PRAGMA user_version", [], |r| r.get(0)).map_err(e)?;
        match version {
            0 => {
                let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(e)?;
                tx.execute_batch(SCHEMA_V1).map_err(e)?;
                tx.execute_batch("PRAGMA user_version = 1;").map_err(e)?;
                tx.commit().map_err(e)?;
                Ok(())
            }
            SCHEMA_VERSION => Ok(()),
            v => Err(Finding::of("E_REGISTRY", format!("registry schema v{v} is newer than this forever-ago (v{SCHEMA_VERSION})"))),
        }
    }

    pub fn emmas(&self) -> rusqlite::Result<Vec<EmmaRow>> {
        emmas_in(&self.conn)
    }

    pub fn manifests(&self) -> rusqlite::Result<Vec<ManifestRow>> {
        let mut st = self
            .conn
            .prepare("SELECT manifest_path, project_key, sha256, state, last_error, registered_at FROM manifests ORDER BY manifest_path")?;
        let rows = st.query_map([], |r| {
            Ok(ManifestRow {
                manifest_path: r.get(0)?,
                project_key: r.get(1)?,
                sha256: r.get(2)?,
                state: r.get(3)?,
                last_error: r.get(4)?,
                registered_at: r.get(5)?,
            })
        })?;
        rows.collect()
    }

    #[cfg(test)]
    pub fn generation_count(&self) -> rusqlite::Result<i64> {
        self.conn.query_row("SELECT COUNT(*) FROM generations", [], |r| r.get(0))
    }

    #[allow(dead_code)]
    pub fn last_log(&self) -> rusqlite::Result<Option<(String, String)>> {
        self.conn
            .query_row("SELECT verb, outcome FROM register_log ORDER BY id DESC LIMIT 1", [], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
    }
}

pub fn emmas_in(conn: &Connection) -> rusqlite::Result<Vec<EmmaRow>> {
    let mut st = conn.prepare(
        "SELECT project_key, emma_id, manifest_path, mode, sole_copy, definition_json, definition_sha, contract_sha,
                generation, source_declared, source_canonical, dest_canonical, candybar_ref, sync_state,
                first_registered_at, updated_at
         FROM emmas ORDER BY project_key, emma_id",
    )?;
    let rows = st.query_map([], |r| {
        let def: String = r.get(5)?;
        Ok(EmmaRow {
            project_key: r.get(0)?,
            emma_id: r.get(1)?,
            manifest_path: r.get(2)?,
            mode: r.get(3)?,
            sole_copy: r.get::<_, i64>(4)? != 0,
            definition: serde_json::from_str(&def).unwrap_or(Json::Null),
            definition_sha: r.get(6)?,
            contract_sha: r.get(7)?,
            generation: r.get(8)?,
            source_declared: r.get(9)?,
            source_canonical: r.get(10)?,
            dest_canonical: r.get(11)?,
            candybar_ref: r.get(12)?,
            sync_state: r.get(13)?,
            first_registered_at: r.get(14)?,
            updated_at: r.get(15)?,
        })
    })?;
    rows.collect()
}

/// The writes `register` applies, all in one transaction.
pub struct Tx<'a> {
    pub tx: rusqlite::Transaction<'a>,
    pub at: String,
}

impl Registry {
    pub fn begin(&mut self) -> rusqlite::Result<Tx<'_>> {
        Ok(Tx { tx: self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?, at: now() })
    }
}

/// What one Emma row is written from.
pub struct EmmaWrite<'a> {
    pub project_key: &'a str,
    pub emma_id: &'a str,
    pub manifest_path: &'a str,
    pub mode: &'a str,
    pub sole_copy: bool,
    pub definition: &'a Json,
    pub definition_sha: &'a str,
    pub contract_sha: &'a str,
    pub source_declared: &'a str,
    pub source_canonical: Option<&'a str>,
    pub dest_canonical: Option<&'a str>,
    pub candybar_ref: Option<&'a str>,
}

impl Tx<'_> {
    pub fn upsert_project(&self, key: &str, identity: &str, project_id: Option<&str>, repo_path: &str) -> rusqlite::Result<()> {
        self.tx.execute(
            "INSERT INTO projects (project_key, identity, project_id, repo_path, last_seen_at) VALUES (?1,?2,?3,?4,?5)
             ON CONFLICT(project_key) DO UPDATE SET identity=excluded.identity, project_id=excluded.project_id,
               repo_path=excluded.repo_path, last_seen_at=excluded.last_seen_at",
            params![key, identity, project_id, repo_path, self.at],
        )?;
        Ok(())
    }

    pub fn upsert_manifest(&self, path: &str, key: &str, sha: &str, state: &str, last_error: Option<&str>) -> rusqlite::Result<()> {
        self.tx.execute(
            "INSERT INTO manifests (manifest_path, project_key, sha256, state, last_error, registered_at, last_seen_at)
             VALUES (?1,?2,?3,?4,?5,?6,?6)
             ON CONFLICT(manifest_path) DO UPDATE SET project_key=excluded.project_key, sha256=excluded.sha256,
               state=excluded.state, last_error=excluded.last_error, last_seen_at=excluded.last_seen_at",
            params![path, key, sha, state, last_error, self.at],
        )?;
        Ok(())
    }

    /// Mark a known manifest without touching its definitions (`invalid`, `missing`, `moved`).
    pub fn mark_manifest(&self, path: &str, state: &str, last_error: Option<&str>) -> rusqlite::Result<()> {
        self.tx.execute(
            "UPDATE manifests SET state=?2, last_error=COALESCE(?3, last_error), last_seen_at=?4 WHERE manifest_path=?1",
            params![path, state, last_error, self.at],
        )?;
        Ok(())
    }

    pub fn insert_emma(&self, w: &EmmaWrite, reason: &str) -> rusqlite::Result<()> {
        self.tx.execute(
            "INSERT INTO emmas (project_key, emma_id, manifest_path, mode, sole_copy, definition_json, definition_sha,
               contract_sha, generation, source_declared, source_canonical, dest_canonical, candybar_ref, sync_state,
               first_registered_at, updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,1,?9,?10,?11,?12,'ok',?13,?13)",
            params![
                w.project_key, w.emma_id, w.manifest_path, w.mode, w.sole_copy as i64, w.definition.to_string(),
                w.definition_sha, w.contract_sha, w.source_declared, w.source_canonical, w.dest_canonical,
                w.candybar_ref, self.at
            ],
        )?;
        self.insert_generation(w.project_key, w.emma_id, 1, w.contract_sha, reason)
    }

    /// Replace a row's definition. Bumps the generation when the contract changed.
    pub fn update_emma(&self, w: &EmmaWrite, old: &EmmaRow, reason: &str) -> rusqlite::Result<i64> {
        let generation = if old.contract_sha != w.contract_sha { old.generation + 1 } else { old.generation };
        self.tx.execute(
            "UPDATE emmas SET manifest_path=?3, mode=?4, sole_copy=?5, definition_json=?6, definition_sha=?7,
               contract_sha=?8, generation=?9, source_declared=?10, source_canonical=?11, dest_canonical=?12,
               candybar_ref=?13, sync_state='ok', updated_at=?14
             WHERE project_key=?1 AND emma_id=?2",
            params![
                w.project_key, w.emma_id, w.manifest_path, w.mode, w.sole_copy as i64, w.definition.to_string(),
                w.definition_sha, w.contract_sha, generation, w.source_declared, w.source_canonical, w.dest_canonical,
                w.candybar_ref, self.at
            ],
        )?;
        if generation != old.generation {
            self.insert_generation(w.project_key, w.emma_id, generation, w.contract_sha, reason)?;
        }
        Ok(generation)
    }

    pub fn set_sync_state(&self, key: &str, id: &str, state: &str) -> rusqlite::Result<()> {
        self.tx.execute(
            "UPDATE emmas SET sync_state=?3, updated_at=?4 WHERE project_key=?1 AND emma_id=?2 AND sync_state != ?3",
            params![key, id, state, self.at],
        )?;
        Ok(())
    }

    /// A `pj rename`: move one manifest's Emmas (and their history) to the new key.
    pub fn rekey(&self, old_key: &str, new_key: &str, manifest_path: &str) -> rusqlite::Result<()> {
        let ids: Vec<String> = {
            let mut st = self.tx.prepare("SELECT emma_id FROM emmas WHERE project_key=?1 AND manifest_path=?2")?;
            st.query_map(params![old_key, manifest_path], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?
        };
        for id in ids {
            self.tx.execute(
                "UPDATE emmas SET project_key=?3, updated_at=?4 WHERE project_key=?1 AND emma_id=?2",
                params![old_key, id, new_key, self.at],
            )?;
            self.tx.execute(
                "UPDATE generations SET project_key=?3 WHERE project_key=?1 AND emma_id=?2",
                params![old_key, id, new_key],
            )?;
        }
        Ok(())
    }

    fn insert_generation(&self, key: &str, id: &str, generation: i64, contract_sha: &str, reason: &str) -> rusqlite::Result<()> {
        self.tx.execute(
            "INSERT INTO generations (project_key, emma_id, generation, contract_sha, started_at, reason) VALUES (?1,?2,?3,?4,?5,?6)",
            params![key, id, generation, contract_sha, self.at, reason],
        )?;
        Ok(())
    }

    pub fn log(&self, verb: &str, manifest_path: Option<&str>, outcome: &str, findings: &Json) -> rusqlite::Result<()> {
        self.tx.execute(
            "INSERT INTO register_log (at, verb, manifest_path, outcome, findings_json) VALUES (?1,?2,?3,?4,?5)",
            params![self.at, verb, manifest_path, outcome, findings.to_string()],
        )?;
        Ok(())
    }

    pub fn commit(self) -> rusqlite::Result<()> {
        self.tx.commit()
    }
}
