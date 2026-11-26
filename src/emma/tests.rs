//! Slice-1 acceptance tests (EMMA-DESIGN section 8). No test runs a legacy
//! backup, touches the real registry, or calls the real pj or candybar.

use super::envelope::exit;
use super::manifest::{parse_str, MANIFEST_NAME};
use super::register::{register, RegisterOpts};
use super::retention::{self, Snap, SnapStatus};
use super::*;
use chrono::TimeZone;
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Fixtures: a fake home, fake `pj` and `candybar`, a private state dir.
// ---------------------------------------------------------------------------

const FAKE_PJ: &str = r#"#!/bin/sh
d=$(dirname "$0")
case "$1" in
  list) cat "$d/projects.json"; exit 0 ;;
  info)
    p=$(pwd -P)
    while [ "$p" != "/" ]; do
      if [ -f "$p/.fakepj" ]; then
        id=$(cut -f1 "$p/.fakepj"); repo=$(cut -s -f2 "$p/.fakepj")
        [ -z "$repo" ] && repo="$p"
        printf '{"ok":true,"project_id":"%s","repo_path":"%s"}\n' "$id" "$repo"; exit 0
      fi
      p=$(dirname "$p")
    done
    echo '{"ok":false,"error":"Not inside a project: no .project.json found. Pass a project ID or run pj init."}'; exit 1 ;;
esac
exit 2
"#;

const FAKE_CANDYBAR: &str = r#"#!/bin/sh
d=$(dirname "$0")
if [ "$1" = show ] && [ -f "$d/cb/$2.json" ]; then cat "$d/cb/$2.json"; exit 0; fi
echo "no candybar named '$2'" >&2
exit 2
"#;

/// Written once per test process and only symlinked afterwards, so no test
/// ever execs a file another thread still has open for writing.
fn shared_scripts() -> &'static PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("forever-ago-emma-test-bin-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        for (name, body) in [("pj", FAKE_PJ), ("candybar", FAKE_CANDYBAR)] {
            let p = dir.join(name);
            fs::write(&p, body).unwrap();
            fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
        }
        dir
    })
}

struct Fx {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    state: PathBuf,
    bin: PathBuf,
    projects: std::cell::RefCell<Vec<(String, PathBuf)>>,
}

impl Fx {
    fn new() -> Fx {
        let tmp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(tmp.path()).unwrap();
        let home = root.join("home");
        let bin = root.join("bin");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(bin.join("cb")).unwrap();
        for name in ["pj", "candybar"] {
            symlink(shared_scripts().join(name), bin.join(name)).unwrap();
        }
        let fx = Fx { state: root.join("state/forever-ago"), _tmp: tmp, home, bin, projects: Default::default() };
        fx.write_projects();
        fx
    }

    fn write_projects(&self) {
        let map: serde_json::Map<String, Json> = self
            .projects
            .borrow()
            .iter()
            .map(|(id, p)| (id.clone(), json!({"project_id": id, "repo_path": p.display().to_string()})))
            .collect();
        fs::write(self.bin.join("projects.json"), json!({"schema_version": 1, "projects": map}).to_string()).unwrap();
    }

    /// A git checkout at `~/rel`, known to fake pjangler as `id`.
    fn project(&self, id: &str, rel: &str) -> PathBuf {
        let dir = self.home.join(rel);
        fs::create_dir_all(dir.join(".git")).unwrap();
        fs::write(dir.join(".fakepj"), id).unwrap();
        self.projects.borrow_mut().push((id.into(), dir.clone()));
        self.write_projects();
        dir
    }

    fn candybar(&self, name: &str, body: Json) {
        fs::write(self.bin.join("cb").join(format!("{name}.json")), body.to_string()).unwrap();
    }

    fn env(&self) -> Env {
        Env {
            home: self.home.clone(),
            state_dir: self.state.clone(),
            pj: self.bin.join("pj").display().to_string(),
            candybar: self.bin.join("candybar").display().to_string(),
            jobs: Some(Vec::new()),
        }
    }

    fn reg(&self, path: Option<&Path>, all: bool, dry_run: bool, update: bool, prune: bool) -> register::Outcome {
        let opts = RegisterOpts { path: path.map(Path::to_path_buf), all, dry_run, update, prune };
        let mut env = self.env();
        register(&opts, &mut env, &self.home)
    }

    fn rows(&self) -> Vec<EmmaRow> {
        Registry::open_read(&self.state).unwrap().map(|r| r.emmas().unwrap()).unwrap_or_default()
    }
}

fn codes(o: &register::Outcome) -> Vec<String> {
    o.findings.iter().map(|f| f.code.clone()).collect()
}

fn states(o: &register::Outcome) -> Vec<String> {
    o.data["emmas"].as_array().map(|a| a.iter().map(|e| e["state"].as_str().unwrap().to_string()).collect()).unwrap_or_default()
}

/// A files Emma backing up `src` (relative to the manifest) into `dest`.
fn files_manifest(id: &str, src: &str, dest: &str, retention: &str) -> String {
    format!(
        r#"schema_version = 1

[[emmas]]
id = "{id}"

[emmas.source]
kind = "files"
path = "{src}"

[emmas.capture]
strategy = "files@1"

[emmas.backend]
strategy = "tar-gz-dir@1"
dir = "{dest}"
prefix = "{id}"

[emmas.retention]
{retention}

[emmas.fidelity]
strategy = "full@1"
"#
    )
}

fn parse_in(dir: &Path, home: &Path, text: &str) -> (Vec<manifest::EmmaDef>, Vec<String>) {
    let (defs, findings) = parse_str(text, dir, home, true);
    (defs, findings.iter().map(|f| f.code.clone()).collect())
}

/// A source dir `data` plus a manifest dir, under a fresh temp home.
fn scratch() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    fs::create_dir_all(root.join("proj/data")).unwrap();
    (tmp, root)
}

// ---------------------------------------------------------------------------
// Manifest validation (3.2): a positive and a negative case per rule.
// ---------------------------------------------------------------------------

#[test]
fn minimal_manifest_parses_and_relative_paths_resolve_against_the_manifest_not_the_cwd() {
    let (_t, root) = scratch();
    let proj = root.join("proj");
    let (defs, codes) = parse_in(&proj, &root, &files_manifest("data", "data", "../backups", r#"strategy = "keep-all@1""#));
    assert!(codes.is_empty(), "{codes:?}");
    let d = &defs[0];
    assert_eq!(d.source_canonical.as_deref(), Some(proj.join("data").as_path()));
    assert_eq!(d.dest_canonical.as_deref(), Some(root.join("backups").as_path()));
    assert_ne!(std::env::current_dir().unwrap(), proj);
    // String shorthand for an axis is accepted.
    let short = "schema_version = 1\n[[emmas]]\nid = \"data\"\ncapture = \"files@1\"\nretention = \"observe-only@1\"\nfidelity = \"full@1\"\n[emmas.source]\nkind = \"files\"\npath = \"data\"\n[emmas.backend]\nstrategy = \"tar-gz-dir@1\"\ndir = \"../backups\"\nprefix = \"data\"\n";
    let (defs, codes) = parse_in(&proj, &root, short);
    assert!(codes.is_empty(), "{codes:?}");
    assert_eq!(defs[0].fidelity.name, "full@1");
    assert_eq!(defs[0].retention.name, "observe-only@1");
}

#[test]
fn schema_version_is_required_and_must_be_1() {
    let (_t, root) = scratch();
    let good = files_manifest("data", "data", "../b", r#"strategy = "keep-all@1""#);
    assert_eq!(parse_in(&root.join("proj"), &root, &good.replace("schema_version = 1\n", "")).1, vec!["E_SCHEMA_VERSION"]);
    assert_eq!(parse_in(&root.join("proj"), &root, &good.replace("schema_version = 1", "schema_version = 2")).1, vec!["E_SCHEMA_VERSION"]);
    assert_eq!(parse_in(&root.join("proj"), &root, "schema_version = 1\n[[emmas\n").1, vec!["E_PARSE"]);
}

#[test]
fn unknown_keys_are_errors_at_every_level() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let base = files_manifest("data", "data", "../b", r#"strategy = "keep-all@1""#);
    for bad in [
        base.replace("schema_version = 1", "schema_version = 1\nextra = 1"),
        base.replace("id = \"data\"", "id = \"data\"\ncolour = \"red\""),
        base.replace("kind = \"files\"", "kind = \"files\"\nfollow_symlinks = true"),
    ] {
        assert!(parse_in(&p, &root, &bad).1.contains(&"E_UNKNOWN_KEY".to_string()), "{bad}");
    }
    let bad_param = base.replace("prefix = \"data\"", "prefix = \"data\"\nkeep = 3");
    assert_eq!(parse_in(&p, &root, &bad_param).1, vec!["E_BAD_PARAM"]);
}

#[test]
fn clock_identity_and_secret_keys_are_refused_with_a_pointer_to_their_owner() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let base = files_manifest("data", "data", "../b", r#"strategy = "keep-all@1""#);
    for (key, code, hint) in [
        ("schedule = \"0 3 * * *\"", "E_CLOCK_KEY", "Candybar owns when"),
        ("cron = \"x\"", "E_CLOCK_KEY", "Candybar owns when"),
        ("project_id = \"x\"", "E_IDENTITY_KEY", "pjangler"),
        ("uuid = \"x\"", "E_IDENTITY_KEY", "pjangler"),
        ("password = \"hunter2\"", "E_SECRET_KEY", "password_ref"),
        ("api_token = \"x\"", "E_SECRET_KEY", "password_ref"),
    ] {
        let text = base.replace("id = \"data\"", &format!("id = \"data\"\n{key}"));
        let (_, findings) = parse_str(&text, &p, &root, true);
        let f = findings.iter().find(|f| f.code == code).unwrap_or_else(|| panic!("{key}: {findings:?}"));
        assert!(f.message.contains(hint), "{}", f.message);
        assert!(!f.message.contains("hunter2"), "a secret value must never be echoed");
    }
    // Also inside a nested table.
    let nested = base.replace("prefix = \"data\"", "prefix = \"data\"\ntimer = \"daily\"");
    assert!(parse_in(&p, &root, &nested).1.contains(&"E_CLOCK_KEY".to_string()));
}

#[test]
fn password_ref_must_be_an_op_reference_and_is_never_echoed() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let mk = |r: &str| {
        files_manifest("data", "data", "../b", r#"strategy = "keep-all@1""#)
            .replace("strategy = \"tar-gz-dir@1\"\ndir = \"../b\"\nprefix = \"data\"", &format!("strategy = \"restic-local@1\"\nrepo = \"../r.restic\"\npassword_ref = \"{r}\""))
    };
    assert!(parse_in(&p, &root, &mk("op://DeLoSecrets/emma-restic/password")).1.is_empty());
    let (_, findings) = parse_str(&mk("sup3r-s3cret-value"), &p, &root, true);
    assert_eq!(findings.iter().map(|f| f.code.as_str()).collect::<Vec<_>>(), vec!["E_BAD_SECRET_REF"]);
    assert!(!findings[0].message.contains("sup3r"));
    assert_eq!(parse_in(&p, &root, &mk("op://only/two")).1, vec!["E_BAD_SECRET_REF"]);
}

#[test]
fn strategy_refs_must_be_known_versioned_on_the_right_axis_and_not_reserved() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let with = |r: &str| files_manifest("data", "data", "../b", &format!("strategy = \"{r}\""));
    assert_eq!(parse_in(&p, &root, &with("age-buckets@1")).1, Vec::<String>::new());
    assert_eq!(parse_in(&p, &root, &with("age-buckets@9")).1, vec!["E_UNKNOWN_STRATEGY"]);
    assert_eq!(parse_in(&p, &root, &with("age-buckets")).1, vec!["E_UNKNOWN_STRATEGY"]);
    assert_eq!(parse_in(&p, &root, &with("latest")).1, vec!["E_UNKNOWN_STRATEGY"]);
    assert_eq!(parse_in(&p, &root, &with("full@1")).1, vec!["E_UNKNOWN_STRATEGY"]);
    let lossy = with("keep-all@1").replace("strategy = \"full@1\"", "strategy = \"conversation-prompts@1\"");
    assert_eq!(parse_in(&p, &root, &lossy).1, vec!["E_RESERVED_STRATEGY"]);
    let pg = with("keep-all@1").replace("strategy = \"files@1\"", "strategy = \"postgres-logical@1\"");
    assert_eq!(parse_in(&p, &root, &pg).1, vec!["E_RESERVED_STRATEGY"]);
}

#[test]
fn zero_and_invalid_retention_policies_are_refused() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let with = |r: &str| files_manifest("data", "data", "../b", r);
    assert!(parse_in(&p, &root, &with("strategy = \"legacy-count@1\"\nkeep = 3")).1.is_empty());
    assert_eq!(parse_in(&p, &root, &with("strategy = \"legacy-count@1\"\nkeep = 0")).1, vec!["E_BAD_PARAM"]);
    assert_eq!(parse_in(&p, &root, &with("strategy = \"legacy-count@1\"\nkeep = -1")).1, vec!["E_BAD_PARAM"]);
    assert_eq!(parse_in(&p, &root, &with("strategy = \"legacy-count@1\"")).1, vec!["E_BAD_PARAM"]);
    assert_eq!(parse_in(&p, &root, &with("strategy = \"legacy-count@1\"\nkeep = \"3\"")).1, vec!["E_BAD_PARAM"]);
    assert_eq!(parse_in(&p, &root, &with("strategy = \"legacy-gfs@1\"\ndaily = 0\nweekly = 0\nmonthly = 0")).1, vec!["E_BAD_PARAM"]);
    // Unset GFS tiers take the legacy CLI's 7/4/4 defaults.
    let (defs, codes) = parse_in(&p, &root, &with("strategy = \"legacy-gfs@1\"\nweekly = 2"));
    assert!(codes.is_empty());
    let r = &defs[0].retention;
    assert_eq!((r.int_param("daily"), r.int_param("weekly"), r.int_param("monthly")), (Some(7), Some(2), Some(4)));
    assert_eq!(parse_in(&p, &root, &with("strategy = \"age-buckets@1\"\nkeep = 3")).1, vec!["E_BAD_PARAM"]);
}

#[test]
fn sole_copy_forbids_thinning() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let with = |r: &str| files_manifest("data", "data", "../b", r).replace("id = \"data\"", "id = \"data\"\nsole_copy = true");
    assert!(parse_in(&p, &root, &with("strategy = \"keep-all@1\"")).1.is_empty());
    assert!(parse_in(&p, &root, &with("strategy = \"observe-only@1\"")).1.is_empty());
    assert_eq!(parse_in(&p, &root, &with("strategy = \"age-buckets@1\"")).1, vec!["E_SOLE_COPY_RETENTION"]);
    assert_eq!(parse_in(&p, &root, &with("strategy = \"legacy-count@1\"\nkeep = 5")).1, vec!["E_SOLE_COPY_RETENTION"]);
}

#[test]
fn activation_fails_closed() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let base = files_manifest("data", "data", "../b", r#"strategy = "keep-all@1""#);
    assert!(parse_in(&p, &root, &format!("{base}\n[emmas.activation]\ncapture = false\nprune = false\n")).1.is_empty());
    assert_eq!(parse_in(&p, &root, &format!("{base}\n[emmas.activation]\ncapture = true\n")).1, vec!["E_NOT_IMPLEMENTED"]);
    assert_eq!(parse_in(&p, &root, &format!("{base}\n[emmas.activation]\nprune = true\n")).1, vec!["E_NOT_IMPLEMENTED"]);
    assert_eq!(parse_in(&p, &root, &format!("{base}\n[emmas.budget]\nstored_bytes_max = 0\n")).1, vec!["E_BAD_PARAM"]);
    assert!(parse_in(&p, &root, &format!("{base}\n[emmas.budget]\nstored_bytes_max = 1000\n")).1.is_empty());
}

#[test]
fn destination_placement_rules() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let keep = r#"strategy = "keep-all@1""#;
    assert_eq!(parse_in(&p, &root, &files_manifest("data", "data", "data/backups", keep)).1, vec!["E_DEST_INSIDE_SOURCE"]);
    assert_eq!(parse_in(&p, &root, &files_manifest("data", "data", "data", keep)).1, vec!["E_DEST_INSIDE_SOURCE"]);
    // Source inside the destination is only a warning.
    let (defs, codes) = parse_in(&p, &root, &files_manifest("data", "data", "..", keep));
    assert_eq!(codes, vec!["W_SOURCE_INSIDE_DEST"]);
    assert_eq!(defs.len(), 1);
}

#[test]
fn ids_are_unique_slugs_and_overlapping_sources_warn() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    fs::create_dir_all(p.join("data/sub")).unwrap();
    let one = files_manifest("data", "data", "../b", r#"strategy = "keep-all@1""#);
    let body = one.trim_start_matches("schema_version = 1\n");
    let dup = format!("{one}{}", body.replace("prefix = \"data\"", "prefix = \"other\""));
    assert!(parse_in(&p, &root, &dup).1.contains(&"E_DUPLICATE_ID".to_string()));
    let overlap = format!("{one}{}", body.replace("id = \"data\"", "id = \"sub\"").replace("path = \"data\"", "path = \"data/sub\"").replace("prefix = \"data\"", "prefix = \"sub\""));
    let (defs, codes) = parse_in(&p, &root, &overlap);
    assert_eq!(codes, vec!["W_SOURCE_OVERLAP"]);
    assert_eq!(defs.len(), 2);
    for bad in ["Data", "-x", "x-", "a_b", &"a".repeat(49)] {
        assert_eq!(parse_in(&p, &root, &one.replace("id = \"data\"", &format!("id = \"{bad}\""))).1, vec!["E_BAD_ID"], "{bad}");
    }
}

#[test]
fn source_must_exist_and_symlinks_canonicalise() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let keep = r#"strategy = "keep-all@1""#;
    assert_eq!(parse_in(&p, &root, &files_manifest("data", "nope", "../b", keep)).1, vec!["E_SOURCE_MISSING"]);
    symlink(p.join("data"), p.join("link")).unwrap();
    let (defs, codes) = parse_in(&p, &root, &files_manifest("data", "link", "../b", keep));
    assert!(codes.is_empty());
    assert_eq!(defs[0].source_canonical.as_deref(), Some(p.join("data").as_path()));
    assert_eq!(defs[0].source_resolved.as_deref(), Some(p.join("link").as_path()));
    // A destination inside the source through a symlink is still caught.
    assert_eq!(parse_in(&p, &root, &files_manifest("data", "link", "data/bk", keep)).1, vec!["E_DEST_INSIDE_SOURCE"]);
}

#[test]
fn paths_expand_home_but_never_variables() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let keep = r#"strategy = "keep-all@1""#;
    let (defs, codes) = parse_in(&p, &root, &files_manifest("data", "~/proj/data", "~/b", keep));
    assert!(codes.is_empty(), "{codes:?}");
    assert_eq!(defs[0].dest_canonical.as_deref(), Some(root.join("b").as_path()));
    assert_eq!(parse_in(&p, &root, &files_manifest("data", "$HOME/proj/data", "~/b", keep)).1, vec!["E_BAD_PARAM"]);
    assert_eq!(parse_in(&p, &root, &files_manifest("data", "~bob/data", "~/b", keep)).1, vec!["E_BAD_PARAM"]);
}

#[test]
fn candybar_ref_mode_and_exclude_rules() {
    let (_t, root) = scratch();
    let p = root.join("proj");
    let base = files_manifest("data", "data", "../b", r#"strategy = "keep-all@1""#);
    assert!(parse_in(&p, &root, &format!("{base}\n[emmas.candybar]\nref = \"forever-ago-vault\"\n")).1.is_empty());
    assert_eq!(parse_in(&p, &root, &format!("{base}\n[emmas.candybar]\nref = \"01a17947-520D\"\n")).1, vec!["E_BAD_CANDYBAR_REF"]);
    assert_eq!(parse_in(&p, &root, &base.replace("id = \"data\"", "id = \"data\"\nmode = \"managed\"")).1, vec!["E_RESERVED_MODE"]);
    assert_eq!(parse_in(&p, &root, &base.replace("id = \"data\"", "id = \"data\"\nmode = \"adopted\"")).1, vec!["E_ADOPT_SHAPE"]);
    assert_eq!(parse_in(&p, &root, &base.replace("path = \"data\"", "path = \"data\"\nexclude = [\"a/*/b\"]")).1, vec!["E_BAD_PARAM"]);
    assert!(parse_in(&p, &root, &base.replace("path = \"data\"", "path = \"data\"\nexclude = [\"*.aup4\", \"node_modules\"]")).1.is_empty());
    fs::write(p.join(".ex"), "# c\n.venv\n").unwrap();
    assert!(parse_in(&p, &root, &base.replace("path = \"data\"", "path = \"data\"\nexclude_from = \".ex\"")).1.is_empty());
    assert_eq!(parse_in(&p, &root, &base.replace("path = \"data\"", "path = \"data\"\nexclude_from = \".nope\"")).1, vec!["E_BAD_PARAM"]);
    // capture must fit the source kind
    assert_eq!(parse_in(&p, &root, &base.replace("strategy = \"files@1\"", "strategy = \"command-export@1\"\nstaging = \"../s\"")).1, vec!["E_BAD_PARAM"]);
}

const CANDYSTORE_MANIFEST: &str = include_str!("../../docs/examples/candystore.emma.toml");

#[test]
fn the_candystore_manifest_validates() {
    let tmp = tempfile::tempdir().unwrap();
    let home = fs::canonicalize(tmp.path()).unwrap();
    fs::create_dir_all(home.join("backups/candystore/event-archive")).unwrap();
    fs::create_dir_all(home.join("code/33GOD/candystore")).unwrap();
    let (defs, findings) = parse_str(CANDYSTORE_MANIFEST, &home.join("code/33GOD/candystore"), &home, true);
    assert!(findings.is_empty(), "{findings:?}");
    assert_eq!(defs.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(), vec!["event-archive", "event-export"]);
    assert!(defs[0].sole_copy);
    assert_eq!(defs[0].retention.name, "keep-all@1");
    assert_eq!(defs[1].retention.name, "age-buckets@1");
    assert!(defs.iter().all(|d| !d.activation_capture && !d.activation_prune && d.candybar_ref.is_none()));
}

// ---------------------------------------------------------------------------
// Registry: atomicity, drift, identity.
// ---------------------------------------------------------------------------

#[test]
fn registry_is_private_and_dry_run_creates_nothing() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();

    let dry = fx.reg(Some(&p), false, true, false, false);
    assert_eq!(dry.exit, exit::OK, "{:?}", dry.findings);
    assert_eq!(states(&dry), vec!["new"]);
    assert!(!fx.state.exists(), "--dry-run must not create the state dir");
    assert!(!fx.state.parent().unwrap().exists());

    let o = fx.reg(Some(&p), false, false, false, false);
    assert_eq!(o.exit, exit::OK, "{:?}", o.findings);
    let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&fx.state), 0o700);
    assert_eq!(mode(&fx.state.join(registry::DB_NAME)), 0o600);
    let rows = fx.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].project_key.as_str(), rows[0].emma_id.as_str(), rows[0].generation), ("alpha", "data", 1));

    let again = fx.reg(Some(&p), false, false, false, false);
    assert_eq!(again.exit, exit::OK);
    assert_eq!(states(&again), vec!["unchanged"]);
    assert_eq!(fx.rows(), rows, "unchanged must not rewrite the row");
}

#[test]
fn retention_edit_is_drift_until_update_and_never_bumps_generation() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    let m = p.join(MANIFEST_NAME);
    fs::write(&m, files_manifest("data", "data", "~/backups", "strategy = \"legacy-count@1\"\nkeep = 3")).unwrap();
    assert_eq!(fx.reg(Some(&p), false, false, false, false).exit, exit::OK);

    fs::write(&m, files_manifest("data", "data", "~/backups", "strategy = \"legacy-count@1\"\nkeep = 5")).unwrap();
    let o = fx.reg(Some(&p), false, false, false, false);
    assert_eq!(o.exit, exit::DRIFT);
    assert_eq!(states(&o), vec!["drift:retention"]);
    let row = &fx.rows()[0];
    assert_eq!(row.sync_state, "drift");
    assert_eq!(row.definition["retention"]["keep"], 3, "drift is reported, not applied");

    let u = fx.reg(Some(&p), false, false, true, false);
    assert_eq!(u.exit, exit::OK);
    let row = &fx.rows()[0];
    assert_eq!((row.definition["retention"]["keep"].as_i64(), row.generation, row.sync_state.as_str()), (Some(5), 1, "ok"));
}

#[test]
fn source_edit_bumps_generation_and_records_history() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    fs::create_dir_all(p.join("other")).unwrap();
    let m = p.join(MANIFEST_NAME);
    fs::write(&m, files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    fs::write(&m, files_manifest("data", "other", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    let o = fx.reg(Some(&p), false, false, true, false);
    assert_eq!(o.exit, exit::OK);
    assert!(states(&o)[0].starts_with("drift:"), "{:?}", states(&o));
    assert!(states(&o)[0].contains("source"));
    let row = &fx.rows()[0];
    assert_eq!(row.generation, 2);
    assert!(row.source_canonical.as_deref().unwrap().ends_with("/other"));
    let reg = Registry::open_read(&fx.state).unwrap().unwrap();
    assert_eq!(reg.generation_count().unwrap(), 2);
}

#[test]
fn a_moved_repo_keeps_its_identity() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    let moved = fx.home.join("code/alpha-moved");
    fs::rename(&p, &moved).unwrap();

    let o = fx.reg(Some(&moved), false, false, false, false);
    assert_eq!(o.exit, exit::DRIFT, "{:?}", o.findings);
    assert_eq!(states(&o), vec!["moved"]);
    let u = fx.reg(Some(&moved), false, false, true, false);
    assert_eq!(u.exit, exit::OK, "{:?}", u.findings);
    let rows = fx.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].project_key, "alpha");
    assert!(rows[0].manifest_path.starts_with(&moved.display().to_string()));
    let reg = Registry::open_read(&fx.state).unwrap().unwrap();
    let old = reg.manifests().unwrap().into_iter().find(|m| m.manifest_path.contains("/alpha/")).unwrap();
    assert_eq!(old.state, "moved");
}

#[test]
fn two_manifests_claiming_one_emma_is_a_conflict_and_changes_nothing() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    let before = fx.rows();

    // A copy of the checkout, also bound to `alpha`, declaring the same id.
    let copy = fx.home.join("code/alpha-copy");
    fs::create_dir_all(copy.join("data")).unwrap();
    fs::create_dir_all(copy.join(".git")).unwrap();
    fs::write(copy.join(".fakepj"), "alpha").unwrap();
    fs::write(copy.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups2", r#"strategy = "keep-all@1""#)).unwrap();
    let o = fx.reg(Some(&copy), false, false, true, false);
    assert_eq!(o.exit, exit::REFUSED, "{:?}", o.findings);
    assert!(codes(&o).contains(&"E_CONFLICT".to_string()));
    assert_eq!(fx.rows(), before);
}

#[test]
fn destination_collision_is_refused() {
    let fx = Fx::new();
    let a = fx.project("alpha", "code/alpha");
    let b = fx.project("beta", "code/beta");
    for p in [&a, &b] {
        fs::create_dir_all(p.join("data")).unwrap();
        fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    }
    assert_eq!(fx.reg(Some(&a), false, false, false, false).exit, exit::OK);
    let o = fx.reg(Some(&b), false, false, false, false);
    assert_eq!(o.exit, exit::REFUSED);
    assert!(codes(&o).contains(&"E_DEST_COLLISION".to_string()));
    assert_eq!(fx.rows().len(), 1);
}

#[test]
fn a_manifest_in_a_linked_worktree_is_not_the_owner() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    let wt = fx.home.join("code/alpha-wt");
    fs::create_dir_all(wt.join("data")).unwrap();
    fs::write(wt.join(".git"), format!("gitdir: {}/.git/worktrees/alpha-wt\n", p.display())).unwrap();
    fs::write(wt.join(".fakepj"), format!("alpha\t{}", p.display())).unwrap();
    fs::write(wt.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    let o = fx.reg(Some(&wt), false, false, false, false);
    assert_eq!(o.exit, exit::REFUSED);
    assert_eq!(codes(&o), vec!["E_NOT_OWNER"]);
    assert!(fx.rows().is_empty());

    // Even one nested inside the canonical checkout.
    let inner = p.join(".worktrees/x");
    fs::create_dir_all(inner.join("data")).unwrap();
    fs::write(inner.join(".git"), format!("gitdir: {}/.git/worktrees/x\n", p.display())).unwrap();
    fs::write(inner.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    let o = fx.reg(Some(&inner), false, false, false, false);
    assert_eq!(o.exit, exit::REFUSED);
}

#[test]
fn an_unbound_directory_is_keyed_by_path_with_a_warning() {
    let fx = Fx::new();
    let d = fx.home.join("loose");
    fs::create_dir_all(d.join("data")).unwrap();
    fs::create_dir_all(d.join(".git")).unwrap();
    fs::write(d.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    let o = fx.reg(Some(&d), false, false, false, false);
    assert_eq!(o.exit, exit::OK);
    assert_eq!(codes(&o), vec!["W_UNBOUND"]);
    assert_eq!(fx.rows()[0].project_key, format!("path:{}", d.display()));
}

#[test]
fn a_corrupt_manifest_keeps_the_last_valid_definition() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    let m = p.join(MANIFEST_NAME);
    fs::write(&m, files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    let before = fx.rows();
    fs::write(&m, "schema_version = 1\n[[emmas]\n").unwrap();
    let o = fx.reg(Some(&p), false, false, true, false);
    assert_eq!(o.exit, exit::USAGE);
    assert_eq!(fx.rows(), before);
    let reg = Registry::open_read(&fx.state).unwrap().unwrap();
    let mrow = reg.manifests().unwrap().pop().unwrap();
    assert_eq!(mrow.state, "invalid");
    assert!(mrow.last_error.unwrap().contains("E_PARSE"));
}

#[test]
fn orphans_are_marked_and_prune_retires_without_deleting() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    fs::create_dir_all(p.join("more")).unwrap();
    let m = p.join(MANIFEST_NAME);
    let two = format!(
        "{}{}",
        files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#),
        files_manifest("more", "more", "~/backups", r#"strategy = "keep-all@1""#).trim_start_matches("schema_version = 1\n")
    );
    fs::write(&m, two).unwrap();
    assert_eq!(fx.reg(Some(&p), false, false, false, false).exit, exit::OK);
    fs::write(&m, files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    let o = fx.reg(Some(&p), false, false, false, false);
    assert!(states(&o).contains(&"orphan".to_string()), "{:?}", states(&o));
    assert_eq!(fx.rows().iter().find(|r| r.emma_id == "more").unwrap().sync_state, "orphan");
    fx.reg(Some(&p), false, false, false, true);
    let rows = fx.rows();
    assert_eq!(rows.len(), 2, "prune never deletes a row");
    assert_eq!(rows.iter().find(|r| r.emma_id == "more").unwrap().sync_state, "retired");
}

#[test]
fn deleting_the_registry_and_registering_all_rebuilds_the_same_rows() {
    let fx = Fx::new();
    let a = fx.project("alpha", "code/alpha");
    let b = fx.project("beta", "code/beta");
    fx.project("gamma", "code/gamma"); // a project without a manifest
    for (p, dest) in [(&a, "~/backups/a"), (&b, "~/backups/b")] {
        fs::create_dir_all(p.join("data")).unwrap();
        fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", dest, "strategy = \"age-buckets@1\"")).unwrap();
    }
    let o = fx.reg(None, true, false, false, false);
    assert_eq!(o.exit, exit::OK, "{:?}", o.findings);
    let strip = |rows: Vec<EmmaRow>| -> Vec<EmmaRow> {
        rows.into_iter().map(|r| EmmaRow { first_registered_at: String::new(), updated_at: String::new(), ..r }).collect()
    };
    let before = strip(fx.rows());
    assert_eq!(before.len(), 2);
    fs::remove_dir_all(&fx.state).unwrap();
    assert_eq!(fx.reg(None, true, false, false, false).exit, exit::OK);
    assert_eq!(strip(fx.rows()), before);
}

#[test]
fn a_held_lock_is_exit_5() {
    use fs2::FileExt as _;
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    fs::create_dir_all(&fx.state).unwrap();
    let lock = fs::OpenOptions::new().create(true).truncate(false).write(true).open(fx.state.join(registry::LOCK_NAME)).unwrap();
    lock.lock_exclusive().unwrap();
    let o = fx.reg(Some(&p), false, false, false, false);
    assert_eq!(o.exit, exit::LOCK_BUSY);
    assert_eq!(codes(&o), vec!["E_LOCK_BUSY"]);
}

// ---------------------------------------------------------------------------
// Adoption and plan.
// ---------------------------------------------------------------------------

fn vault_fixture(fx: &Fx, exe: &str, retain: &str) -> (PathBuf, PathBuf) {
    let p = fx.project("vaultproj", "code/Vault");
    fs::write(p.join(".backup-exclude"), "# x\n.stversions\n").unwrap();
    let dest = fx.home.join("backups/vault");
    fs::create_dir_all(&dest).unwrap();
    for d in ["2026-11-24", "2026-11-25", "2026-11-26"] {
        fs::write(dest.join(format!("vault-{d}.tar.gz")), b"x").unwrap();
        fs::write(dest.join(format!("vault-{d}.tar.gz.sha256")), b"y").unwrap();
    }
    let job = crate::jobs::test_job(
        &[
            exe,
            "--source", &p.display().to_string(),
            "--dest-dir", &dest.display().to_string(),
            "--prefix", "vault",
            "--exclude-from", &p.join(".backup-exclude").display().to_string(),
            "--retain", retain,
            "--once",
        ],
        &fx.home,
        &fx.home,
    );
    let mut env = fx.env();
    env.jobs = Some(vec![job]);
    let o = adopt::adopt(true, &mut env);
    assert_eq!(o.exit, exit::OK, "{:?}", o.findings);
    (p, dest)
}

fn env_with_job(fx: &Fx, exe: &str, p: &Path, dest: &Path, retain: &str) -> Env {
    let job = crate::jobs::test_job(
        &[
            exe,
            "--source", &p.display().to_string(),
            "--dest-dir", &dest.display().to_string(),
            "--prefix", "vault",
            "--exclude-from", &p.join(".backup-exclude").display().to_string(),
            "--retain", retain,
            "--once",
        ],
        &fx.home,
        &fx.home,
    );
    let mut env = fx.env();
    env.jobs = Some(vec![job]);
    env
}

#[test]
fn adopt_writes_an_exact_manifest_and_refuses_to_edit_an_existing_one() {
    let fx = Fx::new();
    let (p, _) = vault_fixture(&fx, "/usr/bin/forever-ago", "3");
    let text = fs::read_to_string(p.join(MANIFEST_NAME)).unwrap();
    for line in [
        "id          = \"vault\"",
        "mode        = \"adopted\"",
        "path = \".\"",
        "exclude_from = \".backup-exclude\"",
        "strategy = \"tar-gz-dir@1\"",
        "dir      = \"~/backups/vault\"",
        "prefix   = \"vault\"",
        "strategy = \"legacy-count@1\"\nkeep     = 3",
    ] {
        assert!(text.contains(line), "missing {line:?} in\n{text}");
    }
    // A cron job has no Candybar unit to confirm, so no ref is written.
    assert!(!text.contains("[emmas.candybar]"));
    let mut env = fx.env();
    env.jobs = env_with_job(&fx, "/usr/bin/forever-ago", &p, &fx.home.join("backups/vault"), "3").jobs;
    let again = adopt::adopt(true, &mut env);
    assert_eq!(again.exit, exit::REFUSED);
    assert!(again.findings.iter().any(|f| f.code == "E_MANIFEST_EXISTS"));
    assert_eq!(fs::read_to_string(p.join(MANIFEST_NAME)).unwrap(), text, "never edited");
    // voxxy-style prefixes drop the project id.
    assert_eq!(adopt::emma_id_for("voxxy-voices", "voxxy"), "voices");
    assert_eq!(adopt::emma_id_for("vault", "delodocs"), "vault");
}

#[test]
fn adopted_emma_registers_against_its_live_job_and_plans_read_only() {
    let fx = Fx::new();
    let (p, dest) = vault_fixture(&fx, "/home/x/code/forever-ago/target/release/forever-ago", "3");
    let mut env = env_with_job(&fx, "/home/x/code/forever-ago/target/release/forever-ago", &p, &dest, "3");
    let o = register(&RegisterOpts { path: Some(p.clone()), all: false, dry_run: false, update: false, prune: false }, &mut env, &fx.home);
    assert_eq!(o.exit, exit::OK, "{:?}", o.findings);

    let db = fx.state.join(registry::DB_NAME);
    let before = (fs::metadata(&db).unwrap().modified().unwrap(), fs::read(&db).unwrap());
    let (code, e, human, findings) = plan_one(&mut env, "vault", None, false);
    assert_eq!(code, exit::OK);
    assert_eq!(e["legacy_job"]["comparison"], "clean");
    assert_eq!(e["inventory"]["snapshots"], 3);
    assert_eq!(e["coverage"], "local-only");
    assert_eq!(e["independence"], "same-device");
    assert_eq!(e["candybar"]["status"], "unscheduled");
    assert!(findings.iter().any(|f| f.code == "W_EXEC_IN_BUILD_DIR"), "{findings:?}");
    assert!(human.contains("local-only"));
    let after = (fs::metadata(&db).unwrap().modified().unwrap(), fs::read(&db).unwrap());
    assert_eq!(before, after, "plan never writes the registry");

    // The unit's flags drift from the manifest: plan says so; register warns.
    let mut drifted = env_with_job(&fx, "/usr/bin/forever-ago", &p, &dest, "5");
    let (_, e, _, findings) = plan_one(&mut drifted, "vaultproj/vault", None, false);
    assert_eq!(e["legacy_job"]["comparison"], "drift:legacy-job:retention");
    assert!(findings.iter().any(|f| f.code == "W_LEGACY_DRIFT"));

    // No legacy job at all: an adopted Emma refuses to register.
    let mut none = fx.env();
    let o = register(&RegisterOpts { path: Some(p.clone()), all: false, dry_run: true, update: false, prune: false }, &mut none, &fx.home);
    assert_eq!(o.exit, exit::USAGE);
    assert!(codes(&o).contains(&"E_ADOPT_NO_JOB".to_string()));
}

#[test]
fn candybar_join_reports_ok_disabled_missing_and_runner_mismatch() {
    let fx = Fx::new();
    let mut cache = candybar::Cache::new(fx.bin.join("candybar").display().to_string(), false);
    fx.candybar(
        "forever-ago-vault",
        json!({"id": "x", "name": "forever-ago-vault", "schedule": "0 3 * * *", "timezone": "America/New_York", "enabled": true, "archived": false,
               "runner": {"kind": "systemd", "unit": "forever-ago-vault.service"},
               "invocations": [{"id": "i1", "result": "success", "exitCode": 0, "startedAt": "2026-11-26T08:00:00+00:00"}]}),
    );
    fx.candybar("paused", json!({"enabled": false, "archived": false, "runner": {"kind": "exec"}}));
    let ok = candybar::join(&mut cache, Some("forever-ago-vault"), Some("forever-ago-vault.service"));
    assert_eq!((ok["status"].as_str(), ok["schedule"].as_str(), ok["last_invocation"]["result"].as_str()), (Some("ok"), Some("0 3 * * *"), Some("success")));
    assert_eq!(candybar::join(&mut cache, Some("forever-ago-vault"), Some("other.service"))["status"], "runner-mismatch");
    assert_eq!(candybar::join(&mut cache, Some("paused"), None)["status"], "disabled");
    assert_eq!(candybar::join(&mut cache, Some("ghost"), None)["status"], "missing");
    assert_eq!(candybar::join(&mut cache, None, None)["status"], "unscheduled");
    let mut off = candybar::Cache::new("/nonexistent/candybar".into(), true);
    assert_eq!(candybar::join(&mut off, Some("forever-ago-vault"), None)["status"], "unknown");
}

#[test]
fn coverage_is_never_green_and_a_missing_source_is_broken() {
    assert!(plan::VERDICTS.iter().all(|v| !["ok", "covered", "green", "protected"].contains(v)));
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups/alpha", r#"strategy = "keep-all@1""#)).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    let mut env = fx.env();
    let (_, e, _, _) = plan_one(&mut env, "data", None, true);
    assert_eq!(e["coverage"], "none");
    assert!(e["blockers"].as_array().unwrap().iter().any(|b| b["code"] == "capture-not-implemented"));

    fs::create_dir_all(fx.home.join("backups/alpha")).unwrap();
    fs::write(fx.home.join("backups/alpha/data-2026-11-26.tar.gz"), b"x").unwrap();
    let (_, e, _, _) = plan_one(&mut env, "data", None, true);
    assert_eq!((e["coverage"].as_str(), e["independence"].as_str()), (Some("local-only"), Some("same-device")));

    fs::remove_dir_all(p.join("data")).unwrap();
    let (_, e, _, f) = plan_one(&mut env, "alpha/data", None, true);
    assert_eq!(e["coverage"], "broken:source-missing");
    assert!(f.iter().any(|x| x.code == "E_SOURCE_MISSING"));
    assert!(plan::VERDICTS.contains(&e["coverage"].as_str().unwrap()));
}

#[test]
fn candystore_emmas_plan_as_declared_with_their_blockers() {
    let fx = Fx::new();
    let p = fx.project("candystore", "code/33GOD/candystore");
    fs::create_dir_all(fx.home.join("backups/candystore/event-archive")).unwrap();
    for i in 0..3 {
        fs::write(fx.home.join(format!("backups/candystore/event-archive/events-x-{i}.jsonl.gz")), b"z").unwrap();
    }
    fs::write(p.join(MANIFEST_NAME), CANDYSTORE_MANIFEST).unwrap();
    let o = fx.reg(Some(&p), false, false, false, false);
    assert_eq!(o.exit, exit::OK, "{:?}", o.findings);
    let mut env = fx.env();
    let (_, a, _, f) = plan_one(&mut env, "event-archive", None, true);
    assert_eq!((a["sole_copy"].as_bool(), a["coverage"].as_str(), a["independence"].as_str()), (Some(true), Some("none"), Some("same-device")));
    assert_eq!(a["source"]["summary"]["files"], 3);
    assert!(f.iter().any(|x| x.code == "W_SOLE_COPY_SAME_DEVICE"));
    let (_, x, _, _) = plan_one(&mut env, "candystore/event-export", None, true);
    assert_eq!(x["coverage"], "none");
    let blockers: Vec<&str> = x["blockers"].as_array().unwrap().iter().map(|b| b["code"].as_str().unwrap()).collect();
    assert!(blockers.contains(&"owner-export-verb-missing"), "{blockers:?}");
    assert!(blockers.contains(&"backend-repo-missing"));
    assert_eq!(x["capture"]["argv"][0], "candystore");
    let (code, _, _, _) = projects(&mut env, None, true);
    assert_eq!(code, exit::OK);
}

#[test]
fn plan_on_an_unknown_or_ambiguous_emma_is_a_usage_error() {
    let fx = Fx::new();
    for (id, rel) in [("alpha", "code/alpha"), ("beta", "code/beta")] {
        let p = fx.project(id, rel);
        fs::create_dir_all(p.join("data")).unwrap();
        fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", &format!("~/backups/{id}"), r#"strategy = "keep-all@1""#)).unwrap();
        fx.reg(Some(&p), false, false, false, false);
    }
    let mut env = fx.env();
    assert_eq!(plan_one(&mut env, "data", None, true).0, exit::USAGE);
    assert_eq!(plan_one(&mut env, "nope", None, true).0, exit::USAGE);
    assert_eq!(plan_one(&mut env, "beta/data", None, true).0, exit::OK);
}

// ---------------------------------------------------------------------------
// age-buckets@1 (5.4) and the other retention plans.
// ---------------------------------------------------------------------------

fn as_of() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 26, 12, 0, 0).unwrap()
}

fn bucket_plan(snaps: &[Snap], at: DateTime<Utc>) -> retention::Plan {
    retention::plan("age-buckets@1", &Json::Null, snaps, at).unwrap()
}

fn kept_ids(p: &retention::Plan) -> Vec<String> {
    let mut v: Vec<String> = p.partitions.iter().flat_map(|x| x.decisions.iter().filter(|d| d.keep).map(|d| d.id.clone())).collect();
    v.sort();
    v
}

#[test]
fn golden_table_from_the_design() {
    // (days, kept, per band, longest gap in whole days): EMMA-DESIGN 5.4.
    for (n, kept, bands, gap) in [
        (30u32, 20usize, [14usize, 6, 0, 0], 3i64),
        (90, 40, [14, 26, 0, 0], 3),
        (180, 54, [14, 26, 14, 0], 7),
        (365, 80, [14, 26, 40, 0], 7),
        (400, 82, [14, 26, 40, 2], 26),
        (730, 93, [14, 26, 40, 13], 31),
    ] {
        let snaps = retention::simulate_daily(n, as_of());
        assert_eq!(snaps.len(), n as usize);
        let p = bucket_plan(&snaps, as_of());
        let part = &p.partitions[0];
        assert!(!part.frozen);
        assert_eq!(part.decisions.iter().filter(|d| d.keep).count(), kept, "{n} days");
        let got: Vec<usize> = part.bands.iter().map(|b| b.kept).collect();
        assert_eq!(got, bands.to_vec(), "{n} days");
        assert_eq!(part.max_gap_s.map(retention::days), Some(gap), "{n} days");
    }
}

#[test]
fn plan_digest_ignores_input_order() {
    let mut snaps = retention::simulate_daily(400, as_of());
    let a = bucket_plan(&snaps, as_of());
    snaps.reverse();
    let mid = snaps.len() / 2;
    snaps.swap(3, mid);
    let b = bucket_plan(&snaps, as_of());
    assert_eq!(a.digest, b.digest);
    assert_eq!(kept_ids(&a), kept_ids(&b));
    // A different as_of is a different plan.
    assert_ne!(a.digest, bucket_plan(&snaps, as_of() + chrono::Duration::hours(1)).digest);
}

fn at_age(id: &str, age: chrono::Duration) -> Snap {
    Snap::new(id, as_of() - age)
}

#[test]
fn band_edges_are_half_open() {
    let d = chrono::Duration::days;
    let s = chrono::Duration::seconds;
    let snaps = vec![
        at_age("a-dense-edge", d(14) - s(1)),
        at_age("b-exactly-14d", d(14)),
        at_age("c-just-under-90d", d(90) - s(1)),
        at_age("d-exactly-90d", d(90)),
        at_age("e-just-under-365d", d(365) - s(1)),
        at_age("f-exactly-365d", d(365)),
        at_age("g-newest", s(60)),
    ];
    let p = bucket_plan(&snaps, as_of());
    let band = |id: &str| p.partitions[0].decisions.iter().find(|x| x.id == id).unwrap().band;
    assert_eq!(band("a-dense-edge"), Some(0));
    assert_eq!(band("b-exactly-14d"), Some(1));
    assert_eq!(band("c-just-under-90d"), Some(1));
    assert_eq!(band("d-exactly-90d"), Some(2));
    assert_eq!(band("e-just-under-365d"), Some(2));
    assert_eq!(band("f-exactly-365d"), Some(3));
}

#[test]
fn future_dated_snapshots_are_kept_for_review() {
    let mut snaps = retention::simulate_daily(30, as_of());
    snaps.push(Snap::new("from-the-future", as_of() + chrono::Duration::days(3)));
    let p = bucket_plan(&snaps, as_of());
    let d = p.partitions[0].decisions.iter().find(|d| d.id == "from-the-future").unwrap();
    assert!(d.keep);
    assert_eq!(d.reasons, vec!["review-future-dated"]);
    // The pin goes to the latest snapshot that is not in the future.
    let latest = p.partitions[0].decisions.iter().find(|d| d.reasons.contains(&"pin-latest".to_string())).unwrap();
    assert_eq!(latest.id, "whatif-2026-11-26T0600Z");
}

#[test]
fn an_empty_dense_band_freezes_the_plan() {
    let snaps: Vec<Snap> = retention::simulate_daily(200, as_of() - chrono::Duration::days(30));
    let p = bucket_plan(&snaps, as_of());
    assert!(p.partitions[0].frozen);
    assert_eq!(kept_ids(&p).len(), 200, "a stalled chain keeps everything");
    assert!(p.notes.iter().any(|n| n.starts_with("frozen")));
    // Empty inventory: frozen, nothing invented.
    let empty = bucket_plan(&[], as_of());
    assert!(empty.partitions.is_empty());
}

#[test]
fn ties_resolve_by_the_greatest_id() {
    let t = as_of() - chrono::Duration::days(40);
    let snaps = vec![Snap::new("snap-a", t), Snap::new("snap-c", t), Snap::new("snap-b", t), Snap::new("recent", as_of() - chrono::Duration::hours(1))];
    let p = bucket_plan(&snaps, as_of());
    let get = |id: &str| p.partitions[0].decisions.iter().find(|d| d.id == id).unwrap().clone();
    assert!(get("snap-c").keep);
    assert_eq!(get("snap-a").reasons, vec!["superseded-by:snap-c"]);
    assert_eq!(get("snap-b").reasons, vec!["superseded-by:snap-c"]);
}

#[test]
fn holds_override_every_band_and_incomplete_snapshots_are_untouched() {
    let mut snaps = retention::simulate_daily(400, as_of());
    let p0 = bucket_plan(&snaps, as_of());
    let held_id = p0.partitions[0]
        .decisions
        .iter()
        .find(|d| !d.keep && (150..300).contains(&(as_of() - d.captured_at).num_days()))
        .expect("fixture: some mid-band snapshot is normally forgotten")
        .id
        .clone();
    let held = snaps.iter().position(|s| s.id == held_id).unwrap();
    snaps[held].hold = Some("legal".into());
    let mut broken = Snap::new("half-written", as_of() - chrono::Duration::days(50));
    broken.status = SnapStatus::Incomplete;
    snaps.push(broken);
    let mut lossy = Snap::new("prompts-only", as_of() - chrono::Duration::days(50));
    lossy.fidelity = "conversation-prompts@1".into();
    snaps.push(lossy);
    let p = bucket_plan(&snaps, as_of());
    let d = p.partitions[0].decisions.iter().find(|d| d.id == held_id).unwrap();
    assert!(d.keep);
    assert_eq!(d.reasons, vec!["hold:legal"]);
    assert!(p.partitions[0].decisions.iter().all(|d| d.id != "half-written" && d.id != "prompts-only"));
    assert_eq!(p.untouched, vec![("half-written".to_string(), "status:incomplete".to_string()), ("prompts-only".to_string(), "fidelity:conversation-prompts@1".to_string())]);
}

#[test]
fn generations_are_separate_partitions() {
    let mut snaps = retention::simulate_daily(60, as_of());
    for s in snaps.iter_mut().skip(30) {
        s.generation = 2;
    }
    let p = bucket_plan(&snaps, as_of());
    assert_eq!(p.partitions.len(), 2);
    // Generation 2 holds only snapshots 30..60 days old: its dense band is empty, so it freezes.
    assert!(p.partitions.iter().find(|x| x.generation == 2).unwrap().frozen);
}

#[test]
fn keep_all_observe_only_and_legacy_plans() {
    let snaps = retention::simulate_daily(10, as_of());
    for s in ["keep-all@1", "observe-only@1"] {
        let p = retention::plan(s, &Json::Null, &snaps, as_of()).unwrap();
        assert_eq!(kept_ids(&p).len(), 10);
    }
    let p = retention::plan("legacy-count@1", &json!({"keep": 3}), &snaps, as_of()).unwrap();
    assert_eq!(kept_ids(&p), vec!["whatif-2026-11-24T0600Z", "whatif-2026-11-25T0600Z", "whatif-2026-11-26T0600Z"]);
    assert_eq!(p.executed_by, "legacy job");
    assert!(p.notes.iter().any(|n| n.contains("Emma will not act")));
    let gfs = retention::plan("legacy-gfs@1", &json!({"daily": 2, "weekly": 0, "monthly": 0}), &snaps, as_of()).unwrap();
    assert_eq!(kept_ids(&gfs).len(), 2);
    assert!(retention::plan("legacy-count@1", &json!({"keep": 0}), &snaps, as_of()).is_err());
    assert!(retention::plan("legacy-gfs@1", &json!({"daily": 0, "weekly": 0, "monthly": 0}), &snaps, as_of()).is_err());
    assert!(retention::plan("age-buckets@2", &Json::Null, &snaps, as_of()).is_err());
}

#[test]
fn retention_plan_command_previews_and_refuses() {
    let fx = Fx::new();
    let p = fx.project("candystore", "code/33GOD/candystore");
    fs::create_dir_all(fx.home.join("backups/candystore/event-archive")).unwrap();
    fs::write(p.join(MANIFEST_NAME), CANDYSTORE_MANIFEST).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    let env = fx.env();
    let args = |emma: &str, sim: Option<u32>| RetentionPlanArgs { emma: emma.into(), as_of: Some("2026-11-26T12:00:00Z".into()), simulate_daily: sim, inventory_json: None, json: true };
    let (code, data, _, _) = retention_plan(&env, &args("event-export", Some(400)));
    assert_eq!(code, exit::OK);
    assert_eq!((data["what_if"].as_bool(), data["apply_authority"].as_bool()), (Some(true), Some(false)));
    assert_eq!(data["partitions"][0]["kept"], 82);
    // Live inventory of a declared restic Emma: nothing exists yet.
    let (code, data, _, f) = retention_plan(&env, &args("event-export", None));
    assert_eq!(code, exit::OK);
    assert_eq!(data["inventory"]["snapshots"], 0);
    assert!(f.iter().any(|x| x.code == "I_NO_INVENTORY"));
    // The sole-copy archive plans to keep everything.
    let (code, data, _, _) = retention_plan(&env, &args("event-archive", Some(30)));
    assert_eq!((code, data["partitions"][0]["kept"].as_u64()), (exit::OK, Some(30)));
    // Supplied inventory.
    let inv = fx.home.join("inv.json");
    fs::write(&inv, r#"[{"id":"a","captured_at":"2026-11-25T00:00:00Z"},{"id":"b","captured_at":"2026-09-01T00:00:00Z","status":"failed"}]"#).unwrap();
    let mut a = args("event-export", None);
    a.inventory_json = Some(inv);
    let (code, data, _, _) = retention_plan(&env, &a);
    assert_eq!((code, data["untouched"][0]["reason"].as_str()), (exit::OK, Some("status:failed")));
}

// ---------------------------------------------------------------------------
// CLI surface and non-regression.
// ---------------------------------------------------------------------------

fn parse(args: &[&str]) -> Result<crate::Cli, clap::Error> {
    use clap::Parser as _;
    crate::Cli::try_parse_from(std::iter::once("forever-ago").chain(args.iter().copied()))
}

#[test]
fn retention_apply_and_unknown_subcommands_are_usage_errors() {
    for bad in [&["retention", "apply", "x"][..], &["retention"], &["frobnicate"], &["plan"]] {
        let err = parse(bad).unwrap_err();
        assert_eq!(err.exit_code(), 2, "{bad:?}");
    }
    assert!(parse(&["retention", "plan", "x", "--simulate-daily", "30"]).is_ok());
    assert!(parse(&["register", "--all", "--dry-run", "--json"]).is_ok());
    assert!(parse(&["projects", "--offline"]).is_ok());
    assert!(parse(&["adopt", "--write"]).is_ok());
    // The legacy forms the two live units use still parse exactly as before.
    let vault = parse(&["--source", "/a", "--dest-dir", "/b", "--prefix", "vault", "--exclude-from", "/a/.x", "--retain", "3", "--once"]).unwrap();
    assert!(vault.command.is_none() && vault.once && vault.retain == 3);
    let voices = parse(&["--source", "/v", "--dest-dir", "/b", "--prefix", "voxxy-voices", "--exclude", "*.aup4", "--exclude", "*.aup4-wal", "--retain", "1", "--once"]).unwrap();
    assert_eq!(voices.excludes, vec!["*.aup4", "*.aup4-wal"]);
    // A subcommand still cannot be mixed with backup flags.
    assert!(parse(&["--prefix", "v", "plan", "x"]).is_err());
}

#[test]
fn nothing_in_emma_can_reach_the_legacy_backup_or_prune_path() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/emma");
    let needles = [concat!("run_", "backup"), concat!("prune_old_", "backups"), concat!("write_tar_", "gz")];
    let mut seen = HashMap::new();
    for e in fs::read_dir(&dir).unwrap() {
        let p = e.unwrap().path();
        let text = fs::read_to_string(&p).unwrap();
        for n in needles {
            assert!(!text.contains(n), "{} references {n}", p.display());
        }
        seen.insert(p, ());
    }
    assert!(seen.len() >= 10, "scanned every module");
}

#[test]
fn json_envelope_shape() {
    let env = envelope::envelope("plan", 6, json!({"x": 1}), &[envelope::Finding::of("W_X", "m")]);
    assert_eq!(env["schema"], 2);
    assert_eq!(env["ok"], false);
    assert_eq!(env["exit"], 6);
    assert_eq!(env["findings"][0]["severity"], "warning");
}

// ---------------------------------------------------------------------------
// Adversarial review (2026-11-26): each test below failed against 439822d.
// ---------------------------------------------------------------------------

#[test]
fn the_exact_live_unit_argv_parses_as_before() {
    // Verbatim ExecStart argv of the two Candybar-started units
    // (~/docker/systemd/user/forever-ago-{vault,voxxy-voices}.service).
    use clap::Parser as _;
    let vault = crate::Cli::try_parse_from([
        "/home/delorenj/code/forever-ago/target/release/forever-ago",
        "--source", "/home/delorenj/code/DeLoDocs",
        "--dest-dir", "/home/delorenj/backups/delodocs",
        "--prefix", "vault",
        "--exclude-from", "/home/delorenj/code/DeLoDocs/.backup-exclude",
        "--retain", "3",
        "--once",
    ])
    .unwrap();
    assert!(vault.command.is_none());
    assert_eq!(vault.source, PathBuf::from("/home/delorenj/code/DeLoDocs"));
    assert_eq!(vault.dest_dir.as_deref(), Some(Path::new("/home/delorenj/backups/delodocs")));
    assert_eq!(vault.prefix.as_deref(), Some("vault"));
    assert_eq!(vault.exclude_from.as_deref(), Some(Path::new("/home/delorenj/code/DeLoDocs/.backup-exclude")));
    assert!(vault.excludes.is_empty());
    assert!(vault.once && !vault.run_now);
    assert_eq!(crate::retention_policy(&vault).unwrap(), crate::RetentionPolicy::Count(3));

    let voices = crate::Cli::try_parse_from([
        "/home/delorenj/.cargo/bin/forever-ago",
        "--source", "/home/delorenj/code/voxxy/voices",
        "--dest-dir", "/home/delorenj/backups",
        "--prefix", "voxxy-voices",
        "--exclude", "*.aup4",
        "--exclude", "*.aup4-wal",
        "--exclude", "*.aup4-shm",
        "--retain", "1",
        "--once",
    ])
    .unwrap();
    assert!(voices.command.is_none());
    assert_eq!(voices.excludes, vec!["*.aup4", "*.aup4-wal", "*.aup4-shm"]);
    assert!(voices.exclude_from.is_none());
    assert!(voices.once);
    assert_eq!(crate::retention_policy(&voices).unwrap(), crate::RetentionPolicy::Count(1));
}

#[test]
fn a_destination_spelled_through_a_missing_dir_and_dotdot_is_still_inside_the_source() {
    // `missing/..` is the manifest dir once `missing` is created, so this
    // destination is proj/data/bk: inside the source. 439822d dropped the
    // `..` and saw proj/missing/data/bk.
    let (_t, root) = scratch();
    let p = root.join("proj");
    let keep = r#"strategy = "keep-all@1""#;
    assert_eq!(parse_in(&p, &root, &files_manifest("data", "data", "missing/../data/bk", keep)).1, vec!["E_DEST_INSIDE_SOURCE"]);
    assert_eq!(parse_in(&p, &root, &files_manifest("data", "data", "a/b/../../data", keep)).1, vec!["E_DEST_INSIDE_SOURCE"]);
    let (defs, codes) = parse_in(&p, &root, &files_manifest("data", "data", "missing/../../b", keep));
    assert!(codes.is_empty(), "{codes:?}");
    assert_eq!(defs[0].dest_canonical.as_deref(), Some(root.join("b").as_path()));
    // Through a symlink that does exist, then a missing tail: still resolved.
    symlink(p.join("data"), p.join("link")).unwrap();
    assert_eq!(parse_in(&p, &root, &files_manifest("data", "data", "link/new/../bk", keep)).1, vec!["E_DEST_INSIDE_SOURCE"]);
    assert_eq!(manifest::canonical_lenient(&p.join("missing/../data/bk")), p.join("data/bk"));
}

#[test]
fn duplicate_snapshot_ids_are_refused_not_collapsed() {
    let t = as_of() - chrono::Duration::days(40);
    let snaps = vec![Snap::new("dup", t), Snap::new("dup", t - chrono::Duration::days(1)), Snap::new("recent", as_of() - chrono::Duration::hours(1))];
    for s in ["age-buckets@1", "keep-all@1", "legacy-count@1"] {
        let err = retention::plan(s, &json!({"keep": 1}), &snaps, as_of()).unwrap_err();
        assert!(err.contains("dup"), "{s}: {err}");
    }
    let fx = Fx::new();
    let p = fx.project("candystore", "code/33GOD/candystore");
    fs::create_dir_all(fx.home.join("backups/candystore/event-archive")).unwrap();
    fs::write(p.join(MANIFEST_NAME), CANDYSTORE_MANIFEST).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    let inv = fx.home.join("inv.json");
    fs::write(&inv, r#"[{"id":"a","captured_at":"2026-11-25T00:00:00Z"},{"id":"a","captured_at":"2026-09-01T00:00:00Z"}]"#).unwrap();
    let a = RetentionPlanArgs { emma: "event-export".into(), as_of: Some("2026-11-26T12:00:00Z".into()), simulate_daily: None, inventory_json: Some(inv), json: true };
    let (code, _, _, f) = retention_plan(&fx.env(), &a);
    assert_eq!(code, exit::USAGE, "{f:?}");
}

#[test]
fn simulate_daily_is_bounded() {
    let fx = Fx::new();
    let p = fx.project("candystore", "code/33GOD/candystore");
    fs::create_dir_all(fx.home.join("backups/candystore/event-archive")).unwrap();
    fs::write(p.join(MANIFEST_NAME), CANDYSTORE_MANIFEST).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    let args = |n: u32| RetentionPlanArgs { emma: "event-export".into(), as_of: Some("2026-11-26T12:00:00Z".into()), simulate_daily: Some(n), inventory_json: None, json: true };
    let started = std::time::Instant::now();
    let (code, _, _, f) = retention_plan(&fx.env(), &args(u32::MAX));
    assert_eq!(code, exit::USAGE, "{f:?}");
    assert!(started.elapsed() < std::time::Duration::from_secs(2), "refused before allocating");
    assert_eq!(retention_plan(&fx.env(), &args(retention::MAX_SIMULATE_DAYS)).0, exit::OK);
}

/// A files Emma in project `id`, then `pj rename` it to `new_id`.
fn renamed_fixture(fx: &Fx, id: &str, new_id: &str) -> PathBuf {
    let p = fx.project(id, &format!("code/{id}"));
    fs::create_dir_all(p.join("data")).unwrap();
    fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    assert_eq!(fx.reg(Some(&p), false, false, false, false).exit, exit::OK);
    fs::write(p.join(".fakepj"), new_id).unwrap();
    p
}

#[test]
fn a_pj_rename_is_identity_changed_not_a_destination_collision() {
    let fx = Fx::new();
    let p = renamed_fixture(&fx, "alpha", "alpha2");
    let o = fx.reg(Some(&p), false, false, false, false);
    assert_eq!(o.exit, exit::DRIFT, "{:?}", o.findings);
    assert_eq!(states(&o), vec!["identity-changed"]);
    let u = fx.reg(Some(&p), false, false, true, false);
    assert_eq!(u.exit, exit::OK, "{:?}", u.findings);
    let rows = fx.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].project_key.as_str(), rows[0].generation, rows[0].sync_state.as_str()), ("alpha2", 1, "ok"));
    assert_eq!(fx.reg(Some(&p), false, false, false, false).exit, exit::OK);
}

#[test]
fn an_unapplied_identity_change_is_still_reported_on_the_next_run() {
    let fx = Fx::new();
    let p = renamed_fixture(&fx, "alpha", "alpha2");
    let before = fx.rows();
    for _ in 0..2 {
        let o = fx.reg(Some(&p), false, false, false, false);
        assert_eq!(o.exit, exit::DRIFT, "{:?}", o.findings);
        assert_eq!(states(&o), vec!["identity-changed"]);
    }
    assert_eq!(fx.rows(), before, "observation alone never rekeys or duplicates a row");
    let reg = Registry::open_read(&fx.state).unwrap().unwrap();
    assert_eq!(reg.manifests().unwrap()[0].project_key, "alpha", "the manifest stays bound to its registered key");
}

#[test]
fn register_all_after_a_move_never_orphans_or_retires_the_moved_emma() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    assert_eq!(fx.reg(None, true, false, false, false).exit, exit::OK);
    let moved = fx.home.join("code/alpha-moved");
    fs::rename(&p, &moved).unwrap();
    fx.projects.borrow_mut()[0].1 = moved.clone();
    fx.write_projects();

    // Observed, not applied: the row keeps its state (and --prune cannot retire it).
    let o = fx.reg(None, true, false, false, true);
    assert_eq!(o.exit, exit::DRIFT, "{:?}", o.findings);
    assert_eq!(states(&o), vec!["moved"]);
    assert_eq!(fx.rows()[0].sync_state, "ok", "a move is not an orphan");

    let u = fx.reg(None, true, false, true, false);
    assert_eq!(u.exit, exit::OK, "{:?}", u.findings);
    let rows = fx.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].sync_state, "ok");
    assert!(rows[0].manifest_path.starts_with(&moved.display().to_string()));
    let reg = Registry::open_read(&fx.state).unwrap().unwrap();
    let old = reg.manifests().unwrap().into_iter().find(|m| m.manifest_path.contains("/alpha/")).unwrap();
    assert_eq!(old.state, "moved");
}

#[test]
fn an_unreadable_registry_fails_closed_instead_of_reading_as_empty() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    fs::write(p.join(MANIFEST_NAME), files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    fs::create_dir_all(&fx.state).unwrap();
    let c = rusqlite::Connection::open(fx.state.join(registry::DB_NAME)).unwrap();
    c.execute_batch("PRAGMA user_version = 1;").unwrap();
    drop(c);
    for dry in [true, false] {
        let o = fx.reg(Some(&p), false, dry, false, false);
        assert_eq!(o.exit, exit::FAILURE, "dry_run={dry}: {:?}", o.findings);
        assert!(codes(&o).contains(&"E_REGISTRY".to_string()), "{:?}", codes(&o));
    }
}

#[test]
fn register_takes_only_a_file_named_emma_toml() {
    let fx = Fx::new();
    let p = fx.project("alpha", "code/alpha");
    fs::create_dir_all(p.join("data")).unwrap();
    let other = p.join("example.emma.toml");
    fs::write(&other, files_manifest("data", "data", "~/backups", r#"strategy = "keep-all@1""#)).unwrap();
    let o = fx.reg(Some(&other), false, false, false, false);
    assert_eq!(o.exit, exit::USAGE, "{:?}", o.findings);
    assert_eq!(codes(&o), vec!["E_NO_MANIFEST"]);
    assert!(fx.rows().is_empty());
}

#[test]
fn an_unbound_project_label_resolves_even_though_its_key_has_slashes() {
    // Unbound keys are `path:/abs/root`; the label printed for an ambiguous
    // id must itself be accepted back as `project/emma`.
    let fx = Fx::new();
    let mut labels = Vec::new();
    for name in ["loose-a", "loose-b"] {
        let d = fx.home.join(name);
        fs::create_dir_all(d.join("data")).unwrap();
        fs::create_dir_all(d.join(".git")).unwrap();
        fs::write(d.join(MANIFEST_NAME), files_manifest("data", "data", &format!("~/backups/{name}"), r#"strategy = "keep-all@1""#)).unwrap();
        assert_eq!(fx.reg(Some(&d), false, false, false, false).exit, exit::OK);
        labels.push(format!("path:{}/data", d.display()));
    }
    let mut env = fx.env();
    let (code, _, _, f) = plan_one(&mut env, "data", None, true);
    assert_eq!(code, exit::USAGE);
    assert!(f[0].message.contains(&labels[0]), "{f:?}");
    for l in &labels {
        let (code, e, _, f) = plan_one(&mut env, l, None, true);
        assert_eq!(code, exit::OK, "{l}: {f:?}");
        assert_eq!(e["label"].as_str(), Some(l.as_str()));
    }
}

#[test]
fn a_supplied_inventory_is_a_closed_schema_so_a_hold_is_never_dropped() {
    let fx = Fx::new();
    let p = fx.project("candystore", "code/33GOD/candystore");
    fs::create_dir_all(fx.home.join("backups/candystore/event-archive")).unwrap();
    fs::write(p.join(MANIFEST_NAME), CANDYSTORE_MANIFEST).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    let inv = fx.home.join("inv.json");
    let run = |body: &str| {
        fs::write(&inv, body).unwrap();
        let a = RetentionPlanArgs { emma: "event-export".into(), as_of: Some("2026-11-26T12:00:00Z".into()), simulate_daily: None, inventory_json: Some(inv.clone()), json: true };
        retention_plan(&fx.env(), &a)
    };
    let ok = r#"{"id":"a","captured_at":"2026-11-25T00:00:00Z"}"#;
    assert_eq!(run(&format!("[{ok}]")).0, exit::OK);
    for bad in [
        r#"{"id":"b","captured_at":"2026-01-01T00:00:00Z","hold":true}"#,
        r#"{"id":"b","captured_at":"2026-01-01T00:00:00Z","hold_":"legal"}"#,
        r#"{"id":"b","captured_at":"2026-01-01T00:00:00Z","status":1}"#,
        r#"{"id":"b","captured_at":"2026-01-01T00:00:00Z","generation":"2"}"#,
        r#"{"id":"b","captured_at":"2026-01-01T00:00:00Z","fidelity":7}"#,
        r#""b""#,
    ] {
        let (code, _, _, f) = run(&format!("[{ok},{bad}]"));
        assert_eq!(code, exit::USAGE, "{bad}: {f:?}");
    }
    // `hold: null` means no hold.
    assert_eq!(run(&format!(r#"[{ok},{{"id":"c","captured_at":"2026-01-01T00:00:00Z","hold":null}}]"#)).0, exit::OK);
}

#[test]
fn the_recorded_as_of_reproduces_the_plan() {
    let fx = Fx::new();
    let p = fx.project("candystore", "code/33GOD/candystore");
    fs::create_dir_all(fx.home.join("backups/candystore/event-archive")).unwrap();
    fs::write(p.join(MANIFEST_NAME), CANDYSTORE_MANIFEST).unwrap();
    fx.reg(Some(&p), false, false, false, false);
    let args = |at: Option<&str>| RetentionPlanArgs { emma: "event-export".into(), as_of: at.map(str::to_string), simulate_daily: Some(30), inventory_json: None, json: true };
    let (_, d, _, _) = retention_plan(&fx.env(), &args(Some("2026-11-26T12:00:00.750Z")));
    assert_eq!(d["as_of"].as_str(), Some("2026-11-26T12:00:00.750Z"), "a supplied as_of is recorded exactly");
    let (_, d, _, _) = retention_plan(&fx.env(), &args(None));
    let recorded = d["as_of"].as_str().unwrap().to_string();
    let (_, again, _, _) = retention_plan(&fx.env(), &args(Some(&recorded)));
    assert_eq!(again["plan_digest"], d["plan_digest"]);
    assert_eq!(again["as_of"], d["as_of"]);
}
