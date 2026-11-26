# Emma on Forever Ago: design (slice 1 build spec)

**Date:** 2026-11-26. **Status:** slice 1 built in forever-ago 0.4.0 (see [emma.md](emma.md)); the text below is the design it was built from. No product code, capture, deletion, clock change or Candybar/ledger edit accompanies this document.
**Supersedes and extends:** [INFR-170 research](../../infra/_bmad-output/planning-artifacts/emma-and-growth/research/forever-ago-design.md) (2026-11-24). Where the two disagree, this one wins; section 1 lists every change and why.

> **Belsky level (`belsky-doctrine`).** Jarad asked for a rolling, tiered backup of CandyStore's event history. This builds it for **every project-owned file, folder or consistent export declared by a project's `.emma.toml`**, and CandyStore is the first project to declare one. It is not a backup platform and not an all-host migration. After this ships, the next project to want a backup writes one `.emma.toml` and nothing else.
>
> **What Jarad gets:** a `.emma.toml` for CandyStore, a registry that shows it beside the two existing Forever Ago jobs, a read-only plan, and a deterministic preview of exactly which restore points the tiered rule would keep and drop. **What the whole level gets:** manifest, registry, `register`, `projects`, `plan`, `retention plan`, and `adopt` for every project.

Everything below marked *verified* was read from source or a live read-only call on 2026-11-26. Nothing here ran a backup, a prune, a restore, `forever-ago --once`, or a write to Candybar, pjangler or CandyStore.

## 0. Decisions at a glance

1. **Emma consumes Candybar for every execution concern** (when, how to run, timeouts, overlap, metering, failure alerts, receipts). It adds no scheduler, no runner, no daemon. The join is a Candybar **name**.
2. **Emma consumes pjangler for project identity** via `pj info --json`. It mints no identity of its own (INFR-170's "persist a UUID in the manifest" is dropped).
3. **Emma owns one thing nobody else holds: backup evidence** (what was captured, where, how big, verified how far, restored how far). That lives in a local SQLite registry that is derived state, never an editable config service.
4. **Nothing is extracted into a new shared code atom.** Candybar (Python), pjangler (TypeScript) and Forever Ago (Rust) cannot share code. What *is* shared is a convention (declare in the project, derive a registry, report drift), and it is recorded as a catalog candidate in `~/code/33GOD/features.toml`, not a platform.
5. **CandyStore's event history becomes two Emmas, not one**, because the code shows two different kinds of data: the live `events` table, which is append-only and thin-able as restore points, and the prune archives, which are the **only copy** of events already deleted. The second must never be thinned.
6. **Slice 1 is observe-only.** It parses, registers, lists and plans. Retention `apply`, event deletion, capture runs and clock changes are not in it, and are `needs_jarad` decisions (section 9).

## 1. What changed from INFR-170, and why

| INFR-170 said | This design says | Evidence that forced it |
|---|---|---|
| Use `.project.json` identity when present, else mint and persist a project UUID in the manifest | Identity is pjangler's `project_id`. A directory pjangler does not know is `identity = unbound`, keyed by canonical path, with a warning. Emma never mints an id | `features.toml:43` makes `project.identity` a pjangler atom. A manifest-held UUID is a second identity store. `pj info --json` already answers (0.17 s) |
| "Candybar owns when" | Candybar also owns **how it runs and the receipt** (exec runner, AD-8 protocol, invocation row) | `candybar/src/candybar/runners.py:111`, `executor/runner.py:165`, `registry/models.py` (`Invocation`) |
| CandyStore history is one source with "rolling tiered retention of full exports" | Two Emmas. The prune archives are sole copies and get `keep-all@1`. Only the live-DB export gets `age-buckets@1` | `candystore/retention.py:144` deletes rows right after the archive fsync. Events are insert-only (no `UPDATE events` anywhere in `candystore/` or `migrations/`) |
| Reuse restic's native retention where possible | Unchanged, but note restic has no 3-day tier. `age-buckets@1` is a small pure selector that hands snapshot ids to the backend after a future approved apply | INFR-170 section 4 stands |
| `forever-ago plan <emma-id>` | `plan <emma>` is read-only and **never writes the registry**. Only `register` writes | Keeps "plan has zero source, destination and registry writes" testable |
| Capacity: 417.5 GiB free, 89% used | **324 GiB free, 92% used** at 2026-11-26 (`df -B1 /`: 348,219,736,064 free of 3,936,290,357,248) | The same disk holds the database, every archive and every local backup. It is shrinking |
| Adoption without clock change | Same, now concrete: both existing Candybars use runner kind `systemd` and stay exactly as they are | Live `GET /candybars/{id}/runner` for both |

## 2. The Candybar-pattern decision table

I read Candybar's source rather than its README: `project.py` (manifest discovery and project binding), `registry/manifest.py` and `runners.py` (definition and runner validation), `executor/runner.py` and `executor/worker.py` (the run wrapper and its protocol), `registry/models.py` (the registry), `api/app.py` (the :8700 API), `cli/control.py` (`register`, `ls`, `show`, `run`), `sdks/rust/` (the Rust protocol crate), plus Holocene's `apps/api/src/candybar.ts`, `wax.ts` and `apps/web/app/hub/navigation-model.ts`, pjangler's `pj info`, and `features.toml`.

Legend: **CONSUME** = call the existing owner through its existing interface, write no copy. **EMMA** = genuinely Emma's. **CONVENTION** = same rule or vocabulary, independently implemented because the languages differ. No row is "extract into a new shared library".

| # | Piece | What exists (verified) | Decision | Interface Emma uses | Owner |
|---|---|---|---|---|---|
| 1 | **When** (cron, timezone, DST, misfire, skip) | Candybar dispatcher; `CandybarManifest.schedule`, `timezone` (`registry/manifest.py:37`) | **CONSUME** | A Candybar **name** in `[emmas.candybar] ref`. `.emma.toml` rejects `schedule`, `cron`, `timer`, `interval` outright with a pointer to Candybar | candybar (`features.toml:45`, atom `clock`) |
| 2 | **Execution wrapper** (spawn, cgroup scope, timeout, overlap skip, metering, `on_failure`) | `executor/runner.py` + `worker.py`; runner kinds `exec`, `systemd`, `hermes`, `n8n` (`runners.py:111-253`) | **CONSUME** | Future `forever-ago run` is a plain `exec` runner: `argv = ["forever-ago","run","<emma>","--once"]`, `workdir` = project root (Candybar defaults it there). Adopted jobs keep their existing `systemd` runner | candybar |
| 3 | **Process protocol** | AD-8: `CANDYBAR_INVOCATION_ID`, `CANDYBAR_ID`, `CANDYBAR_TRIGGER`, `CANDYBAR_TICK_ID`, `CANDYBAR_RESULT_FILE` in; `{status, message<=500, metrics{token_spend,cost_usd}}` out (`runner.py:80`, `165-193`) | **CONSUME**, slice 2 | The `candybar-sdk` crate (`sdks/rust`, zero dependencies, edition 2024, rust-version 1.99.1; installed rustc is 1.99.1). Git dependency, never a vendored copy. Not in slice 1 because slice 1 has no `run` | candybar |
| 4 | **Execution receipt** | `Invocation` row: result, exit, runtime, cpu, memory, trigger, tick id. Read as `candybar show <name> --json` (0.37 s, returns definition, `runner`, recent `invocations`) | **CONSUME** read-through | `plan` shows last invocation. Slice 2 stores `CANDYBAR_INVOCATION_ID` on each snapshot record as a join. Ids live only in receipts, never in manifests | candybar |
| 5 | **Backup evidence** (bytes added, integrity scope, restore-proof scope, failure domain, holds, lineage) | Candybar's result file cannot carry it: only `status`, a 500-char `message`, `token_spend`, `cost_usd` (`runner.py:80-87`) | **EMMA** | The registry (section 4), populated from slice 2 | forever-ago |
| 6 | **Manifest discovery and validation rules** | `project.py`: git toplevel is the project (`:193`), files at `<root>/.candybars/*.candybar.toml` and `.candybar.toml` (`:59-66`), relative paths resolve against the project root and symlinks are kept (`:247`), strict top-level keys, all-or-nothing validation, duplicate-name check (`:400-423`) | **CONVENTION** | Same rules, one file: `<project root>/.emma.toml` (the name Jarad asked for). No `.emmas/` directory. Rust implementation in `src/emma/manifest.rs`. Candybar is Python and cannot be imported; copying its code is not an option and copying its rules is just conformance | candybar defines the pattern; forever-ago implements it |
| 7 | **Project identity and binding** | Candybar derives `project:<basename of git toplevel>` itself (`project.py:136,174-190`), so a repo `DeLoDocs` is tagged `project:DeLoDocs` while pjangler's id is `delodocs`. Candybar's steward separately reads pjangler `:8764/v1/registry` (`steward/jev.py:38`) | **CONSUME pjangler**, ignore Candybar's tag | `pj info --json` run in the manifest's directory returns `{project_id, repo_path, manifest_path}` (verified for `delodocs`, `candystore`). The listed `GET :8764/v1/resolve?cwd=` is planned and returns `Unknown registry endpoint` today, so it must not be called. When it lands Emma swaps the one function `identity::resolve` | pjangler (`features.toml:43`, atom `project.identity`) |
| 8 | **Registry** | Postgres, sole writer is the Candybar daemon, schema guard (`registry/schema_guard.py`), API on :8700 | **EMMA** (separate store) | Local SQLite under `$XDG_STATE_HOME/forever-ago/`. Not Candybar's registry: backup metadata must outlive and not depend on a service, and Candybar's guard forbids other writers | forever-ago |
| 9 | **Register / drift / ls vocabulary** | `candybar register [--dry-run\|--update\|--prune]`; sync states `new`, `unchanged`, `drift:<fields>`, `orphan`, `unowned`, `conflict:<project>`; "files own definition fields, never lifecycle"; "removing a file never deletes or archives" (README "Register and list the project", "Automatic sync on save") | **CONVENTION** | Identical verbs and state words. `drift()` is reimplemented on Emma's own fields. Exit codes follow the platform table (0 ok, 1 failure, 2 usage/config, 3 refused, 4 partial, 5 lock busy, 6 drift, 130 interrupted; `33god-platform` story 7.3, skillex `result.ts`) | candybar vocabulary; platform exit table |
| 10 | **Sync on save / watcher** | `candybar sync`, `watch`, Neovim hook | **NOT ADOPTED** | Emma has no daemon (INFR-170 constraint). `register` is explicit | n/a |
| 11 | **Failure alerting** | `on_failure` argv, runs `ntfy-alert` | **CONSUME** | Configured on the Candybar row, never by Emma | candybar |
| 12 | **Clock-change safety** | Migration ledger, `candybar doctor`, scheduling policy | **CONSUME** | Emma never schedules, so it never needs the ledger. Slice 1 touches no Candybar | candybar |
| 13 | **JSON envelope** | skillex `{schema:2, command, ok, exit, data, findings}` is the platform envelope (`HANDOFF-EPIC-7`, `epics.md` story 7.3) | **CONSUME** | Every new subcommand's `--json` emits it. Diagnostic codes `E_*`, `W_*`, `I_*` | 33god-platform (cli-surface candidate) |
| 14 | **Holocene control surface** | `apps/api/src/wax.ts` (reads `wax catalog --json`), `candybar.ts` (read-only proxy, cached, `source.down` degraded envelope), `HUB_TABS`/`NAV_GROUPS` entry in `navigation-model.ts:24` | **CONSUME the pattern** | Section 7 | holocene (L4 control surface: consumes, never owns a primitive) |
| 15 | **Secrets** | `op://` refs via op-router; Candybar `env` is non-secret literals only | **CONSUME** | Manifest holds `op://` references only, shape-checked and never resolved in slice 1 | op-router |
| 16 | **Bus facts** | Candybar emits `bloodbank.cron.candybar.*` | **NOT IN SLICE 1** | Emma emits nothing. Option for slice 3: facts for snapshot committed / retention planned. Undecided, no schema invented now | bloodbank |

### What this adds to the catalog (`~/code/33GOD/features.toml`)

- **No new atom.** Every atom Emma touches already has an owner (`clock`, `project.identity`). Emma is a consumer of both.
- **`[candidate.emma]`**, status `planned`: the capability spans forever-ago (executable), project repos (`.emma.toml`), Candybar (clock join), pjangler (identity), candystore (export verb, owner of its data) and Holocene (surface). That is the F1 test for a feature candidate. It is a candidate, not authorization for a new repo; forever-ago is the proposed hub.
- **`[candidate.project-declarations]`**, status `scattered`: four systems independently implement "declare in the project, derive a registry, report drift": Candybar (`.candybars/*.candybar.toml`), target-watch (`.watch/*.toml`, per `features.toml` `[candidate.target-watch]`), pjangler (`.project.json` plus registry reindex) and Emma (`.emma.toml`). The shared asset is a written contract of the rules in row 6 and 9, not a library. It is inventory only.
- **One request on an existing owner:** pjangler's planned `GET /v1/resolve?cwd=` now has a third wanting consumer (Candybar steward `jev.py`, Candybar's own `project:` tag, Emma). Emma wraps the provisional call in a single function so the swap is one line.

## 3. `.emma.toml` v1 schema

One file at the project root, owned and committed by the project. Parsed with `deny_unknown_fields` at every level. A key not listed here is an error, not ignored.

### 3.1 Grammar

```toml
schema_version = 1                      # required, integer, must equal 1

[[emmas]]                               # one or more; order is not significant
id          = "event-archive"           # required, ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$, <= 48 chars, unique in the file
description = "free text"               # optional, <= 200 chars, no control characters
mode        = "declared"                # optional: "declared" (default) | "adopted". "managed" is reserved and rejected in v1
sole_copy   = false                     # optional bool, default false (see 3.4)

[emmas.source]                          # required
kind = "files"                          # "files" | "export"

# kind = "files"
path         = "."                      # required. Relative paths resolve against the directory holding .emma.toml, NOT the cwd.
                                        # "~/" expands to $HOME. No $VAR expansion. The directory must exist at register time.
exclude      = ["*.aup4"]               # optional, same grammar as forever-ago --exclude, validated by the existing ExcludePattern::parse
exclude_from = ".backup-exclude"        # optional, resolved like path; same line grammar as --exclude-from

# kind = "export"
command  = ["candystore", "export"]     # required, argv, no shell. argv[0] with a slash must be absolute or is resolved against the project root
contract = "shards@1"                   # required, the owner's pinned export contract id (string, shape ^[a-z0-9-]+@[0-9]+$)

[emmas.capture]                         # a bare string "files@1" is shorthand for { strategy = "files@1" }
strategy = "files@1"                    # required (see 3.3)
staging  = "~/.local/state/forever-ago/staging/x"   # only for command-export@1: same-filesystem scratch dir

[emmas.backend]                         # string shorthand allowed when there are no params
strategy = "restic-local@1"             # required
repo     = "~/backups/emma/x.restic"    # restic-local@1: required path to a local repository
password_ref = "op://DeLoSecrets/<item>/<field>"   # restic-local@1: optional now, required before capture is enabled. Shape-checked only
dir      = "~/backups/delodocs"         # tar-gz-dir@1: required destination directory
prefix   = "vault"                      # tar-gz-dir@1: required file prefix (<prefix>-YYYY-MM-DD.tar.gz)

[emmas.retention]                       # string shorthand allowed
strategy = "age-buckets@1"              # required (see 3.3)
keep     = 3                            # legacy-count@1 only, integer >= 1
daily = 7                               # legacy-gfs@1 only; weekly and monthly likewise. At least one >= 1
weekly = 4
monthly = 4

[emmas.fidelity]                        # string shorthand allowed
strategy = "full@1"                     # required

[emmas.candybar]                        # optional. Absent means "unscheduled, run by hand", and plan says so
ref = "forever-ago-vault"               # a Candybar NAME (lowercase slug). Ids never appear in a manifest

[emmas.activation]                      # optional; every key defaults false
capture = false                         # true is rejected with E_NOT_IMPLEMENTED in slice 1 (fail closed)
prune   = false                         # same

[emmas.budget]                          # optional; all integers > 0
stored_bytes_max  = 0                   # no default. Absent means "capture is blocked" once capture exists
scratch_bytes_max = 0
duration_s_max    = 0
```

Rejected with a specific message instead of a generic unknown-key error: `schedule`, `cron`, `timer`, `interval`, `every`, `at` ("Candybar owns when; set `[emmas.candybar] ref`"); `project`, `project_id`, `uuid`, `slug` ("identity comes from pjangler; run `pj init`"); `password`, `token`, `secret`, `key` (anything but a `password_ref`).

### 3.2 Validation rules (all-or-nothing per file, then across the registry)

| Rule | Failure |
|---|---|
| `schema_version` present and equal to 1 | `E_SCHEMA_VERSION` |
| Valid TOML, no unknown key, ids unique | `E_PARSE`, `E_UNKNOWN_KEY`, `E_DUPLICATE_ID` |
| Every strategy ref exists in the catalog (3.3) with the given params only | `E_UNKNOWN_STRATEGY`, `E_BAD_PARAM`, `E_RESERVED_STRATEGY` |
| `files` source path exists, is a directory (or regular file) after canonicalisation | `E_SOURCE_MISSING` |
| Destination (`dir`/`repo`) is not inside the canonical source, and the source is not inside the destination | `E_DEST_INSIDE_SOURCE` (error), `W_SOURCE_INSIDE_DEST` (warning) |
| Destination is not claimed by another registered Emma (same canonical `dir`+`prefix`, or same `repo`) | `E_DEST_COLLISION` |
| Two Emmas in one file with overlapping canonical sources | `W_SOURCE_OVERLAP` |
| Retention params are positive; `legacy-count@1 keep = 0` and all-zero GFS are refused, matching `retention_policy` in `main.rs:269` | `E_BAD_PARAM` |
| `sole_copy = true` requires retention `keep-all@1` or `observe-only@1` | `E_SOLE_COPY_RETENTION` |
| `activation.capture` or `.prune` true | `E_NOT_IMPLEMENTED` |
| `candybar.ref` matches `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$` (Candybar's own `SLUG_RE`) | `E_BAD_CANDYBAR_REF`. Whether it *exists* is a `plan`/`projects` fact (`candybar: missing`), not a register error, so an offline Candybar never blocks registration |
| `password_ref` matches `^op://[^/]+/[^/]+/[^/]+$` | `E_BAD_SECRET_REF` |
| `mode = "adopted"` requires a `tar-gz-dir@1` backend and a `legacy-*` retention, and (checked live by `register`) a discovered legacy job with the same canonical source, `dir` and `prefix` | `E_ADOPT_NO_JOB`; later flag changes in the unit show as `drift:legacy-job` |
| Manifest must sit in the project's canonical checkout: the directory equals pjangler's `repo_path`, or is under it and is not a linked git worktree | `E_NOT_OWNER`. Exit 3, nothing registered, never silently repointed |

Paths are stored twice in the registry: as declared, and canonicalised (symlinks resolved) for overlap and destination checks, as INFR-170 required.

### 3.3 Strategy catalog v1

A strategy ref is `<name>@<int>`. The catalog is compiled in. There is no `latest`, no remote registry, no plugin runtime. Status values: **available** = accepted and fully planable in slice 1; **declared** = accepted, plan reports `capture: not implemented in this version`, coverage stays `none`; **reserved** = rejected by `register`.

| Axis | Strategy | Status | Meaning |
|---|---|---|---|
| capture | `files@1` | declared | Walk `source.path` honouring excludes. The writer is the existing tar writer for `tar-gz-dir@1`. Adopted Emmas are executed by their legacy job, not by Emma |
| capture | `command-export@1` | declared | Run the owner's export argv, which must be read-only on the source, deterministic for closed ranges and write a manifest. Checks producer and consumer exit status |
| capture | `postgres-logical@1` | reserved | From INFR-170 (Plane). Needs a bound owner source |
| backend | `tar-gz-dir@1` | available (adopt/plan) | Today's writer: `<prefix>-YYYY-MM-DD.tar.gz` plus `.sha256`. Inventory reuses `parse_backup_date` and `list::snapshots_in` |
| backend | `restic-local@1` | declared | Local encrypted repo. Host has restic 0.18.0 (verified); no repo exists |
| retention | `observe-only@1` | available | Inventory only; never selects anything for deletion. Default for adopted Emmas |
| retention | `keep-all@1` | available | Keep everything, assert it. Required by `sole_copy` |
| retention | `legacy-count@1 {keep}` | available | The existing `--retain N`. Preserved exactly |
| retention | `legacy-gfs@1 {daily,weekly,monthly}` | available | The existing `select_retained` (`main.rs:1037`), including "daily means N newest snapshots, not N calendar days" |
| retention | `age-buckets@1` | available (plan only) | Section 5.4. Proposed, not a committed policy |
| fidelity | `full@1` | available | Original content within the declared source scope. Exclusions stay explicit |
| fidelity | `conversation-prompts@1` | reserved | Lossy derived tier (section 6.4). `lossy = true`, separate Emma, never replaces a restore point |

### 3.4 `mode` and `sole_copy`

- `mode = "adopted"`: a Forever Ago legacy job (systemd unit, Candybar-run) already captures and prunes by its own flags. Emma records and checks that, and **never runs, prunes or reschedules it**. `legacy-*` retention is therefore documentation of what the legacy job does, executed by the legacy job.
- `mode = "declared"`: nothing executes yet. Coverage is `none` until a later slice.
- `sole_copy = true`: the artifacts at this source are the only copy of data already deleted from its live store (the CandyStore prune archives). Thinning is forbidden; see rule `E_SOLE_COPY_RETENTION`. `plan` always prints the independence axis next to it.

### 3.5 Registered files proposed for this slice (written by the build run, not by this design)

CandyStore, `~/code/33GOD/candystore/.emma.toml` (project id `candystore`, `.project.json` verified):

```toml
schema_version = 1

# The prune archives. `candystore prune` deletes rows right after fsyncing these
# files, so this directory is the ONLY copy of every event it has removed.
[[emmas]]
id          = "event-archive"
description = "CandyStore prune archives (jsonl.gz + manifest.json), sole copy of pruned events"
sole_copy   = true

[emmas.source]
kind = "files"
path = "~/backups/candystore/event-archive"   # candystore.retention.DEFAULT_ARCHIVE_DIR; CANDYSTORE_ARCHIVE_DIR can override it

[emmas.capture]
strategy = "files@1"

[emmas.backend]
strategy = "restic-local@1"
repo     = "~/backups/emma/candystore-event-archive.restic"

[emmas.retention]
strategy = "keep-all@1"

[emmas.fidelity]
strategy = "full@1"

# The live events table as a rolling, tiered series of full-fidelity restore points.
[[emmas]]
id          = "event-export"
description = "Live CandyStore events (+embeddings) as day-sharded exports; age-buckets restore points"

[emmas.source]
kind     = "export"
command  = ["candystore", "export"]            # owner verb requested in section 6.3; does not exist yet
contract = "shards@1"

[emmas.capture]
strategy = "command-export@1"
staging  = "~/.local/state/forever-ago/staging/candystore-event-export"

[emmas.backend]
strategy = "restic-local@1"
repo     = "~/backups/emma/candystore-event-export.restic"

[emmas.retention]
strategy = "age-buckets@1"

[emmas.fidelity]
strategy = "full@1"
```

Neither has `[emmas.candybar]` yet, so both show `unscheduled`. Adding the candybars is a clock change and a `needs_jarad` item.

The two existing jobs, from `adopt` (section 5), written to the project repos:

```toml
# ~/code/DeLoDocs/.emma.toml  (project id delodocs)
schema_version = 1

[[emmas]]
id          = "vault"
description = "Nightly local tarball of the DeLoDocs vault"
mode        = "adopted"

[emmas.source]
kind         = "files"
path         = "."
exclude_from = ".backup-exclude"

[emmas.capture]
strategy = "files@1"

[emmas.backend]
strategy = "tar-gz-dir@1"
dir      = "~/backups/delodocs"
prefix   = "vault"

[emmas.retention]
strategy = "legacy-count@1"
keep     = 3

[emmas.fidelity]
strategy = "full@1"

[emmas.candybar]
ref = "forever-ago-vault"
```

```toml
# ~/code/voxxy/.emma.toml  (project id voxxy)
schema_version = 1

[[emmas]]
id          = "voices"
description = "Weekly local tarball of the voxxy reference voices"
mode        = "adopted"

[emmas.source]
kind    = "files"
path    = "voices"
exclude = ["*.aup4", "*.aup4-wal", "*.aup4-shm"]

[emmas.capture]
strategy = "files@1"

[emmas.backend]
strategy = "tar-gz-dir@1"
dir      = "~/backups"
prefix   = "voxxy-voices"

[emmas.retention]
strategy = "legacy-count@1"
keep     = 1

[emmas.fidelity]
strategy = "full@1"

[emmas.candybar]
ref = "forever-ago-voxxy-voices"
```

Those values are read from the live units (`forever-ago jobs --all`, `~/docker/systemd/user/forever-ago-*.service`), not guessed: vault retains 3, excludes from `.backup-exclude`; voices retains 1 with three `*.aup4*` excludes.

## 4. Registry schema

One SQLite file, `$XDG_STATE_HOME/forever-ago/registry.sqlite3` (default `~/.local/state/forever-ago/`, absent today). Directory `0700`, file `0600`, WAL mode, `PRAGMA user_version = 1`, `PRAGMA foreign_keys = ON`. `FOREVER_AGO_STATE_DIR` overrides it (tests). `register` takes an `flock` on `registry.lock` and applies everything for one run inside one `BEGIN IMMEDIATE` transaction, after every manifest in the run has validated. Nothing else writes it.

**Derived state.** In slice 1 every row except `generations` history and the `register_log` is recomputable from manifests plus `pj info`. Deleting the file and running `register --all` rebuilds it. That recoverability is a tested acceptance criterion. Snapshot evidence (a later slice) cannot be recomputed from manifests, which is why its tables are specified below and created by the migration that first needs them.

```sql
-- schema v1 (slice 1)
CREATE TABLE projects (
  project_key   TEXT PRIMARY KEY,           -- pjangler project_id, or 'path:<canonical git toplevel>' when unbound
  identity      TEXT NOT NULL CHECK (identity IN ('pjangler','unbound')),
  project_id    TEXT,                       -- null when unbound
  repo_path     TEXT NOT NULL,              -- canonical checkout
  last_seen_at  TEXT NOT NULL               -- RFC 3339 UTC
);

CREATE TABLE manifests (
  manifest_path TEXT PRIMARY KEY,           -- canonical absolute path of .emma.toml
  project_key   TEXT NOT NULL REFERENCES projects(project_key),
  sha256        TEXT NOT NULL,              -- of the file bytes
  state         TEXT NOT NULL CHECK (state IN ('ok','missing','invalid','moved')),
  last_error    TEXT,                       -- findings JSON when invalid
  registered_at TEXT NOT NULL,
  last_seen_at  TEXT NOT NULL
);

CREATE TABLE emmas (
  project_key     TEXT NOT NULL REFERENCES projects(project_key),
  emma_id         TEXT NOT NULL,
  manifest_path   TEXT NOT NULL REFERENCES manifests(manifest_path),
  mode            TEXT NOT NULL CHECK (mode IN ('declared','adopted')),
  sole_copy       INTEGER NOT NULL DEFAULT 0,
  definition_json TEXT NOT NULL,            -- the validated, canonicalised definition, keys sorted
  definition_sha  TEXT NOT NULL,
  contract_sha    TEXT NOT NULL,            -- hash of source + capture + backend + fidelity only
  generation      INTEGER NOT NULL,         -- bumps when contract_sha changes
  source_declared TEXT NOT NULL,
  source_canonical TEXT,                    -- null for kind=export
  dest_canonical  TEXT,                     -- tar dir or restic repo, canonical
  candybar_ref    TEXT,                     -- a Candybar NAME or null
  sync_state      TEXT NOT NULL CHECK (sync_state IN ('ok','drift','orphan','retired')),
  first_registered_at TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  PRIMARY KEY (project_key, emma_id)
);
CREATE UNIQUE INDEX emmas_dest ON emmas(dest_canonical, json_extract(definition_json,'$.backend.prefix'))
  WHERE dest_canonical IS NOT NULL;          -- the E_DEST_COLLISION backstop

CREATE TABLE generations (                   -- append-only history; NOT recomputable
  project_key TEXT NOT NULL, emma_id TEXT NOT NULL, generation INTEGER NOT NULL,
  contract_sha TEXT NOT NULL, started_at TEXT NOT NULL, reason TEXT NOT NULL,
  PRIMARY KEY (project_key, emma_id, generation)
);

CREATE TABLE register_log (                  -- append-only audit of every register run; NOT recomputable
  id INTEGER PRIMARY KEY, at TEXT NOT NULL, verb TEXT NOT NULL,
  manifest_path TEXT, outcome TEXT NOT NULL, findings_json TEXT NOT NULL
);
```

Rules:

- **Identity** is `(project_key, emma_id)`. A repo moved on disk keeps its key: `register` sees the same `(project_id, emma_id)` at a new `manifest_path` and reports `moved` (updated by `--update`). If the old path still holds a manifest claiming the same key, that is `conflict:duplicate-owner` (exit 3) and nothing changes. A `pj rename` shows as `identity-changed` on the same `manifest_path` and rekeys only under `--update`.
- **Generation** bumps only when `contract_sha` changes (source, capture, backend, fidelity). Retention, candybar and budget edits never bump it, so a retention tweak cannot orphan history.
- **Sync states** mirror Candybar's: `new` (in manifest, not registered), `ok`, `drift:<fields>` (rerun with `--update`), `orphan` (registered, absent from its manifest), `conflict:<project>`. `--prune` marks orphans `retired`; it never deletes rows, generations or, later, snapshot records.
- **Removing or corrupting a manifest never deletes anything.** A missing file shows `manifests.state = missing`; a malformed one keeps the last valid definition and shows `invalid`, as Candybar does.

### Reserved for slice 2, not created in slice 1

```sql
CREATE TABLE snapshots (
  snapshot_id TEXT PRIMARY KEY,              -- immutable run id
  project_key TEXT NOT NULL, emma_id TEXT NOT NULL, generation INTEGER NOT NULL,
  captured_at TEXT NOT NULL,                 -- UTC; legacy tar: file mtime, filename date when mtime unusable
  time_source TEXT NOT NULL CHECK (time_source IN ('captured','mtime','filename')),
  status      TEXT NOT NULL CHECK (status IN ('success','incomplete','failed')),
  fidelity    TEXT NOT NULL,                 -- 'full@1' etc; derived rows carry lossy = 1
  lossy       INTEGER NOT NULL DEFAULT 0,
  parent_snapshot_id TEXT,                   -- lineage for derived archives
  capture_tool TEXT, capture_tool_version TEXT,
  consistency TEXT, integrity_scope TEXT, integrity_checked_at TEXT,
  restore_proof_scope TEXT, restore_proof_at TEXT, restore_proof_result TEXT,
  logical_bytes INTEGER, stored_bytes INTEGER, added_bytes INTEGER, reclaimed_bytes INTEGER,
  locator TEXT NOT NULL,                     -- backend-specific, no secrets
  candybar_invocation_id TEXT,               -- join to the execution receipt
  hold TEXT                                  -- manual/legal hold name, null if none
);
```

These columns carry INFR-170's separate evidence axes: capture consistency, byte integrity, restore proof and independence are never collapsed into one "ok".

## 5. CLI surface for this delivery

All new subcommands are additive variants of the existing `Command` enum in `main.rs`. The legacy top-level flags and the `jobs` and `list` subcommands are untouched. clap's `args_conflicts_with_subcommands = true` already keeps the two modes apart. **Nothing new calls `run_backup` or `prune_old_backups`**, and no new subcommand accepts `--once`.

Global contract for new subcommands: `--json` prints `{schema:2, command, ok, exit, data, findings[]}`; human output is the default; exit codes per the platform table (0 ok, 1 failure, 2 usage/config including invalid manifest, 3 refused, 4 partial, 5 lock busy, 6 drift, 130 interrupted). An unknown subcommand exits 2. Each external read has a bounded timeout: `pj` 10 s, `candybar` 10 s, no retries. `--offline` skips both and reports `candybar: unknown`, `identity: unverified`.

| Command | Reads | Writes | Behaviour |
|---|---|---|---|
| `forever-ago register [PATH] [--all] [--dry-run] [--update] [--prune] [--json]` | the manifest(s), `pj info --json`, `jobs::discover_host` (for `adopted`), `candybar show <name> --json` (info only) | registry (never in `--dry-run`, which also does not create the state dir) | `PATH` is a dir or the file, default cwd, resolved to the nearest `.emma.toml` up to the git toplevel. `--all` iterates `pj list --json` repo paths and reads **only** `<repo_path>/.emma.toml` (no home scan). Validates everything, then one transaction. Per Emma prints `new`, `unchanged`, `drift:<fields>`, `moved`, `orphan`, `conflict:<project>`. Exit 0, or 6 when drift exists and `--update` was not given, 2 invalid, 3 conflict/not owner. Registration enables nothing, changes no clock |
| `forever-ago adopt [--write] [--json]` | `jobs::discover_host(false)`, `pj info --json` per source, `candybar show <name> --json` | with `--write`: a new `<repo_path>/.emma.toml` only if absent | For each discovered legacy job inside a pjangler project, prints the `.emma.toml` entry that describes it exactly. The Candybar ref is the unit stem (`forever-ago-vault.service` becomes `forever-ago-vault`) and is kept only if `candybar show` confirms `runner.unit` equals that unit; otherwise a finding and no ref. An existing manifest is never edited (exit 3; the proposal is printed to merge by hand). Never touches a unit file, Candybar, or the ledger |
| `forever-ago projects [--project ID] [--offline] [--json]` | registry, live artifact scan (`tar-gz-dir@1`), Candybar read-through | none | One row per Emma: project, id, mode, sync state, schedule and last invocation from Candybar, source, destination, independence (`same-device` or `same-host`), snapshot count, newest, bytes, **coverage verdict** (below) |
| `forever-ago plan <[PROJECT/]EMMA> [--measure] [--max-entries N] [--json]` | registry, source and destination metadata (`stat`, device ids, free space), Candybar, legacy jobs | **none, not even the registry** | Pure report: resolved canonical paths and strategy revisions; capture argv with no secrets; destination existence, device and free bytes; Candybar join (`ok`, `disabled`, `archived`, `missing`, `unknown`, `runner-mismatch`); for `adopted`, the legacy job comparison (`drift:legacy-job`) and `W_EXEC_IN_BUILD_DIR` when the unit executes a binary under a `target/` directory (the vault unit does today, so `cargo clean` would silently end the nightly backup); live snapshot inventory; retention semantics; blockers for any future `run`. `--measure` does a bounded read-only walk (entries and bytes) for `files` sources, default off |
| `forever-ago retention plan <[PROJECT/]EMMA> [--as-of RFC3339] [--simulate-daily DAYS] [--inventory-json FILE] [--json]` | the inventory, or a supplied/synthetic one | **none** | Section 5.4. There is **no `retention apply`** in this version; typing it is a usage error (exit 2) |

`[PROJECT/]EMMA` accepts `delodocs/vault` or just `vault` when unambiguous in the registry (ambiguity is exit 2 listing the candidates).

### 5.1 Coverage verdict

Computed on demand, never stored. Slice 1 emits only these, and **never a green**:

| Verdict | When |
|---|---|
| `broken:source-missing` | the canonical source path no longer exists |
| `none` | no snapshots found (every `declared` Emma today) |
| `local-only` | snapshots exist and the destination is on this host. `independence` is printed beside it: `same-device` (same `st_dev` as the source, or as the live store for `sole_copy`) or `same-host` |
| `unknown` | the destination cannot be read |

A green verdict requires an independent failure domain, verified integrity and a scoped restore proof. None can exist before slice 3, so none can be shown. This is the INFR-170 negative case made concrete: an absent source, a same-device destination, an untested restore or a derived-only copy can never display as covered.

### 5.2 Registry versus existing `jobs` and `list`

`forever-ago jobs --all` today reports both existing jobs as "schedule NONE ... WARNING will not fire: its unit is not enabled/active" (verified). That is a false alarm: Candybar starts the unit (live: `forever-ago-vault`, `0 3 * * *` America/New_York, enabled, 32 of 32 runs successful, last run 2026-11-26 08:00 UTC, 26 s). `jobs` has no Candybar scheduler variant. `plan` resolves the truth through the join. Fixing the legacy `jobs` output is deliberately not in slice 1 (changing it risks the byte-identical non-regression check); it is a follow-up.

### 5.3 Module layout and dependencies

`src/emma/{mod,manifest,identity,candybar,registry,strategies,retention,plan,envelope}.rs`, wired from `main.rs`. Reuse, do not copy: `ExcludePattern::parse`, `read_exclude_file`, `jobs::discover_host`, `jobs::canonical_or_normalized`, `parse_backup_date`, `human_size`, `select_retained`. New crates: `serde` (derive), `toml`, `rusqlite` (`bundled`). `identity.rs` and `candybar.rs` shell out to `pj` and `candybar` through `std::process::Command` with timeouts (the pattern `jobs.rs` already uses for `systemctl`), so there is no HTTP client and no second implementation of either owner's resolution.

### 5.4 `age-buckets@1`: the deterministic retention plan

Specification (frozen when this slice ships; any change is `age-buckets@2`):

- Inputs: a set of snapshots `{id, captured_at (UTC), status, hold?}`, a frozen reference time `as_of` (default: now, recorded in the plan), and the partition key `(project, emma, generation, fidelity, source contract)`. Snapshots in different partitions are never compared.
- Candidates are `status = success` and full fidelity only. `incomplete` and `failed` snapshots are listed separately and **never selected, pinned or forgotten** by this plan.
- Age bands are half-open on `as_of - captured_at`:
  - `[0, 14d)`: keep **all**.
  - `[14d, 90d)`: keep the newest per **3-day bucket**, buckets fixed and anchored to the UTC Unix epoch (`floor(unix_day / 3)`).
  - `[90d, 365d)`: keep the newest per **ISO week** (UTC).
  - `>= 365d`: keep the newest per **UTC calendar month**.
- "Newest" is the greatest `captured_at`; a tie is broken by the lexicographically greatest `id`. A band only groups its own members, so a bucket straddling a band edge can be represented in both bands.
- Independent pins that no band can override: the latest successful snapshot, every snapshot with a `hold`, and the last known-good restore point (empty in slice 1, since no restore proof exists).
- A snapshot with `captured_at > as_of` is **kept for review**, never forgotten.
- **Freeze rule:** if the `[0,14d)` band holds no eligible snapshot, the plan is `frozen`: everything is kept and the finding says why. A failed or stalled chain can never age its only usable history away.
- Empty periods invent nothing.
- Output per snapshot: `keep` with one or more reasons (`band0-all`, `bucket3d:<n>`, `week:<iso>`, `month:<yyyy-mm>`, `pin-latest`, `hold:<name>`, `review-future-dated`, `frozen`) or `forget` with `superseded-by:<id>`. Also per band: counts and the **maximum recovery gap** the kept set leaves (including boundary effects), and a `plan_digest` = SHA-256 over canonical JSON of strategy, `as_of`, partition and decisions. The same inputs must give the same digest regardless of input order.
- `observe-only@1` and `keep-all@1` plan to "keep all". `legacy-count@1` and `legacy-gfs@1` plan through the existing `select_retained` and are labelled **executed by the legacy job; Emma will not act**. For `sole_copy`, only the first two are legal.
- `--simulate-daily N` fabricates one snapshot per day for N days ending `as_of`, marked `what_if = true`. `--inventory-json` plans over a supplied list, also `what_if`. A what-if plan carries no apply authority and says so.

Reference results from a throwaway implementation of exactly this rule (`/tmp/emma-sim/sim.py`, one snapshot per day at 06:00 UTC, `as_of` 2026-11-26 12:00 UTC). These are the slice's golden values and what Jarad should review before approving the policy:

| Days of daily history | Restore points kept | Dense `[0,14d)` | `[14d,90d)` | `[90d,365d)` | `>=365d` | Longest gap between kept points |
|---:|---:|---:|---:|---:|---:|---:|
| 30 | 20 | 14 | 6 | 0 | 0 | 3 d |
| 90 | 40 | 14 | 26 | 0 | 0 | 3 d |
| 180 | 54 | 14 | 26 | 14 | 0 | 7 d |
| 365 | 80 | 14 | 26 | 40 | 0 | 7 d |
| 400 | 82 | 14 | 26 | 40 | 2 | 26 d |
| 730 | 93 | 14 | 26 | 40 | 13 | 31 d |

The steady state after the first year is about 80 points plus one per month, and the longest gap grows from 3 days to about a month at the far end, which is "less detail, more spread, the further back". The 26-day gap at 400 days is a real boundary effect (the monthly band starts at day 365 and the first month bucket is partial), and the plan reports it instead of hiding it.

## 6. CandyStore event history as the first Emma

### 6.1 What the code and disk show

| Fact | Evidence |
|---|---|
| CandyStore is pjangler project `candystore`, repo `~/code/33GOD/candystore`, manifest `.project.json` present | `pj info --json`, registry `:8764` |
| Events are **insert-only**. The only writers are ingest (`candystore/db.py:314`) and `restore`'s `ON CONFLICT DO NOTHING` (`retention.py:191`). No `UPDATE events` exists in `candystore/` or `migrations/` | grep, 2026-11-26 |
| `candystore prune` archives a batch (gzip JSONL, one `{"event": ..., "embedding": ...}` line per row, `search_text` dropped), fsyncs file and directory, **then deletes exactly those ids** in the same transaction | `retention.py:127-149` |
| Archives live in `~/backups/candystore/event-archive/`, 507 MiB (531,516,311 bytes) today, per-run `*.jsonl.gz` plus `*.manifest.json {types, older_than_days, cutoff, deleted, archive, archive_sha256, archive_bytes}`, newest dated 2026-11-25 08:30 | `ls`, manifest read |
| Rules: `.candybars/candystore-event-retention/rules.txt`, three types at 30 days, Jarad-approved 2026-11-23 | file |
| The clock is Candybar `candystore-event-retention`, daily `30 8 * * *` America/New_York, tag `project:candystore`, **enabled in the live registry** and producing archives, while its committed manifest says `enabled = false`. That is not a contradiction: `enabled` is creation-time state, pause and resume own it afterwards (Candybar README) | live `GET /candybars`, archive mtimes |
| The README itself states: archives sit on the same disk as the database, which is not off-host recovery | `retention-candybar README.md` |
| A separate unmanaged native clock (`/etc/cron.daily/db-backup`, `postgres-nightly-dump.sh`) writes local whole-database dumps (2,712,791,494 bytes on 2026-11-11, ten retained at 22.4 GB). Its Candybar classification is open in CandyStore's own audit | `docs/audits/2026-11-11-content-retention.md` section 5. Not touched here, not an Emma in slice 1 |
| `events` is ~41 GB over ~6.1M rows (supplied in the brief, not remeasured) | brief |
| Root disk is 92% used, 324 GiB free | `df`, 2026-11-26 |

So complete event history is **live table plus archives**. They need different treatment: the first is thinnable restore points of a whole table, the second is irreplaceable files.

### 6.2 The two Emmas

| | `event-archive` | `event-export` |
|---|---|---|
| Source | the prune archive directory | a read-only export of the live `events` + `event_embeddings` |
| Kind | `files`, `sole_copy = true` | `export`, owner verb |
| Retention | `keep-all@1` (anything else is `E_SOLE_COPY_RETENTION`) | `age-buckets@1` |
| Why | They hold events that exist nowhere else. Thinning a snapshot series of an append-only directory would be lossless, but `keep-all` is the safe and trivially checkable rule, and snapshot metadata is nearly free | Restore points protect against a bad prune rule, a bad migration or corruption. Here point-in-time matters, because `prune` is a legitimate mutation |
| Backend | `restic-local@1` (declared) | `restic-local@1` (declared) |
| Slice 1 shows | 12 archive files observed, bytes, newest, `same-device`, `sole_copy`, verdict `none` | `declared`, verdict `none`, blocker `owner export verb missing`, plus the `retention plan --simulate-daily` preview |

**Why not simply a dated full dump every day, thinned by `age-buckets@1`?** Because it multiplies a large artifact. At the steady state above that is about 80 independent full copies. At the only measured dump size (2.71 GB, taken when the whole database was 30 GB; it is larger now), that is **at least 217 GB, about 62% of the 348 GB free today**, with no dedup. That alone rules out the naive form. Hence the export format below, so each restore point costs roughly the new data since the last one, not a whole copy. The dedup ratio has not been measured and is not claimed; slice 2 must measure it on real shards before any capture is enabled.

### 6.3 The export contract (a request to CandyStore, the owner of its data)

Emma owns what, how and where of the backups. CandyStore owns its schema and its rows, so the export verb is CandyStore's to build and the contract is its to accept. Requested as a CandyStore board item (CANDYS), **not built or filed here**:

`candystore export --out DIR --format shards@1 [--since YYYY-MM-DD] [--until YYYY-MM-DD]`

- One read-only `REPEATABLE READ` transaction. It holds no write lock and has no DSN in Emma (Emma never sees a database credential, never connects, cannot delete).
- Output is **day shards**: `events/YYYY/MM/YYYY-MM-DD.jsonl.gz`, rows grouped by `received_at` UTC day and ordered `(time, id)`, deterministic gzip (fixed level, no embedded mtime, single member), so a closed day is byte-identical across runs and restic dedups it completely. Only the open day, and days from which `prune` removed rows, change.
- **Record shape is exactly the prune archive's** (`{"event": ..., "embedding": ...}`). That is what lets the existing `candystore restore <shard>` replay any shard unchanged. There is one restore path for archives and exports. Emma writes no restore logic.
- A `manifest.json` with per-shard row counts, `sha256`, the transaction's high-water mark and the contract id. Emma records it and treats the export as incomplete if it is missing or inconsistent.
- Exit 0 only for a complete export. Emma checks the producer's exit status, never a masked pipe.

Slice 1 only *declares* this contract; it neither calls nor mocks it.

### 6.4 Optional lossy derived tier (declared future, reserved in v1)

A third Emma, `event-prompts`, derived **from the lossless export, never from the live table**, using fidelity `conversation-prompts@1`: keep prompt-bearing records (the candidate is `bloodbank.conversation.turn.started`, whose `data.prompt` the 2026-11-11 audit measured at 100% coverage for Claude and Codex) with stable ids, roles, timestamps, ordering and source hashes; drop tool payloads. Rules carried from INFR-170 and Jarad's stated direction: it is a **separate** archive with `lossy = true` and its own lineage; it never replaces a restore point; it never mutates, strips or deletes canonical history; the exact field contract and event-type allowlist are the owner's to approve; unknown types fail closed. `register` rejects it as `E_RESERVED_STRATEGY` until a reviewed contract and a fixture-proven implementation exist.

### 6.5 Relation to `candystore prune`, `restore` and `rules.txt` (reuse, do not duplicate)

| Existing piece | Emma's relation |
|---|---|
| `rules.txt` (what leaves the live DB, and when) | Untouched. Emma never reads, writes or encodes event-type or day rules. Backup retention and source retention are independent decisions (the 2026-11-11 audit already says so) |
| `candystore prune` | Untouched. It stays the only thing that deletes events. Emma's archive Emma treats its output as opaque immutable files; it may later verify each file against the owner's own `archive_sha256` in `*.manifest.json` (read-only), not recompute a second scheme |
| `candystore restore` | The single restore verb for event records. A future `restore-test` runs it against a disposable database; Emma adds no insert logic |
| Candybar `candystore-event-retention` | Unchanged. Emma adds no second candybar for the same job |
| `event-archive` Emma | New, and the real gap this finds: the archives are the sole copy of deleted data and share a disk with the database |
| Ordering | A tightened rule moves rows into archives, and until the archive Emma has captured them (and a copy is off-disk) the only copy is local. `plan` shows `sole_copy` with `same-device` so this never reads as protected |

CandyStore also has an independent source-of-truth for retention policy (its audit proposes 60 days online, lossless archiving beyond). If that changes, it changes `rules.txt` and nothing in `.emma.toml`.

## 7. Holocene surface

Holocene is an L4 control surface: it consumes, it never owns a primitive. Existing pattern to follow (read, not guessed): `apps/api/src/wax.ts` runs `wax catalog --json` and projects it; `candybar.ts` proxies Candybar read-only with short caches, bounded concurrency and a last-good `source.down` envelope; the tab is a `NAV_GROUPS` item under System (`navigation-model.ts:24` for Candybar).

- **Source:** `forever-ago projects --json` and, per row, `plan --json` for detail. Holocene shells out through the same `execFile` pattern, timeout-bounded, `FOREVER_AGO_BIN` override, and validates `schema === 2`. It reads no SQLite file and no manifest.
- **API module:** `apps/api/src/emma.ts`, `GET /api/modules/emma/projects`, cached 10 s, `source.down` with last good on failure (never healthy emptiness), AD-9 envelope fields added by Holocene (`generated_at`, freshness) around Emma's data.
- **Web:** a read-only **Backups** tab in System, one row per Emma: project, id, mode, sync state chip, verdict chip, **independence badge** (`same disk` is loud), schedule and last result from the Candybar join with a link to the existing `/candybar` row, newest snapshot age, bytes. A `sole_copy` marker. The retention preview (`retention plan --simulate-daily`) renders as the keep/forget timeline.
- **No write route.** "Run now" is Candybar's existing row action; Holocene adds none. Holocene's own `events.sqlite3` retention (`event-retention.ts`) is a different store from CandyStore and is not touched or conflated.
- **When:** slice 3, a Holocene ticket after `projects --json` is stable. The CLI is useful alone before that.

## 8. Build slice 1 (what the next run builds)

**Goal:** the manifest, the registry, and `register` / `adopt` / `projects` / `plan` / `retention plan`, observe-only, adopting the two live jobs with no clock change, plus the CandyStore declaration.

### Deliverables

1. `src/emma/*` per 5.3; `Command` variants wired in `main.rs`; `serde`, `toml`, `rusqlite` added. Version bump to `0.4.0`.
2. `docs/emma.md` operator page and the README section ("Scheduling" must stop recommending new systemd timers or the built-in daemon on this host and point at Candybar; the stale text is INFR-170 finding, update it here).
3. `.emma.toml` in `~/code/33GOD/candystore`, `~/code/DeLoDocs`, `~/code/voxxy` (section 3.5), each committed to its own repo by explicit path only. DeLoDocs and voxxy are written by `forever-ago adopt --write`, so the run proves `adopt`.
4. `forever-ago register --all` against the real host, then `projects`, `plan` for all four Emmas, and `retention plan` previews.
5. The golden table in 5.4 as tests.
6. Land everything with `git unpushed` clean: forever-ago, candystore, DeLoDocs, voxxy, and the installed binary (`cargo install --path .`, not `target/release`, so a `cargo clean` can never end a backup; the existing vault unit still points at `target/release` and `plan` flags it, and re-pointing a unit is a separate approved change).

### Tests (acceptance)

- **Manifest:** every row of 3.2 has a positive and a negative test, including: `schedule`/`cron` rejected with the Candybar pointer, `project_id` rejected, unknown strategy, zero keep, all-zero GFS, `sole_copy` with `age-buckets@1`, `activation.capture = true`, destination inside source, destination collision, secret-shaped key, relative paths resolving against the manifest directory not the cwd, symlinked source, manifest in a linked git worktree (`E_NOT_OWNER`).
- **Registry:** created `0700`/`0600`; `register --dry-run` creates nothing; second `register` is `unchanged`; editing a retention value is `drift:retention` and exit 6 until `--update`; editing the source bumps `generation`; a moved repo is `moved`; two manifests claiming one `(project, id)` is `conflict` exit 3 with nothing changed; deleting the registry then `register --all` reproduces the same `emmas` rows; a corrupt manifest keeps the last valid definition and reports `invalid`.
- **Adoption:** `adopt --json` produces exactly the section 3.5 vault and voices entries from the live units, with Candybar refs verified; an unconfirmed ref is dropped with a finding; `adopt --write` refuses an existing file.
- **Plan:** for `delodocs/vault`: Candybar `ok`, `0 3 * * *` `America/New_York`, last run success; 3 snapshots found; `local-only`, `same-device`; `W_EXEC_IN_BUILD_DIR`; the legacy-job comparison clean. For `candystore/event-archive`: archive file count, bytes, newest, `sole_copy`, verdict `none`. For `candystore/event-export`: verdict `none`, blocker `owner export verb missing`. `plan` leaves the registry file's mtime and bytes unchanged.
- **Negative coverage cases:** absent source gives `broken:source-missing`; same-device destination never exceeds `local-only`; no code path in slice 1 can emit a green verdict.
- **Retention:** the golden counts in 5.4 reproduced; permuting input order gives an identical `plan_digest`; future-dated snapshots kept for review; empty dense band yields `frozen`; ties resolve by id; holds override every band; incomplete snapshots untouched; an unknown strategy or a `sole_copy` Emma with `age-buckets@1` is refused. `retention apply` is a usage error.
- **Non-regression of legacy behaviour:** `forever-ago jobs --all` and `forever-ago list` output is byte-identical before and after (diff against a captured baseline); the existing `cargo test` suite passes unchanged; nothing under `src/emma/` references `run_backup` or `prune_old_backups` (checked by a test that greps the module tree). No test and no acceptance step runs a legacy backup against a real directory.
- **Heavy work:** `cargo build --release` and the full test run go through `heavy <cmd>`.

### Explicitly not in slice 1

`run`, capture of any kind, `restic init`, any repository or staging directory creation, `retention apply`, forgetting or pruning any snapshot, deleting any CandyStore event, adding or changing any Candybar, enabling the `activation` flags, off-host upload, the lossy derived tier, Holocene code, the `candystore export` verb, the Candybar `project:` tag question, and changing the legacy `jobs` output. Each becomes a decision below.

## 9. Decisions only Jarad can make (all parked, none assumed)

1. **Approve `age-buckets@1` as the committed rule** (14 days dense, 3-day, ISO week, month) after reading the preview in 5.4. Until approved it is a plan on a declared Emma, not a policy.
2. **Approve retention `apply`** (the first deletion of snapshot records) and physical reclaim, as a separate later slice with its own plan-digest gate. Not in slice 1.
3. **Approve an `event-export` capture run and its budget.** It needs `candystore export` built by CandyStore first, a restic repository created, and a measured dedup ratio. The disk is 92% full with 324 GiB free.
4. **Off-disk destination for the sole-copy archives.** This is the real durability gap: the only copy of every pruned event shares a disk with the database. Any new cloud or paid storage is a spend decision; same-root MinIO does not count as a second failure domain.
5. **Clock changes.** Candybars for the two CandyStore Emmas (names and times; propose `event-archive` shortly after the 08:30 America/New_York prune, then `event-export`), and classifying the native `cron.daily` database dump.
6. **Lossy derived tier:** the event-type allowlist, field contract and the 14-day boundary, and whether raw exports may ever be deleted after derivation (default: no).
7. **Whether `candystore prune` should refuse to run while its archive directory has no off-disk copy** (a policy coupling between two components; not designed here).

## 10. Follow-ups (not decisions)

- CandyStore board item for the `shards@1` export verb (section 6.3).
- pjangler: a third consumer for `GET /v1/resolve?cwd=`; Candybar's `project:<basename>` tag differs from pjangler ids (`DeLoDocs` vs `delodocs`), a candidate for Candybar to consume the resolver.
- Forever Ago `jobs` has no Candybar scheduler variant and shows false "will not fire" warnings for both live jobs.
- Re-point the vault unit from `target/release` to the installed binary (approved change, plus the migration-ledger rule if it touches a Candybar runner).
- Plane and agent-session Emmas (INFR-170 section 6) become the next two `.emma.toml` files once their owner source contracts exist; they are the next members of the neighbouring level and need nothing new.
- Holocene Backups tab (section 7); optional bus facts (row 16).

## Sources and scope of proof

Read: `~/code/33GOD/candybar/src/candybar/{project.py,runners.py,registry/manifest.py,registry/models.py,executor/runner.py,executor/worker.py,api/app.py,cli/control.py,steward/jev.py}`, `sdks/rust/`, `README.md`, `AGENTS.md`; `~/code/33GOD/holocene/apps/api/src/{candybar.ts,wax.ts,event-retention.ts}`, `apps/web/app/hub/navigation-model.ts`; `~/code/33GOD/features.toml`; the 33GOD CLI-surface handoff and story 7.3; skillex `src/core/result.ts`; `~/code/forever-ago/src/{main.rs,jobs.rs,list.rs}`; `~/code/33GOD/candystore/{candystore/retention.py,.candybars/candystore-event-retention/*,docs/audits/2026-11-11-content-retention.md}`; the two live forever-ago units and their Candybar manifests.

Executed, read-only: `GET http://127.0.0.1:8700/candybars` and `/candybars/{id}`, `/runner`, `/invocations`; `candybar show forever-ago-vault --json`; `GET http://127.0.0.1:8764/v1/registry`; `pj info --json` in `~/code/DeLoDocs` and `~/code/33GOD/candystore`; `forever-ago jobs --all` (the installed 0.3.0 discovery view); `ls`/`cat` of the archive directory and manifests; `df`; and one throwaway Python simulation of `age-buckets@1` (outside every repo). No legacy backup, prune, restore, `--once`, database query, restart or write to any service ran. Not verified and so not claimed: dedup savings, the current dump size, the exact current `events` size, restore correctness of any new path, off-host durability of anything.
