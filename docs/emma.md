# Emma operator guide (forever-ago 0.4.0)

Emma lets a project declare, in one file, what must be backed up. Forever Ago reads that file, remembers it in a
local registry and tells you the truth about it. **This version never captures, prunes, restores or schedules.**
Every command below is read-only toward your data; `register` and `adopt --write` write only the registry and a
new `.emma.toml`.

Design, ownership decisions and the parked questions: [EMMA-DESIGN.md](EMMA-DESIGN.md).

## Who owns what

| Concern | Owner | How Emma uses it |
|---|---|---|
| When it runs, run history | Candybar | `[emmas.candybar] ref = "<name>"`, read with `candybar show <name> --json`. A manifest has no schedule key; `schedule`/`cron`/`timer` are refused with `E_CLOCK_KEY`. |
| Project identity | pjangler | `pj info --json` run in the manifest's directory. A directory pjangler does not know is `unbound`, keyed `path:<root>`. `project_id`/`uuid` in a manifest are refused (`E_IDENTITY_KEY`). |
| What the data is and how to export it | the project | `source`, and for exports the project's own export verb. |
| Registry, plan, retention arithmetic | forever-ago | `src/emma/`. |

## Commands

### `register [PATH | --all]`
Loads `.emma.toml` (searching upward from PATH to the git toplevel), validates it, binds it to its pjangler
project and records it. `--all` visits `<repo_path>/.emma.toml` for every pjangler project (no home scan).
`--dry-run` writes nothing, not even the state directory. Re-running is a no-op (`unchanged`).

| State | Meaning | Exit |
|---|---|---|
| `new`, `unchanged` | | 0 |
| `drift:<fields>` | manifest changed since registration; applied only with `--update` | 6 |
| `moved` | same project, new path; `--update` applies it | 6 |
| `orphan` | registered Emma no longer in the manifest; `--prune` marks it `retired` (rows and history are never deleted) | 0 |
| `conflict:duplicate-owner`, destination collision, manifest in a linked worktree (`E_NOT_OWNER`) | refused, nothing changed | 3 |
| invalid manifest | last valid definition is kept and the manifest is marked `invalid` | 2 |

Other exit codes: 1 failure, 4 partial, 5 registry lock busy. `--json` wraps every command in
`{schema:2, command, ok, exit, data, findings[]}`.

### `adopt [--write]`
Reads the live legacy jobs (`forever-ago jobs`), binds each to its project and prints the `.emma.toml` entry that
describes it exactly, with `mode = "adopted"`: the legacy job keeps capturing and pruning by its own flags. A
Candybar `ref` is written only when Candybar confirms that Candybar starts that unit. `--write` creates the file
only if absent (`E_MANIFEST_EXISTS` otherwise); an existing manifest is never edited. Jobs that run forever-ago's own
daemon loop are skipped.

### `projects` and `plan`
`projects` lists every registered Emma with schedule, snapshots, independence and a coverage verdict. `plan
<project/emma>` shows one in full plus the blockers a future capture run would still hit. `--measure` does a bounded
read-only walk of a files source. `--offline` skips Candybar.

**Coverage verdicts** are exactly `broken:source-missing`, `none`, `local-only` or `unknown`. There is no green:
nothing here can prove a backup is restorable or off-host. Independence is `same-device` or `same-host` today.

### `retention plan <project/emma>`
Which restore points the declared retention would keep and forget, as a table, with a `plan_digest` over canonical
JSON that does not depend on input order. There is **no `retention apply`** in this version (usage error, exit 2).
`--simulate-daily N` previews on N synthetic daily snapshots ending at `--as-of`; `--inventory-json FILE` previews on
your own list of `{id, captured_at, status?, hold?, generation?, fidelity?}`.

## `.emma.toml` (schema_version = 1)

Closed schema: an unknown key is an error at every level. Relative paths resolve against the directory holding the
manifest (not the cwd); `~/` is `$HOME`; `$VAR` is refused.

```toml
schema_version = 1

[[emmas]]
id = "vault"                       # slug, <= 48 chars, unique in the file
description = "..."
mode = "adopted"                   # "declared" (default) | "adopted"
sole_copy = false                  # true requires keep-all@1 or observe-only@1

[emmas.source]
kind = "files"                     # or "export" (command = [...], contract = "shards@1")
path = "."
exclude = ["*.aup4"]
exclude_from = ".backup-exclude"

[emmas.capture]
strategy = "files@1"
[emmas.backend]
strategy = "tar-gz-dir@1"          # dir, prefix.  restic-local@1: repo, password_ref = "op://..."
dir = "~/backups/delodocs"
prefix = "vault"
[emmas.retention]
strategy = "legacy-count@1"        # keep.  Also legacy-gfs@1, keep-all@1, observe-only@1, age-buckets@1
keep = 3
[emmas.fidelity]
strategy = "full@1"
[emmas.candybar]
ref = "forever-ago-vault"          # a Candybar NAME, never an id
```

Strategies are `name@version`. Status is `available` (implemented), `declared` (accepted, not implemented yet;
`plan` lists it as a blocker) or `reserved` (refused). `activation.capture` and `activation.prune` default to false
and `true` is refused with `E_NOT_IMPLEMENTED` (fail closed). Secrets are never stored: `password_ref` must be an
`op://vault/item/field` reference, and a raw value is rejected without being echoed.
Examples: `docs/examples/candystore.emma.toml`, and the adopted manifests in `~/code/DeLoDocs` and `~/code/voxxy`.

## `age-buckets@1`

Rolling, tiered restore points: less detail the further back, wider spacing the older.

| Age | Kept |
|---|---|
| [0, 14 d) | every snapshot |
| [14 d, 90 d) | the newest per 3-day bucket (`floor(unix_day/3)`) |
| [90 d, 365 d) | the newest per ISO week |
| >= 365 d | the newest per UTC month |

"Newest" is the greatest `captured_at`; ties go to the greatest id. The newest snapshot is always pinned
(`pin-latest`) and a `hold:<name>` overrides every band. Future-dated snapshots are kept for review. If the dense band
is empty (capture has stalled) the plan freezes and keeps everything. Incomplete, failed and lossy snapshots are left
untouched. Daily snapshots at 06:00Z, as of 2026-11-26T12:00Z:

| History | Kept | Per band | Longest gap |
|---|---|---|---|
| 30 d | 20 | 14 / 6 | 3 d |
| 90 d | 40 | 14 / 26 | 3 d |
| 180 d | 54 | 14 / 26 / 14 | 7 d |
| 365 d | 80 | 14 / 26 / 40 | 7 d |
| 400 d | 82 | 14 / 26 / 40 / 2 | 26 d |
| 730 d | 93 | 14 / 26 / 40 / 13 | 31 d |

These are golden tests in `src/emma/tests.rs`.

## Registry

SQLite at `$FOREVER_AGO_STATE_DIR` or `$XDG_STATE_HOME/forever-ago/registry.sqlite3` (default
`~/.local/state/forever-ago/`), directory `0700`, file `0600`, WAL, one transaction per `register`, guarded by
`registry.lock`. It is **derived**: delete it and `forever-ago register --all` rebuilds the same rows from the
manifests. The generation counter bumps only when the contract (source, capture, backend, fidelity) changes, never for a
retention edit.

## Not in this version

Capture, `restic init`, staging or repository creation, `retention apply`, deleting or forgetting anything, Candybar
creation or edits, `activation` flags, off-host copies, the lossy derived tier, Holocene, and the `candystore export`
verb (the CandyStore export Emma reports it as a blocker). Each is a decision in EMMA-DESIGN.md section 9.

## Environment

`FOREVER_AGO_STATE_DIR`, `FOREVER_AGO_PJ_BIN` (default `pj`), `FOREVER_AGO_CANDYBAR_BIN`.
