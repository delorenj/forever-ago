# forever-ago

Nightly `tar.gz` backups of a directory with SHA-256 verification, retention pruning, and path excludes.

## Install

```bash
cargo install forever-ago
```

## One-shot run (manual test)

```bash
forever-ago \
  --source $HOME/.openclaw \
  --dest-dir $HOME/backups \
  --prefix openclaw-$(hostname) \
  --retain 7 \
  --once
```

This produces:

`$HOME/backups/openclaw-myhost-YYYY-MM-DD.tar.gz`
and
`$HOME/backups/openclaw-myhost-YYYY-MM-DD.tar.gz.sha256`

The archive is written to a temp file, re-hashed, and only then moved into place.
Old backups are pruned only after that succeeds, and the backup a run just wrote
is never pruned.

## Retention

Pick one:

- `--retain N` (default 7): keep the newest N backups.
- `--keep-daily D --keep-weekly W --keep-monthly M`: grandfather-father-son.
  Keeps the D newest backups, plus the newest backup in each of the W most recent
  ISO weeks, plus the newest in each of the M most recent months. Buckets, not
  calendar days: a week whose Sunday run was missed still keeps its newest backup.
  Any `--keep-*` flag switches to GFS; tiers you leave out default to 7 / 4 / 4.

The two cannot be combined, and a policy that would keep nothing
(`--retain 0`, or all three tiers 0) is refused.

## Excludes

```bash
forever-ago ... --exclude node_modules --exclude '*.pyc' --exclude-from ./backup-exclude
```

Patterns match each entry's path relative to the source root:

| pattern             | excludes                                                    |
| ------------------- | ----------------------------------------------------------- |
| `name`              | any path component named exactly `name` (`.venv`)           |
| `./name`            | `name` at the source root only                              |
| `*.ext`             | files with that extension, case-insensitive (`*.pyc`)       |
| `*.ext/`            | directories with that extension (`*.egg-info/`)             |
| `na*e?`             | any path component matching `*` / `?` (`*.sync-conflict-*`) |
| `a/b/c`             | that subtree of the source root                             |

An excluded directory is pruned whole and never walked. `--exclude-from` reads one
pattern per line and skips blank lines and lines starting with `#`.

This is deliberately not a full glob engine. A pattern that could never match is a
startup error, so a typo fails the run loudly instead of quietly archiving what it
was meant to drop. Examples: wildcards inside `a/b/c`, a leading `/`, an empty line,
or a lone `*`. If a `*.ext` pattern meets a directory with that extension, the run
keeps the directory and logs a WARN suggesting `*.ext/`.

## What goes into the archive

- Regular files are copied at the size they had when opened, so a file that
  changes mid-read can never misalign the archive. Changes are detected by size,
  mtime and ctime, and each one is logged as a WARN. A write through a shared mmap
  can slip past all three.
- Symlinks are stored as links and never followed, including dangling ones.
- Paths that vanish mid-walk are logged and skipped. Any other read error fails the run.
- Sockets, FIFOs and devices are skipped with a WARN.

## Scheduling

**On this host, Candybar is the only clock.** Do not add a systemd timer, a cron entry, a PM2 process or the
built-in daemon loop for a forever-ago job: register a Candybar whose runner starts
`forever-ago ... --once` (see the `candybar` skill and `~/code/33GOD/candybar/` docs). A Candybar owns *when*,
the run history and the receipts; forever-ago owns *what* is captured and how it is thinned. The two live jobs
(`forever-ago-vault`, `forever-ago-voxxy-voices`) already run that way.

`--once` is the invocation a Candybar should use: one backup, then exit.

```bash
forever-ago --source ~/vault --dest-dir ~/backups/vault --prefix vault --retain 7 --once
```

Without `--once`, forever-ago stays running and backs up nightly at `--at` (default `03:00`, local time; `--run-now`
also backs up on startup). That is a private clock. It remains for hosts that have no Candybar; on a host that
does, `forever-ago adopt` skips such a job and says so (`W_LEGACY_DAEMON_CLOCK`).

## Which snapshots exist for this directory?

```console
$ cd ~/code/DeLoDocs && forever-ago list
Snapshots:
1   2026-10-01  (today)         330M
2   2026-09-30  (1d ago)        329M
...
10  2026-08-31  (1 month ago)   256M
11  2026-07-31  (2 months ago)  236M

~/backups/delodocs/vault-YYYY-MM-DD.tar.gz
```

Snapshots are found through the jobs that write them (see below), newest first.
If the current directory has none of its own, `list` climbs to the nearest
ancestor (stopping at `~`) that does. Snapshots missing their `.sha256` are marked.

## Which jobs cover this directory?

```bash
forever-ago jobs          # jobs backing up the current directory, or its nearest ancestor up to ~
forever-ago jobs --all    # every job on the machine
```

forever-ago keeps no registry, so `jobs` finds invocations where the schedulers
keep them:

- systemd units, user and system. It uses systemd's own search path (including
  `systemd-run` transient units) and handles template instances, drop-ins,
  `Environment=`/`EnvironmentFile=` and specifiers. Units can be started by a
  timer, a path unit, or a target that wants them.
- your crontab, `/etc/crontab`, and `/etc/cron.d`, read as a shell would: `cd X &&`,
  `sh -c`, wrappers like `nice`/`flock`/`timeout`, `$VAR`, and `$(hostname)`
- the PM2 dump (`~/.pm2/dump.pm2`)
- forever-ago daemons that are running right now

Each invocation is re-parsed with forever-ago's own argument parser. For each
job it shows the schedule, next and last run (from systemd), destination,
retention, excludes, and the backups currently on disk. It also warns when the
current directory is excluded from that job, and when a job will never fire
(timer disabled, pm2 process stopped, or `--once` with nothing to trigger it).

A job whose excludes leave the current directory out does not count as covering
it, so `jobs` and `list` keep climbing past it and say which job they skipped.

## Emma: project-declared backups

A project declares what it wants backed up in a `.emma.toml` at its root. Forever Ago validates it, records it in
a derived local registry, and plans it. Version 0.4.0 is **observe-only**: nothing in `register`, `adopt`,
`projects`, `plan` or `retention plan` captures, prunes, restores or schedules anything, and none of them calls the
legacy backup path. The legacy flags above behave exactly as before.

```bash
forever-ago adopt [--write]              # print (or create) the .emma.toml entry that describes each live job
forever-ago register [PATH|--all] [--dry-run] [--update] [--prune]
forever-ago projects [--project ID] [--json]     # every Emma: schedule, snapshots, independence, coverage
forever-ago plan <project/emma> [--measure] [--json]
forever-ago retention plan <project/emma> [--simulate-daily N] [--as-of RFC3339] [--json]
```

Full reference: [docs/emma.md](docs/emma.md). Design and decisions: [docs/EMMA-DESIGN.md](docs/EMMA-DESIGN.md).
