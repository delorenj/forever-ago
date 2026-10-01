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

| pattern             | excludes                                                   |
| ------------------- | ---------------------------------------------------------- |
| `name`              | any path component named exactly `name` (`.venv`)          |
| `*.ext`             | files with that extension, case-insensitive (`*.pyc`)      |
| `na*e?`             | any path component matching `*` / `?` (`*.sync-conflict-*`) |
| `a/b/c`             | that subtree of the source root                            |

An excluded directory is pruned whole and never walked. `--exclude-from` reads one
pattern per line and skips blank lines and lines starting with `#`.

This is deliberately not a full glob engine. A pattern that could never match is a
startup error, so a typo fails the run loudly instead of quietly archiving what it
was meant to drop. Examples: wildcards inside `a/b/c`, a leading `/`, an empty line,
or a lone `*`.

## What goes into the archive

- Regular files are copied at the size they had when opened. If a file changes
  while it is being read, the run logs a WARN for it rather than writing a
  misaligned, corrupt archive.
- Symlinks are stored as links and never followed, including dangling ones.
- Paths that vanish mid-walk are logged and skipped. Any other read error fails the run.
- Sockets, FIFOs and devices are skipped with a WARN.

## Scheduling

### systemd timer (recommended)

```ini
# ~/.config/systemd/user/forever-ago-vault.service
[Service]
Type=oneshot
ExecStart=%h/.cargo/bin/forever-ago --source %h/vault --dest-dir %h/backups/vault \
    --prefix vault --keep-daily 7 --keep-weekly 4 --keep-monthly 4 --once

# ~/.config/systemd/user/forever-ago-vault.timer
[Timer]
OnCalendar=*-*-* 03:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
systemctl --user enable --now forever-ago-vault.timer
```

### Built-in daemon (e.g. under PM2)

Without `--once`, forever-ago stays running and backs up nightly at `--at` (default `03:00`, local time).
Add `--run-now` to also back up immediately on startup.

```bash
pm2 start "$(command -v forever-ago)" --name forever-ago -- \
  --source "$HOME/.openclaw" --prefix openclaw --retain 7
pm2 save
```

## Which jobs cover this directory?

```bash
forever-ago jobs          # jobs backing up the current directory, or its nearest ancestor up to ~
forever-ago jobs --all    # every job on the machine
```

forever-ago keeps no registry, so `jobs` finds invocations where the schedulers
keep them:

- systemd unit files (user and system, with drop-ins, timers, and whether they are enabled)
- your crontab, `/etc/crontab`, and `/etc/cron.d`
- the PM2 dump (`~/.pm2/dump.pm2`)
- forever-ago daemons that are running right now

Each invocation is re-parsed with forever-ago's own argument parser. For each
job it shows the schedule, next and last run (from systemd), destination,
retention, excludes, and the backups currently on disk. It also warns when the
current directory is excluded from that job, and when a job will never fire
(timer disabled, pm2 process stopped, or `--once` with nothing to trigger it).
