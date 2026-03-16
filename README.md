# forever-ago

Nightly `tar.gz` backups of a directory with SHA-256 verification and retention pruning.

## Install

```bash
cargo install forever-ago
```

## One-shot run (manual test)

```bash
forever-ago \
  --source $HOME/.openclaw \
  --dest-dir $HOME/backups \
  --prefix openclaw-$(`hostname`) \
  --retain 7 \
  --once
```

This produces:

`$HOME/backups/openclaw-myhost-YYYY-MM-DD.tar.gz`
and
`$HOME/backups/openclaw-myhost-YYYY-MM-DD.tar.gz.sha256`

## Run as a service via PM2

```bash
pm2 start forever-ago/pm2/ecosystem.config.cjs
pm2 logs forever-ago
pm2 save
pm2 startup   # follow the printed instructions
```

Defaults in the PM2 config:

- `cwd`: `~/.openclaw` (so `--source .` backs up this directory)
- destination: `~/backups`
- schedule: `03:00` EST local time
- retention: keep newest 7 backups
