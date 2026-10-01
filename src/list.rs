//! `forever-ago list` — the snapshots that exist for this directory.
//!
//! An archive only names its source by basename, so the link from a
//! directory to its snapshots is the job that writes them: find the jobs
//! backing up the directory (see `jobs`), then list what sits in their
//! destinations under their prefixes.

use crate::jobs::{self, Job};
use crate::{human_size, parse_backup_date};
use anyhow::Result;
use chrono::{Local, NaiveDate};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    date: NaiveDate,
    size: u64,
    has_checksum: bool,
}

/// Snapshots of one job's destination, newest first. Jobs that write to the
/// same place (a timer and the daemon it is running right now) share one.
type Shelf = ((PathBuf, String), Vec<Snapshot>);

pub(crate) fn run() -> Result<()> {
    let found = jobs::discover_host(false)?;
    let cwd = std::env::current_dir()?;
    let today = Local::now().date_naive();
    print!("{}", report(&found.jobs, &cwd, &found.home, today));
    Ok(())
}

fn report(jobs: &[Job], cwd: &Path, home: &Path, today: NaiveDate) -> String {
    let cwd = jobs::canonical_or_normalized(cwd);
    let stop = jobs::canonical_or_normalized(home);
    let mut out = String::new();
    // Jobs between `cwd` and `floor` whose excludes leave `cwd` out: their
    // snapshots do not contain it, so the climb passed them by.
    let excluding = |floor: &Path| -> String {
        let skipped: Vec<(&Job, &str)> = jobs
            .iter()
            .filter(|j| cwd.starts_with(&j.source) && j.source.starts_with(floor))
            .filter_map(|j| jobs::excluded_by(j, &cwd).map(|p| (j, p)))
            .collect();
        jobs::excluding_note(&skipped, &cwd, home)
    };

    let Some((dir, shelves)) = jobs::climb(&cwd, &stop, |dir| {
        let shelves = shelves_for(jobs, dir, &cwd);
        (!shelves.is_empty()).then_some(shelves)
    }) else {
        let (limit, floor) = if cwd.starts_with(&stop) {
            (jobs::tilde(&stop, home), stop.clone())
        } else {
            ("/".to_string(), PathBuf::from("/"))
        };
        out.push_str(&format!(
            "no snapshots of {} (searched it and every parent up to {limit})\n",
            jobs::tilde(&cwd, home)
        ));
        let skipped = excluding(&floor);
        out.push_str(&skipped);
        // A job that has simply not run yet deserves a mention; otherwise
        // the answer to "where are my snapshots?" is to set one up.
        match jobs::covering(jobs, &cwd, &stop).found {
            Some((covered, found)) => {
                for job in found {
                    out.push_str(&format!(
                        "{} backs up {} but has not written a snapshot yet\n",
                        job.describe(),
                        jobs::tilde(&covered, home)
                    ));
                }
            }
            None if skipped.is_empty() => {
                out.push_str("no scheduled job backs it up either; see `forever-ago jobs --all`\n");
            }
            None => {}
        }
        return out;
    };

    if dir != cwd {
        out.push_str(&format!(
            "no snapshots of {} itself; nearest ancestor with snapshots: {}\n",
            jobs::tilde(&cwd, home),
            jobs::tilde(&dir, home)
        ));
    }
    out.push_str(&excluding(&dir));
    if !out.is_empty() {
        out.push('\n');
    }
    let single = shelves.len() == 1;
    for (i, ((dest, prefix), snaps)) in shelves.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        if single {
            out.push_str("Snapshots:\n");
        } else {
            out.push_str(&format!("Snapshots in {}:\n", location(dest, prefix, home)));
        }
        out.push_str(&table(snaps, today));
    }
    if single {
        let ((dest, prefix), _) = &shelves[0];
        out.push_str(&format!("\n{}\n", location(dest, prefix, home)));
    }
    out
}

fn location(dest: &Path, prefix: &str, home: &Path) -> String {
    format!("{}/{prefix}-YYYY-MM-DD.tar.gz", jobs::tilde(dest, home))
}

/// The jobs backing up exactly `dir`, as destinations that hold at least one
/// snapshot. A job that excludes `cwd` is skipped: its snapshots do not have it.
fn shelves_for(jobs: &[Job], dir: &Path, cwd: &Path) -> Vec<Shelf> {
    let mut places: BTreeMap<(PathBuf, String), ()> = BTreeMap::new();
    for job in jobs.iter().filter(|j| j.source == dir && jobs::excluded_by(j, cwd).is_none()) {
        if let Some(prefix) = &job.prefix {
            places.insert((job.dest_dir.clone(), prefix.clone()), ());
        }
    }
    places
        .into_keys()
        .map(|place| {
            let snaps = snapshots_in(&place.0, &place.1);
            (place, snaps)
        })
        .filter(|(_, snaps)| !snaps.is_empty())
        .collect()
}

/// Archives named `<prefix>-YYYY-MM-DD.tar.gz` in `dest`, newest first.
fn snapshots_in(dest: &Path, prefix: &str) -> Vec<Snapshot> {
    let Ok(rd) = fs::read_dir(dest) else { return Vec::new() };
    let mut snaps: Vec<Snapshot> = rd
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let date = parse_backup_date(prefix, &name)?;
            Some(Snapshot {
                date,
                size: e.metadata().map(|m| m.len()).unwrap_or(0),
                has_checksum: dest.join(format!("{name}.sha256")).is_file(),
            })
        })
        .collect();
    snaps.sort_by_key(|s| std::cmp::Reverse(s.date));
    snaps
}

/// ```text
/// 1   2026-09-15  (2w ago)       1.23GB
/// 2   2026-09-01  (1 month ago)  985M
/// ```
fn table(snaps: &[Snapshot], today: NaiveDate) -> String {
    let index_width = (snaps.len().to_string().len() + 1).max(4);
    let ages: Vec<String> = snaps.iter().map(|s| format!("({})", ago(s.date, today))).collect();
    let age_width = ages.iter().map(String::len).max().unwrap_or(0);
    let mut out = String::new();
    for (i, (snap, age)) in snaps.iter().zip(&ages).enumerate() {
        out.push_str(&format!(
            "{:<index_width$}{}  {age:<age_width$}  {}{}\n",
            i + 1,
            snap.date,
            human_size(snap.size),
            if snap.has_checksum { "" } else { "  (no checksum)" }
        ));
    }
    out
}

/// "today", "3d ago", "2w ago", "1 month ago", "2 years ago".
fn ago(date: NaiveDate, today: NaiveDate) -> String {
    let days = (today - date).num_days();
    let plural = |n: i64, unit: &str| format!("{n} {unit}{} ago", if n == 1 { "" } else { "s" });
    match days {
        ..0 => format!("in {}d", -days),
        0 => "today".to_string(),
        1..7 => format!("{days}d ago"),
        7..30 => format!("{}w ago", days / 7),
        30..365 => plural(days / 30, "month"),
        _ => plural(days / 365, "year"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;

    /// The exact example from the spec, as of 2026-10-01.
    #[test]
    fn table_matches_the_spec_example() {
        let snaps = vec![
            Snapshot { date: day("2026-09-15"), size: (1.23 * GIB as f64) as u64, has_checksum: true },
            Snapshot { date: day("2026-09-01"), size: 985 * MIB, has_checksum: true },
        ];
        assert_eq!(
            table(&snaps, day("2026-10-01")),
            "1   2026-09-15  (2w ago)       1.23GB\n2   2026-09-01  (1 month ago)  985M\n"
        );
    }

    #[test]
    fn ages() {
        let today = day("2026-10-01");
        let cases = [
            ("2026-10-01", "today"),
            ("2026-09-30", "1d ago"),
            ("2026-09-25", "6d ago"),
            ("2026-09-24", "1w ago"),
            ("2026-09-15", "2w ago"),
            ("2026-09-02", "4w ago"),
            ("2026-09-01", "1 month ago"),
            ("2026-07-31", "2 months ago"),
            ("2025-10-01", "1 year ago"),
            ("2023-09-01", "3 years ago"),
            ("2026-10-03", "in 2d"),
        ];
        for (date, want) in cases {
            assert_eq!(ago(day(date), today), want, "{date}");
        }
    }

    #[test]
    fn sizes() {
        assert_eq!(human_size(512), "512B");
        assert_eq!(human_size(12 * 1024), "12K");
        assert_eq!(human_size(985 * MIB), "985M");
        assert_eq!(human_size(330 * MIB + 123), "330M");
        assert_eq!(human_size(GIB), "1.00GB");
        assert_eq!(human_size(3 * 512 * GIB), "1.50TB");
    }

    #[test]
    fn index_column_widens_past_three_digits_only() {
        let snaps: Vec<Snapshot> = (0..12)
            .map(|i| Snapshot { date: day("2026-01-01") + chrono::Duration::days(i), size: MIB, has_checksum: i != 0 })
            .collect();
        let t = table(&snaps, day("2026-01-20"));
        assert!(t.starts_with("1   2026-01-01  (2w ago)  1M  (no checksum)\n"), "{t}");
        assert!(t.contains("\n12  2026-01-12  (1w ago)  1M\n"), "{t}");
    }

    struct Tree {
        _tmp: tempfile::TempDir,
        home: PathBuf,
    }

    impl Tree {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let home = fs::canonicalize(tmp.path()).unwrap().join("home");
            fs::create_dir_all(&home).unwrap();
            Self { _tmp: tmp, home }
        }

        fn dir(&self, rel: &str) -> PathBuf {
            let p = self.home.join(rel);
            fs::create_dir_all(&p).unwrap();
            p
        }

        fn archive(&self, dest: &str, name: &str, size: u64) {
            let f = fs::File::create(self.dir(dest).join(name)).unwrap();
            f.set_len(size).unwrap(); // sparse: costs no disk
            fs::write(self.home.join(dest).join(format!("{name}.sha256")), "x").unwrap();
        }

        fn job(&self, source: &str, dest: &str, prefix: &str) -> Job {
            jobs::test_job(
                &["forever-ago", "--source", source, "--dest-dir", dest, "--prefix", prefix, "--once"],
                &self.home,
                &self.home,
            )
        }
    }

    #[test]
    fn lists_snapshots_for_the_cwd() {
        let t = Tree::new();
        let vault = t.dir("vault");
        t.archive("backups", "vault-2026-09-15.tar.gz", (1.23 * GIB as f64) as u64);
        t.archive("backups", "vault-2026-09-01.tar.gz", 985 * MIB);
        // Neighbours that must not show up: another prefix, a temp file, a checksum.
        t.archive("backups", "vault-old-2026-09-20.tar.gz", MIB);
        fs::write(t.home.join("backups/vault-2026-09-30.tar.gz.tmp-99"), "x").unwrap();
        let jobs = vec![t.job("vault", "backups", "vault")];

        assert_eq!(
            report(&jobs, &vault, &t.home, day("2026-10-01")),
            "Snapshots:\n\
             1   2026-09-15  (2w ago)       1.23GB\n\
             2   2026-09-01  (1 month ago)  985M\n\
             \n\
             ~/backups/vault-YYYY-MM-DD.tar.gz\n"
        );
    }

    #[test]
    fn climbs_to_the_nearest_ancestor_with_snapshots() {
        let t = Tree::new();
        let deep = t.dir("code/vault/Notes/2026");
        t.dir("code/vault/Notes");
        t.archive("backups/code", "code-2026-09-30.tar.gz", MIB);
        let jobs = vec![
            // A job for vault/Notes that has never run: it has no snapshots, so keep climbing.
            t.job("code/vault/Notes", "backups/notes", "notes"),
            t.job("code", "backups/code", "code"),
        ];
        let out = report(&jobs, &deep, &t.home, day("2026-10-01"));
        assert!(
            out.starts_with("no snapshots of ~/code/vault/Notes/2026 itself; nearest ancestor with snapshots: ~/code\n\nSnapshots:\n1   2026-09-30  (1d ago)  1M\n"),
            "{out}"
        );
    }

    #[test]
    fn stops_at_home() {
        let t = Tree::new();
        let cwd = t.dir("projects/x");
        let parent = t.home.parent().unwrap().to_str().unwrap().to_string();
        t.archive("backups", "root-2026-09-30.tar.gz", MIB);
        let jobs = vec![t.job(&parent, "backups", "root")];
        let out = report(&jobs, &cwd, &t.home, day("2026-10-01"));
        assert_eq!(
            out,
            "no snapshots of ~/projects/x (searched it and every parent up to ~)\n\
             no scheduled job backs it up either; see `forever-ago jobs --all`\n"
        );
    }

    /// A job that excludes the cwd has no snapshots *of* it: skip to the next
    /// level up, and say which job was passed over.
    #[test]
    fn skips_snapshots_that_exclude_the_cwd() {
        let t = Tree::new();
        let cwd = t.dir("vault/node_modules/pkg");
        t.archive("backups/vault", "vault-2026-09-30.tar.gz", MIB);
        t.archive("backups/home", "home-2026-09-29.tar.gz", 2 * MIB);
        let excl = jobs::test_job(
            &["forever-ago", "--source", "vault", "--dest-dir", "backups/vault", "--prefix", "vault", "--once", "--exclude", "node_modules"],
            &t.home,
            &t.home,
        );
        let jobs = vec![excl, t.job(".", "backups/home", "home")];
        let out = report(&jobs, &cwd, &t.home, day("2026-10-01"));
        assert_eq!(
            out,
            "no snapshots of ~/vault/node_modules/pkg itself; nearest ancestor with snapshots: ~\n\
             test-job (cron) backs up ~/vault but excludes ~/vault/node_modules/pkg (`node_modules`)\n\
             \n\
             Snapshots:\n\
             1   2026-09-29  (2d ago)  2M\n\
             \n\
             ~/backups/home/home-YYYY-MM-DD.tar.gz\n"
        );

        // Nothing else has it: name the job that skips it instead of saying nothing backs it up.
        let only = vec![jobs.into_iter().next().unwrap()];
        let out = report(&only, &cwd, &t.home, day("2026-10-01"));
        assert!(out.contains("but excludes ~/vault/node_modules/pkg"), "{out}");
        assert!(!out.contains("no scheduled job backs it up"), "{out}");
    }

    #[test]
    fn mentions_a_job_that_has_not_run_yet() {
        let t = Tree::new();
        let vault = t.dir("vault");
        let jobs = vec![t.job("vault", "backups", "vault")];
        let out = report(&jobs, &vault, &t.home, day("2026-10-01"));
        assert!(out.contains("test-job (cron) backs up ~/vault but has not written a snapshot yet"), "{out}");
    }

    #[test]
    fn separate_destinations_get_separate_lists() {
        let t = Tree::new();
        let vault = t.dir("vault");
        t.archive("backups/local", "vault-2026-09-30.tar.gz", MIB);
        t.archive("mnt/usb", "vault-2026-09-29.tar.gz", 2 * MIB);
        let jobs = vec![
            t.job("vault", "backups/local", "vault"),
            t.job("vault", "mnt/usb", "vault"),
            // Same destination twice (say, a timer and its running daemon): listed once.
            t.job("vault", "backups/local", "vault"),
        ];
        let out = report(&jobs, &vault, &t.home, day("2026-10-01"));
        assert_eq!(
            out,
            "Snapshots in ~/backups/local/vault-YYYY-MM-DD.tar.gz:\n\
             1   2026-09-30  (1d ago)  1M\n\
             \n\
             Snapshots in ~/mnt/usb/vault-YYYY-MM-DD.tar.gz:\n\
             1   2026-09-29  (2d ago)  2M\n"
        );
    }
}
