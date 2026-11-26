//! Retention planning (EMMA-DESIGN 5.4). Pure functions: no I/O, no deletion.
//!
//! `age-buckets@1` is frozen as specified; any change is `age-buckets@2`.
//! There is no `apply` in this version: a plan carries no authority.

use crate::{select_retained, RetentionPolicy};
use chrono::{DateTime, Datelike, NaiveDate, SecondsFormat, Utc};
use serde_json::{json, Value as Json};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};

pub const DAY: i64 = 86_400;

/// The longest `--simulate-daily` what-if: a century of daily snapshots.
pub const MAX_SIMULATE_DAYS: u32 = 36_525;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapStatus {
    Success,
    Incomplete,
    Failed,
}

impl SnapStatus {
    pub fn label(self) -> &'static str {
        match self {
            SnapStatus::Success => "success",
            SnapStatus::Incomplete => "incomplete",
            SnapStatus::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "success" => Some(SnapStatus::Success),
            "incomplete" => Some(SnapStatus::Incomplete),
            "failed" => Some(SnapStatus::Failed),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Snap {
    pub id: String,
    pub captured_at: DateTime<Utc>,
    /// The archive's own date (legacy tar filename), else the UTC date of `captured_at`.
    pub date: NaiveDate,
    pub status: SnapStatus,
    pub hold: Option<String>,
    pub generation: i64,
    pub fidelity: String,
    pub time_source: &'static str,
}

impl Snap {
    pub fn new(id: impl Into<String>, captured_at: DateTime<Utc>) -> Self {
        Snap {
            id: id.into(),
            date: captured_at.date_naive(),
            captured_at,
            status: SnapStatus::Success,
            hold: None,
            generation: 1,
            fidelity: "full@1".into(),
            time_source: "captured",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Decision {
    pub id: String,
    pub captured_at: DateTime<Utc>,
    pub keep: bool,
    pub reasons: Vec<String>,
    /// 0..=3 for age-buckets bands; None for future-dated or non-banded plans.
    pub band: Option<usize>,
}

pub const BANDS: [(&str, &str); 4] = [
    ("dense", "[0,14d)"),
    ("3-day", "[14d,90d)"),
    ("iso-week", "[90d,365d)"),
    ("month", ">=365d"),
];

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BandSummary {
    pub members: usize,
    pub kept: usize,
    pub forgotten: usize,
    /// Longest span between consecutive kept points whose OLDER point is in
    /// this band, so the edge into the next-newer band is counted.
    pub max_gap_s: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartitionPlan {
    pub generation: i64,
    pub frozen: bool,
    pub decisions: Vec<Decision>,
    pub bands: Vec<BandSummary>,
    pub max_gap_s: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub strategy: String,
    pub as_of: DateTime<Utc>,
    pub partitions: Vec<PartitionPlan>,
    /// Never selected, pinned or forgotten: incomplete/failed/lossy snapshots.
    pub untouched: Vec<(String, String)>,
    pub executed_by: &'static str,
    pub notes: Vec<String>,
    pub digest: String,
}

fn band_of(age_s: i64) -> usize {
    if age_s < 14 * DAY {
        0
    } else if age_s < 90 * DAY {
        1
    } else if age_s < 365 * DAY {
        2
    } else {
        3
    }
}

fn unix_day(t: &DateTime<Utc>) -> i64 {
    t.timestamp().div_euclid(DAY)
}

fn bucket_key(band: usize, t: &DateTime<Utc>) -> String {
    match band {
        1 => format!("bucket3d:{}", unix_day(t).div_euclid(3)),
        2 => {
            let w = t.iso_week();
            format!("week:{}-W{:02}", w.year(), w.week())
        }
        _ => format!("month:{}", t.format("%Y-%m")),
    }
}

/// Split by eligibility, then by partition (generation). Snapshots in
/// different partitions are never compared.
fn partition(snaps: &[Snap]) -> (BTreeMap<i64, Vec<Snap>>, Vec<(String, String)>) {
    let mut parts: BTreeMap<i64, Vec<Snap>> = BTreeMap::new();
    let mut untouched = Vec::new();
    for s in snaps {
        if s.status != SnapStatus::Success {
            untouched.push((s.id.clone(), format!("status:{}", s.status.label())));
        } else if s.fidelity != "full@1" {
            untouched.push((s.id.clone(), format!("fidelity:{}", s.fidelity)));
        } else {
            parts.entry(s.generation).or_default().push(s.clone());
        }
    }
    untouched.sort();
    (parts, untouched)
}

/// Newest wins: greatest captured_at, a tie broken by the greatest id.
fn newer(a: &Snap, b: &Snap) -> bool {
    (a.captured_at, &a.id) > (b.captured_at, &b.id)
}

pub fn age_buckets_partition(generation: i64, snaps: &[Snap], as_of: DateTime<Utc>) -> PartitionPlan {
    let mut dec: HashMap<String, Decision> = HashMap::new();
    let mut banded: [Vec<&Snap>; 4] = Default::default();
    for s in snaps {
        let age = (as_of - s.captured_at).num_seconds();
        if s.captured_at > as_of {
            dec.insert(
                s.id.clone(),
                Decision { id: s.id.clone(), captured_at: s.captured_at, keep: true, reasons: vec!["review-future-dated".into()], band: None },
            );
        } else {
            banded[band_of(age)].push(s);
        }
    }
    let frozen = banded[0].is_empty();
    for (band, members) in banded.iter().enumerate() {
        if frozen {
            for s in members {
                dec.insert(s.id.clone(), Decision { id: s.id.clone(), captured_at: s.captured_at, keep: true, reasons: vec!["frozen".into()], band: Some(band) });
            }
            continue;
        }
        if band == 0 {
            for s in members {
                dec.insert(s.id.clone(), Decision { id: s.id.clone(), captured_at: s.captured_at, keep: true, reasons: vec!["band0-all".into()], band: Some(0) });
            }
            continue;
        }
        // A band only groups its own members.
        let mut winners: HashMap<String, &Snap> = HashMap::new();
        for s in members {
            let k = bucket_key(band, &s.captured_at);
            match winners.get(&k) {
                Some(w) if !newer(s, w) => {}
                _ => {
                    winners.insert(k, s);
                }
            }
        }
        for s in members {
            let k = bucket_key(band, &s.captured_at);
            let w = winners[&k];
            let d = if w.id == s.id {
                Decision { id: s.id.clone(), captured_at: s.captured_at, keep: true, reasons: vec![k], band: Some(band) }
            } else {
                Decision { id: s.id.clone(), captured_at: s.captured_at, keep: false, reasons: vec![format!("superseded-by:{}", w.id)], band: Some(band) }
            };
            dec.insert(s.id.clone(), d);
        }
    }

    // Independent pins no band can override.
    let pin = |d: &mut Decision, why: String| {
        if !d.keep {
            d.keep = true;
            d.reasons.clear();
        }
        d.reasons.push(why);
    };
    if let Some(latest) = snaps.iter().filter(|s| s.captured_at <= as_of).reduce(|a, b| if newer(b, a) { b } else { a })
        && let Some(d) = dec.get_mut(&latest.id)
    {
        pin(d, "pin-latest".into());
    }
    for s in snaps {
        if let Some(h) = &s.hold
            && let Some(d) = dec.get_mut(&s.id)
        {
            pin(d, format!("hold:{h}"));
        }
    }
    // The last known-good restore point is a third pin; no restore proof
    // exists in this version, so it pins nothing yet.

    let mut decisions: Vec<Decision> = dec.into_values().collect();
    for d in &mut decisions {
        d.reasons.sort();
        d.reasons.dedup();
    }
    decisions.sort_by(|a, b| (b.captured_at, &b.id).cmp(&(a.captured_at, &a.id)));
    let (bands, max_gap_s) = summarize(&decisions, true);
    PartitionPlan { generation, frozen, decisions, bands, max_gap_s }
}

/// Band counts and the longest gap the kept set leaves.
fn summarize(decisions: &[Decision], banded: bool) -> (Vec<BandSummary>, Option<i64>) {
    let mut bands = vec![BandSummary::default(); if banded { 4 } else { 1 }];
    for d in decisions {
        let Some(b) = (if banded { d.band } else { Some(0) }) else { continue };
        bands[b].members += 1;
        if d.keep {
            bands[b].kept += 1;
        } else {
            bands[b].forgotten += 1;
        }
    }
    // Oldest first, future-dated excluded (they are not restore points yet).
    let mut kept: Vec<&Decision> = decisions.iter().filter(|d| d.keep && (!banded || d.band.is_some())).collect();
    kept.sort_by_key(|d| (d.captured_at, d.id.clone()));
    let mut overall = None;
    for pair in kept.windows(2) {
        let gap = (pair[1].captured_at - pair[0].captured_at).num_seconds();
        let b = if banded { pair[0].band.unwrap_or(0) } else { 0 };
        bands[b].max_gap_s = Some(bands[b].max_gap_s.map_or(gap, |g: i64| g.max(gap)));
        overall = Some(overall.map_or(gap, |g: i64| g.max(gap)));
    }
    (bands, overall)
}

fn flat_partition(generation: i64, snaps: &[Snap], keep: &HashSet<String>, keep_reason: &str, forget_reason: &str) -> PartitionPlan {
    let mut decisions: Vec<Decision> = snaps
        .iter()
        .map(|s| {
            let k = keep.contains(&s.id);
            Decision {
                id: s.id.clone(),
                captured_at: s.captured_at,
                keep: k,
                reasons: vec![if k { keep_reason.to_string() } else { forget_reason.to_string() }],
                band: None,
            }
        })
        .collect();
    decisions.sort_by(|a, b| (b.captured_at, &b.id).cmp(&(a.captured_at, &a.id)));
    let (bands, max_gap_s) = summarize(&decisions, false);
    PartitionPlan { generation, frozen: false, decisions, bands, max_gap_s }
}

/// The legacy rule exactly as the legacy prune applies it: snapshots sorted
/// by archive date, `--retain N` keeps the newest N, GFS goes through
/// `select_retained`.
fn legacy_partition(generation: i64, snaps: &[Snap], policy: &RetentionPolicy) -> PartitionPlan {
    let mut by_date: Vec<(NaiveDate, String)> = snaps.iter().map(|s| (s.date, s.id.clone())).collect();
    by_date.sort();
    let (keep, rule) = match policy {
        RetentionPolicy::Count(n) => {
            let skip = by_date.len().saturating_sub(*n);
            (by_date.iter().skip(skip).map(|(_, id)| id.clone()).collect::<HashSet<_>>(), format!("newest-{n}"))
        }
        RetentionPolicy::Gfs { daily, weekly, monthly } => {
            (select_retained(&by_date, *daily, *weekly, *monthly), format!("gfs-{daily}d-{weekly}w-{monthly}m"))
        }
    };
    flat_partition(generation, snaps, &keep, &format!("legacy:{rule}"), &format!("legacy:beyond-{rule}"))
}

/// RFC 3339 UTC with every non-zero sub-second digit, so a recorded `as_of`
/// replays to the same plan (whole seconds print as before).
fn ts(t: &DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// Plan one strategy over an inventory. `Err` is a refusal message.
pub fn plan(strategy: &str, params: &Json, snaps: &[Snap], as_of: DateTime<Utc>) -> Result<Plan, String> {
    // Decisions are keyed by id: a repeated id would silently fold two
    // restore points into one decision (and make the digest order-dependent).
    let mut ids = HashSet::new();
    if let Some(dup) = snaps.iter().find(|s| !ids.insert(s.id.as_str())) {
        return Err(format!("snapshot id {:?} appears more than once in the inventory; refusing to plan", dup.id));
    }
    let legacy = strategy.starts_with("legacy-");
    // Legacy rules count every archive, as the legacy prune does; the other
    // strategies only ever select among successful full-fidelity snapshots.
    let (parts, untouched) = if legacy {
        let mut p: BTreeMap<i64, Vec<Snap>> = BTreeMap::new();
        for s in snaps {
            p.entry(s.generation).or_default().push(s.clone());
        }
        (p, Vec::new())
    } else {
        partition(snaps)
    };
    let mut notes = Vec::new();
    let (partitions, executed_by): (Vec<PartitionPlan>, &'static str) = match strategy {
        "age-buckets@1" => (parts.iter().map(|(g, s)| age_buckets_partition(*g, s, as_of)).collect(), "emma (no apply in this version)"),
        "keep-all@1" | "observe-only@1" => {
            let label = strategy.trim_end_matches("@1");
            (
                parts
                    .iter()
                    .map(|(g, s)| flat_partition(*g, s, &s.iter().map(|x| x.id.clone()).collect(), label, label))
                    .collect(),
                "emma (keeps everything)",
            )
        }
        "legacy-count@1" => {
            let n = params.get("keep").and_then(Json::as_i64).filter(|n| *n >= 1).ok_or("legacy-count@1 needs keep >= 1")?;
            let policy = RetentionPolicy::Count(n as usize);
            notes.push("executed by the legacy job; Emma will not act".into());
            (parts.iter().map(|(g, s)| legacy_partition(*g, s, &policy)).collect(), "legacy job")
        }
        "legacy-gfs@1" => {
            let get = |k: &str, d: i64| params.get(k).and_then(Json::as_i64).unwrap_or(d).max(0) as usize;
            let (daily, weekly, monthly) = (get("daily", 7), get("weekly", 4), get("monthly", 4));
            if daily == 0 && weekly == 0 && monthly == 0 {
                return Err("legacy-gfs@1 with all tiers 0 would delete every backup".into());
            }
            let policy = RetentionPolicy::Gfs { daily, weekly, monthly };
            notes.push("executed by the legacy job; Emma will not act".into());
            (parts.iter().map(|(g, s)| legacy_partition(*g, s, &policy)).collect(), "legacy job")
        }
        other => return Err(format!("unknown retention strategy {other:?}; refusing to plan")),
    };
    if partitions.iter().any(|p| p.frozen) {
        notes.push("frozen: the [0,14d) band holds no eligible snapshot, so everything is kept (a stalled chain never ages its only history away)".into());
    }
    let mut p = Plan { strategy: strategy.to_string(), as_of, partitions, untouched, executed_by, notes, digest: String::new() };
    p.digest = digest(&p);
    Ok(p)
}

/// SHA-256 over canonical JSON of strategy, as_of, partitions and decisions,
/// sorted so the same inputs in any order give the same digest.
pub fn digest(p: &Plan) -> String {
    let mut decisions: Vec<Json> = p
        .partitions
        .iter()
        .flat_map(|part| part.decisions.iter().map(move |d| json!({"generation": part.generation, "id": d.id, "keep": d.keep, "reasons": d.reasons})))
        .collect();
    decisions.sort_by_key(|d| (d["generation"].as_i64(), d["id"].as_str().map(str::to_string)));
    let mut untouched = p.untouched.clone();
    untouched.sort();
    let canon = json!({
        "strategy": p.strategy,
        "as_of": ts(&p.as_of),
        "partitions": p.partitions.iter().map(|x| json!({"generation": x.generation, "fidelity": "full@1"})).collect::<Vec<_>>(),
        "decisions": decisions,
        "untouched": untouched,
    });
    hex::encode(Sha256::digest(canon.to_string().as_bytes()))
}

pub fn days(s: i64) -> i64 {
    s.div_euclid(DAY)
}

pub fn to_json(p: &Plan, what_if: bool) -> Json {
    let parts: Vec<Json> = p
        .partitions
        .iter()
        .map(|part| {
            let bands: Vec<Json> = part
                .bands
                .iter()
                .enumerate()
                .map(|(i, b)| {
                    let (name, range) = if part.bands.len() == 4 { BANDS[i] } else { ("all", "all") };
                    json!({
                        "band": name, "range": range, "members": b.members, "kept": b.kept, "forgotten": b.forgotten,
                        "max_gap_s": b.max_gap_s, "max_gap_days": b.max_gap_s.map(days),
                    })
                })
                .collect();
            json!({
                "generation": part.generation,
                "fidelity": "full@1",
                "frozen": part.frozen,
                "kept": part.decisions.iter().filter(|d| d.keep).count(),
                "forgotten": part.decisions.iter().filter(|d| !d.keep).count(),
                "max_gap_s": part.max_gap_s,
                "max_gap_days": part.max_gap_s.map(days),
                "bands": bands,
                "decisions": part.decisions.iter().map(|d| json!({
                    "id": d.id, "captured_at": ts(&d.captured_at),
                    "decision": if d.keep { "keep" } else { "forget" }, "reasons": d.reasons,
                    "band": d.band.map(|b| BANDS[b].0),
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({
        "strategy": p.strategy,
        "as_of": ts(&p.as_of),
        "what_if": what_if,
        "apply_authority": false,
        "apply_note": if what_if { "what-if plan over a synthetic or supplied inventory: it carries no apply authority" } else { "there is no `retention apply` in this version" },
        "executed_by": p.executed_by,
        "partitions": parts,
        "untouched": p.untouched.iter().map(|(id, why)| json!({"id": id, "reason": why})).collect::<Vec<_>>(),
        "notes": p.notes,
        "plan_digest": p.digest,
    })
}

/// One successful full-fidelity snapshot per day for `n` days ending at
/// `as_of`, captured at 06:00 UTC (the golden-table fixture).
pub fn simulate_daily(n: u32, as_of: DateTime<Utc>) -> Vec<Snap> {
    // The newest 06:00 UTC at or before as_of, then one per day before it.
    let mut first = as_of.date_naive().and_hms_opt(6, 0, 0).expect("06:00 exists").and_utc();
    if first > as_of {
        first -= chrono::Duration::days(1);
    }
    (0..n as i64)
        .map(|i| {
            let t = first - chrono::Duration::days(i);
            let mut s = Snap::new(format!("whatif-{}", t.format("%Y-%m-%dT%H%MZ")), t);
            s.time_source = "simulated";
            s
        })
        .collect()
}
