//! The Candybar join (atom `clock`): read-through only.
//!
//! Emma never schedules. A manifest names a Candybar by NAME; this module asks
//! `candybar show <name> --json` what Candybar knows (schedule, runner, last
//! invocation). Nothing here writes to Candybar.

use super::run_bounded;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::time::Duration;

const CANDYBAR_TIMEOUT: Duration = Duration::from_secs(10);

/// `candybar`, or `$FOREVER_AGO_CANDYBAR_BIN`.
pub fn candybar_bin() -> String {
    std::env::var("FOREVER_AGO_CANDYBAR_BIN").unwrap_or_else(|_| "candybar".into())
}

#[derive(Clone, Debug)]
pub enum Show {
    Found(Json),
    Missing,
    Unknown(String),
}

/// One `candybar show` per name per process.
pub struct Cache {
    bin: String,
    offline: bool,
    seen: HashMap<String, Show>,
}

impl Cache {
    pub fn new(bin: String, offline: bool) -> Self {
        Cache { bin, offline, seen: HashMap::new() }
    }

    pub fn show(&mut self, name: &str) -> Show {
        if self.offline {
            return Show::Unknown("--offline".into());
        }
        let bin = self.bin.clone();
        self.seen.entry(name.to_string()).or_insert_with(|| show(&bin, name)).clone()
    }
}

pub fn show(bin: &str, name: &str) -> Show {
    match run_bounded(bin, &["show", name, "--json"], None, CANDYBAR_TIMEOUT) {
        Err(e) => Show::Unknown(e),
        Ok(out) => {
            if out.code == Some(0)
                && let Ok(v) = serde_json::from_str::<Json>(&out.stdout)
            {
                return Show::Found(v);
            }
            if out.stderr.contains("no candybar named") || out.stdout.contains("no candybar named") {
                return Show::Missing;
            }
            Show::Unknown(format!("candybar show exited {:?}", out.code))
        }
    }
}

/// The join as `plan` and `projects` report it.
/// `expect_unit`: for adopted Emmas, the systemd unit the legacy job lives in.
pub fn join(cache: &mut Cache, candybar_ref: Option<&str>, expect_unit: Option<&str>) -> Json {
    let Some(name) = candybar_ref else {
        return json!({"status": "unscheduled", "ref": null, "note": "no [emmas.candybar] ref: runs only by hand"});
    };
    match cache.show(name) {
        Show::Missing => json!({"status": "missing", "ref": name}),
        Show::Unknown(why) => json!({"status": "unknown", "ref": name, "note": why}),
        Show::Found(v) => {
            let runner = v.get("runner").cloned().unwrap_or(Json::Null);
            let archived = v.get("archived").and_then(Json::as_bool).unwrap_or(false);
            let enabled = v.get("enabled").and_then(Json::as_bool).unwrap_or(false);
            let mismatch = expect_unit.is_some_and(|unit| {
                runner.get("kind").and_then(Json::as_str) != Some("systemd") || runner.get("unit").and_then(Json::as_str) != Some(unit)
            });
            let status = if archived {
                "archived"
            } else if mismatch {
                "runner-mismatch"
            } else if !enabled {
                "disabled"
            } else {
                "ok"
            };
            let last = v.get("invocations").and_then(Json::as_array).and_then(|a| a.first()).map(|i| {
                json!({
                    "id": i.get("id"),
                    "result": i.get("result"),
                    "exit_code": i.get("exitCode"),
                    "trigger": i.get("trigger"),
                    "started_at": i.get("startedAt"),
                    "finished_at": i.get("finishedAt"),
                    "runtime_ms": i.get("runtimeMs"),
                })
            });
            json!({
                "status": status,
                "ref": name,
                "id": v.get("id"),
                "schedule": v.get("schedule"),
                "timezone": v.get("timezone"),
                "enabled": enabled,
                "archived": archived,
                "runner": runner,
                "expected_unit": expect_unit,
                "total_invoked": v.get("totalInvoked"),
                "total_successful": v.get("totalSuccessful"),
                "last_invocation": last,
            })
        }
    }
}
