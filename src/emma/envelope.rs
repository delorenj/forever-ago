//! The platform JSON envelope, diagnostics and exit codes.
//!
//! Every new subcommand's `--json` prints `{schema:2, command, ok, exit, data, findings}`
//! (the skillex / 33god-platform envelope). Diagnostic codes are `E_*` (error),
//! `W_*` (warning) and `I_*` (info).

use serde_json::{json, Value};

/// Exit codes from the platform table (33god-platform story 7.3).
pub mod exit {
    pub const OK: i32 = 0;
    pub const FAILURE: i32 = 1;
    pub const USAGE: i32 = 2;
    pub const REFUSED: i32 = 3;
    #[allow(dead_code)]
    pub const PARTIAL: i32 = 4;
    pub const LOCK_BUSY: i32 = 5;
    pub const DRIFT: i32 = 6;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
    Info,
}

impl Severity {
    fn label(self) -> &'static str {
        match self {
            Severity::Error => "error",
            Severity::Warning => "warning",
            Severity::Info => "info",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finding {
    pub code: String,
    pub severity: Severity,
    pub message: String,
    pub path: Option<String>,
    pub emma: Option<String>,
}

impl Finding {
    fn new(code: &str, message: impl Into<String>) -> Self {
        let severity = match code.as_bytes().first() {
            Some(b'W') => Severity::Warning,
            Some(b'I') => Severity::Info,
            _ => Severity::Error,
        };
        Finding { code: code.to_string(), severity, message: message.into(), path: None, emma: None }
    }

    /// Severity follows the code's prefix: `E_` error, `W_` warning, `I_` info.
    pub fn of(code: &str, message: impl Into<String>) -> Self {
        Finding::new(code, message)
    }

    pub fn at(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn emma(mut self, id: impl Into<String>) -> Self {
        self.emma = Some(id.into());
        self
    }

    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
    }

    pub fn to_json(&self) -> Value {
        json!({
            "code": self.code,
            "severity": self.severity.label(),
            "message": self.message,
            "path": self.path,
            "emma": self.emma,
        })
    }

    pub fn human(&self) -> String {
        let mut s = format!("{} {}: {}", self.severity.label(), self.code, self.message);
        if let Some(e) = &self.emma {
            s.push_str(&format!(" [emma {e}]"));
        }
        if let Some(p) = &self.path {
            s.push_str(&format!(" ({p})"));
        }
        s
    }
}

pub fn envelope(command: &str, exit: i32, data: Value, findings: &[Finding]) -> Value {
    json!({
        "schema": 2,
        "command": command,
        "ok": exit == exit::OK,
        "exit": exit,
        "data": data,
        "findings": findings.iter().map(Finding::to_json).collect::<Vec<_>>(),
    })
}

/// Print the result in the requested form and return the exit code.
/// Human mode writes the report to stdout and findings to stderr.
pub fn emit(command: &str, json_out: bool, exit: i32, data: Value, human: &str, findings: &[Finding]) -> i32 {
    if json_out {
        println!("{}", serde_json::to_string_pretty(&envelope(command, exit, data, findings)).unwrap_or_default());
    } else {
        print!("{human}");
        for f in findings {
            eprintln!("{}", f.human());
        }
    }
    exit
}

pub fn has_errors(findings: &[Finding]) -> bool {
    findings.iter().any(Finding::is_error)
}
