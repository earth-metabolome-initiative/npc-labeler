use crate::db::{self, LabelCount};
use chrono::Local;
use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::queue;
use crossterm::style::{Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor};
use crossterm::terminal::{
    BeginSynchronizedUpdate, Clear, ClearType, DisableLineWrap, EnableLineWrap,
    EndSynchronizedUpdate, EnterAlternateScreen, LeaveAlternateScreen, size,
};
use std::collections::VecDeque;
use std::io::{self, IsTerminal, Write};
use std::time::{Duration, Instant};

const MAX_EVENTS: usize = 10;
const MAX_ERRORS: usize = 5;
const CACHE_TTL: Duration = Duration::from_mins(1);

struct DbCache {
    total: i64,
    classified: i64,
    empty: i64,
    invalid: i64,
    failed: i64,
    pending: i64,
    top_pathways: Vec<LabelCount>,
    top_superclasses: Vec<LabelCount>,
    top_classes: Vec<LabelCount>,
    last_refresh: Instant,
}

pub struct Ui {
    interactive: bool,
    started_at: Instant,
    current_smiles: Option<String>,
    current_cid: Option<i32>,
    last_result: Option<String>,
    session_requests: u64,
    session_classified: u64,
    session_empty: u64,
    session_invalid: u64,
    session_errors: u64,
    recent_events: VecDeque<String>,
    recent_errors: VecDeque<String>,
    ntfy_url: String,
    cache: DbCache,
}

pub struct TerminalGuard {
    active: bool,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut stderr = io::stderr();
        let _ = crossterm::execute!(stderr, Show, EnableLineWrap, LeaveAlternateScreen);
    }
}

impl Ui {
    pub fn new(ntfy_url: String) -> Self {
        Self {
            interactive: io::stderr().is_terminal(),
            started_at: Instant::now(),
            current_smiles: None,
            current_cid: None,
            last_result: None,
            session_requests: 0,
            session_classified: 0,
            session_empty: 0,
            session_invalid: 0,
            session_errors: 0,
            recent_events: VecDeque::new(),
            recent_errors: VecDeque::new(),
            ntfy_url,
            cache: DbCache {
                total: 0,
                classified: 0,
                empty: 0,
                invalid: 0,
                failed: 0,
                pending: 0,
                top_pathways: Vec::new(),
                top_superclasses: Vec::new(),
                top_classes: Vec::new(),
                last_refresh: Instant::now().checked_sub(CACHE_TTL).unwrap(),
            },
        }
    }

    pub fn enter_terminal(&self) -> Option<TerminalGuard> {
        if !self.interactive {
            return None;
        }
        let mut stderr = io::stderr();
        crossterm::execute!(stderr, EnterAlternateScreen, DisableLineWrap, Hide).ok()?;
        Some(TerminalGuard { active: true })
    }

    pub fn note_current(&mut self, cid: i32, smiles: &str) {
        self.current_cid = Some(cid);
        self.current_smiles = Some(if smiles.len() > 60 {
            format!("{}...", &smiles[..57])
        } else {
            smiles.to_string()
        });
        self.session_requests += 1;
    }

    pub fn note_classified(&mut self, cid: i32) {
        self.session_classified += 1;
        let msg = format!("classified CID {cid}");
        self.last_result = Some(msg.clone());
        push_ring(&mut self.recent_events, MAX_EVENTS, &msg);
    }

    pub fn note_empty(&mut self, cid: i32) {
        self.session_empty += 1;
        let msg = format!("empty CID {cid}");
        self.last_result = Some(msg.clone());
        push_ring(&mut self.recent_events, MAX_EVENTS, &msg);
    }

    pub fn note_invalid(&mut self, cid: i32) {
        self.session_invalid += 1;
        let msg = format!("invalid CID {cid}");
        self.last_result = Some(msg.clone());
        push_ring(&mut self.recent_events, MAX_EVENTS, &msg);
    }

    pub fn note_error(&mut self, cid: i32, error: &str) {
        self.session_errors += 1;
        let msg = format!("error CID {cid}: {error}");
        self.last_result = Some(format!("error CID {cid}"));
        push_ring(&mut self.recent_errors, MAX_ERRORS, &msg);
    }

    pub fn note_rate_limit(&mut self, cid: i32) {
        let msg = format!("rate limited CID {cid}, sleeping 30s");
        push_ring(&mut self.recent_events, MAX_EVENTS, &msg);
    }

    fn refresh_cache(&mut self, conn: &mut diesel::SqliteConnection) {
        if self.cache.last_refresh.elapsed() < CACHE_TTL {
            return;
        }
        self.cache.total = db::count_total(conn);
        self.cache.classified = db::count_by_status(conn, "classified");
        self.cache.empty = db::count_by_status(conn, "empty");
        self.cache.invalid = db::count_by_status(conn, "invalid");
        self.cache.failed = db::count_by_status(conn, "failed");
        self.cache.pending = db::count_by_status(conn, "pending");
        self.cache.top_pathways = db::top_labels(conn, "pathway_results", 5);
        self.cache.top_superclasses = db::top_labels(conn, "superclass_results", 5);
        self.cache.top_classes = db::top_labels(conn, "class_results", 5);
        self.cache.last_refresh = Instant::now();
    }

    pub fn render(&mut self, conn: &mut diesel::SqliteConnection) {
        self.refresh_cache(conn);

        if !self.interactive {
            let uptime = self.started_at.elapsed().as_secs().max(1);
            let rate = self.session_requests as f64 / uptime as f64;
            let done =
                self.cache.classified + self.cache.empty + self.cache.invalid + self.cache.failed;
            let pct = done as f64 / self.cache.total.max(1) as f64 * 100.0;
            eprintln!(
                "[progress] {done}/{} ({pct:.1}%) | {rate:.1}/s | classified={} empty={} invalid={} errors={}",
                self.cache.total,
                self.session_classified,
                self.session_empty,
                self.session_invalid,
                self.session_errors
            );
            return;
        }

        let _ = self.render_dashboard();
    }

    fn render_dashboard(&self) -> io::Result<()> {
        let uptime = self.started_at.elapsed().as_secs().max(1);
        let rate = self.session_requests as f64 / uptime as f64;
        let (width, height) = size().unwrap_or((120, 36));

        // Live counts: cached baseline + session deltas
        let total = self.cache.total;
        let classified = self.cache.classified + self.session_classified as i64;
        let empty = self.cache.empty + self.session_empty as i64;
        let invalid = self.cache.invalid + self.session_invalid as i64;
        let failed = self.cache.failed;
        let pending = self.cache.pending
            - self.session_classified as i64
            - self.session_empty as i64
            - self.session_invalid as i64;

        let mut lines = vec![
            format!(
                "NPClassifier scraper    {}",
                Local::now().format("%Y-%m-%d %H:%M:%S")
            ),
            format!("uptime={}s | req_rate={:.2}/s", uptime, rate),
            format!(
                "current: CID {} | {}",
                self.current_cid
                    .map_or("idle".to_string(), |c| c.to_string()),
                self.current_smiles.as_deref().unwrap_or(""),
            ),
            self.last_result.as_ref().map_or_else(
                || "last result: none".to_string(),
                |v| format!("last result: {v}"),
            ),
            format!(
                "session: requests={} classified={} empty={} invalid={} errors={}",
                self.session_requests,
                self.session_classified,
                self.session_empty,
                self.session_invalid,
                self.session_errors
            ),
            format!(
                "db: total={total} classified={classified} empty={empty} invalid={invalid} failed={failed} pending={pending}"
            ),
            format!("ntfy: {}", self.ntfy_url),
        ];

        lines.push("top pathways:".to_string());
        push_label_lines(&mut lines, &self.cache.top_pathways);
        lines.push("top superclasses:".to_string());
        push_label_lines(&mut lines, &self.cache.top_superclasses);
        lines.push("top classes:".to_string());
        push_label_lines(&mut lines, &self.cache.top_classes);
        lines.push("recent events:".to_string());
        push_recent(&mut lines, &self.recent_events, "(none)");
        lines.push("recent errors:".to_string());
        push_recent(&mut lines, &self.recent_errors, "(none)");

        let mut stderr = io::stderr();
        queue!(
            stderr,
            BeginSynchronizedUpdate,
            MoveTo(0, 0),
            Clear(ClearType::All)
        )?;

        for (row, line) in lines.into_iter().enumerate().take(height as usize) {
            if row > 0 {
                write!(stderr, "\r\n")?;
            }
            write_styled_line(&mut stderr, &line, width as usize)?;
        }

        queue!(stderr, EndSynchronizedUpdate)?;
        stderr.flush()?;
        Ok(())
    }
}

fn push_ring(ring: &mut VecDeque<String>, max: usize, msg: &str) {
    if ring.len() >= max {
        ring.pop_front();
    }
    ring.push_back(format!("[{}] {msg}", Local::now().format("%H:%M:%S")));
}

fn push_label_lines(lines: &mut Vec<String>, labels: &[LabelCount]) {
    if labels.is_empty() {
        lines.push("  (none)".to_string());
        return;
    }
    for l in labels {
        lines.push(format!("  {} = {}", l.label, l.cnt));
    }
}

fn push_recent(lines: &mut Vec<String>, events: &VecDeque<String>, empty: &str) {
    if events.is_empty() {
        lines.push(format!("  {empty}"));
        return;
    }
    for event in events {
        lines.push(format!("  {event}"));
    }
}

fn ellipsize(s: &str, width: usize) -> String {
    if s.chars().count() <= width.saturating_sub(1) {
        return s.to_string();
    }
    if width <= 2 {
        return ".".to_string();
    }
    let mut t: String = s.chars().take(width - 2).collect();
    t.push('…');
    t
}

fn write_styled_line(stderr: &mut io::Stderr, line: &str, width: usize) -> io::Result<()> {
    let line = ellipsize(line, width);
    if line.starts_with("NPClassifier scraper") {
        queue!(
            stderr,
            SetForegroundColor(Color::DarkCyan),
            SetAttribute(Attribute::Bold),
            Print(&line),
            ResetColor,
            SetAttribute(Attribute::Reset)
        )?;
    } else if is_section(&line) {
        queue!(
            stderr,
            SetForegroundColor(Color::DarkBlue),
            SetAttribute(Attribute::Bold),
            Print(&line),
            ResetColor,
            SetAttribute(Attribute::Reset)
        )?;
    } else if line.contains("error") && line.starts_with("  [") {
        queue!(
            stderr,
            SetForegroundColor(Color::DarkRed),
            Print(&line),
            ResetColor
        )?;
    } else if line.starts_with("  [") {
        queue!(
            stderr,
            SetForegroundColor(Color::DarkGreen),
            Print(&line),
            ResetColor
        )?;
    } else {
        queue!(stderr, Print(&line))?;
    }
    Ok(())
}

fn is_section(line: &str) -> bool {
    matches!(
        line,
        "top pathways:"
            | "top superclasses:"
            | "top classes:"
            | "recent events:"
            | "recent errors:"
    )
}
