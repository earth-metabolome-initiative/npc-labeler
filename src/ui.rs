use chrono::Local;
use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::queue;
use crossterm::style::{Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor};
use crossterm::terminal::{
    BeginSynchronizedUpdate, Clear, ClearType, DisableLineWrap, EnableLineWrap,
    EndSynchronizedUpdate, EnterAlternateScreen, LeaveAlternateScreen, size,
};
use std::io::{self, IsTerminal, Write};
use std::time::Instant;

pub struct Ui {
    interactive: bool,
    ntfy_url: Option<String>,
    started_at: Instant,
    current_smiles: Option<String>,
    current_cid: Option<i32>,
    last_result: Option<String>,
    session_requests: u64,
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
            ntfy_url: if ntfy_url.is_empty() {
                None
            } else {
                Some(ntfy_url)
            },
            started_at: Instant::now(),
            current_smiles: None,
            current_cid: None,
            last_result: None,
            session_requests: 0,
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
        if self.interactive {
            self.current_smiles = Some(if smiles.len() > 60 {
                format!("{}...", &smiles[..57])
            } else {
                smiles.to_string()
            });
        }
        self.session_requests += 1;
    }

    pub fn note_classified(&mut self, cid: i32) {
        self.last_result = Some(format!("classified CID {cid}"));
    }

    pub fn note_empty(&mut self, cid: i32) {
        self.last_result = Some(format!("empty CID {cid}"));
    }

    pub fn note_invalid(&mut self, cid: i32) {
        self.last_result = Some(format!("invalid CID {cid}"));
    }

    pub fn note_error(&mut self, cid: i32, error: &str) {
        self.last_result = Some(format!("error CID {cid}: {error}"));
    }

    pub fn note_rate_limit(&mut self, cid: i32) {
        self.last_result = Some(format!("rate limited CID {cid}"));
    }

    pub fn render(&mut self) {
        if !self.interactive {
            let uptime = self.started_at.elapsed().as_secs().max(1);
            let rate = self.session_requests as f64 / uptime as f64;
            let current = self
                .current_cid
                .map_or_else(|| "idle".to_string(), |cid| format!("CID {cid}"));
            let last_result = self.last_result.as_deref().unwrap_or("none");
            eprintln!(
                "[progress] uptime={uptime}s | req_rate={rate:.1}/s | current={current} | last={last_result}"
            );
            return;
        }

        let _ = self.render_dashboard();
    }

    fn render_dashboard(&self) -> io::Result<()> {
        let uptime = self.started_at.elapsed().as_secs().max(1);
        let rate = self.session_requests as f64 / uptime as f64;
        let (width, height) = size().unwrap_or((120, 12));
        let lines = self.dashboard_lines(uptime, rate);

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

    fn dashboard_lines(&self, uptime: u64, rate: f64) -> Vec<String> {
        let mut lines = vec![
            format!(
                "NPClassifier scraper    {}",
                Local::now().format("%Y-%m-%d %H:%M:%S")
            ),
            format!("uptime={}s | req_rate={:.2}/s", uptime, rate),
        ];
        if let Some(ntfy_url) = &self.ntfy_url {
            lines.push(format!("ntfy subscribe: {ntfy_url}"));
        }
        lines.push(format!(
            "current: CID {} | {}",
            self.current_cid
                .map_or("idle".to_string(), |c| c.to_string()),
            self.current_smiles.as_deref().unwrap_or(""),
        ));
        lines.push(self.last_result.as_ref().map_or_else(
            || "last result: none".to_string(),
            |v| format!("last result: {v}"),
        ));
        lines
    }
}

#[cfg(test)]
impl Ui {
    pub(crate) fn test_noninteractive() -> Self {
        Self {
            interactive: false,
            ntfy_url: None,
            started_at: Instant::now(),
            current_smiles: None,
            current_cid: None,
            last_result: None,
            session_requests: 0,
        }
    }

    pub(crate) fn test_interactive() -> Self {
        Self {
            interactive: true,
            ntfy_url: None,
            started_at: Instant::now(),
            current_smiles: None,
            current_cid: None,
            last_result: None,
            session_requests: 0,
        }
    }
}

fn ellipsize(s: &str, width: usize) -> String {
    if s.chars().count() <= width.saturating_sub(1) {
        return s.to_string();
    }
    if width <= 4 {
        return ".".to_string();
    }
    let mut t: String = s.chars().take(width - 4).collect();
    t.push_str("...");
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
    } else {
        queue!(stderr, Print(&line))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn note_current_truncates_long_smiles_and_updates_result_fields() {
        let mut ui = Ui {
            interactive: true,
            ntfy_url: None,
            started_at: Instant::now(),
            current_smiles: None,
            current_cid: None,
            last_result: None,
            session_requests: 0,
        };

        ui.note_current(
            42,
            "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
        );
        ui.note_classified(42);

        assert_eq!(ui.current_cid, Some(42));
        assert!(
            ui.current_smiles
                .as_deref()
                .expect("smiles")
                .ends_with("...")
        );
        assert_eq!(ui.last_result.as_deref(), Some("classified CID 42"));
        assert_eq!(ui.session_requests, 1);
    }

    #[test]
    fn render_non_interactive_smoke_test() {
        let mut ui = Ui {
            interactive: false,
            ntfy_url: None,
            started_at: Instant::now(),
            current_smiles: Some("CCO".to_string()),
            current_cid: Some(7),
            last_result: Some("empty CID 7".to_string()),
            session_requests: 3,
        };

        ui.render();
    }

    #[test]
    fn ellipsize_handles_narrow_and_wide_widths() {
        assert_eq!(ellipsize("abcdef", 10), "abcdef");
        assert_eq!(ellipsize("abcdef", 4), ".");
        assert_eq!(ellipsize("abcdef", 6), "ab...");
    }

    #[test]
    fn enter_terminal_returns_none_when_ui_is_non_interactive() {
        let ui = Ui::test_noninteractive();

        assert!(ui.enter_terminal().is_none());
    }

    #[test]
    fn note_helpers_cover_remaining_status_messages() {
        let mut ui = Ui::test_noninteractive();

        ui.note_empty(7);
        assert_eq!(ui.last_result.as_deref(), Some("empty CID 7"));
        ui.note_invalid(8);
        assert_eq!(ui.last_result.as_deref(), Some("invalid CID 8"));
        ui.note_error(9, "boom");
        assert_eq!(ui.last_result.as_deref(), Some("error CID 9: boom"));
        ui.note_rate_limit(10);
        assert_eq!(ui.last_result.as_deref(), Some("rate limited CID 10"));
    }

    #[test]
    fn render_dashboard_and_write_styled_line_smoke_tests() {
        let mut ui = Ui::test_interactive();
        ui.ntfy_url = Some("https://ntfy.sh/test-topic".to_string());
        ui.current_smiles = Some("CCO".to_string());
        ui.current_cid = Some(7);
        ui.last_result = Some("empty CID 7".to_string());
        ui.session_requests = 3;

        let lines = ui.dashboard_lines(1, 3.0);
        assert!(lines.iter().any(|line| line.contains("ntfy subscribe:")));

        ui.render_dashboard().expect("render dashboard");
        ui.render();

        let terminal = ui.enter_terminal();
        drop(terminal);

        let mut stderr = io::stderr();
        write_styled_line(&mut stderr, "NPClassifier scraper", 80).expect("title line");
        write_styled_line(&mut stderr, "plain line", 80).expect("plain line");
    }

    #[test]
    fn inactive_terminal_guard_drop_is_a_noop() {
        let guard = TerminalGuard { active: false };
        drop(guard);
    }
}
