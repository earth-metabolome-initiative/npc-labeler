mod api;
mod failure_log;
mod output;
mod state;
#[cfg(test)]
mod test_support;
mod ui;
mod zenodo;

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDate, Timelike, Utc};
use clap::Parser;
use flate2::read::GzDecoder;

use api::{ApiResponse, ClassifyError};
use failure_log::FailureLogger;
use output::{ChunkIndex, ChunkWriter, build_release, cleanup_completed_dir};
use state::{LineIndex, StateStore};
use ui::Ui;

const COMPLETED_DIR: &str = "completed";
const LOG_DIR: &str = "logs";
const NTFY_BASE: &str = "https://ntfy.sh";
const PUBLISH_INTERVAL: Duration = Duration::from_hours(168);
const RELEASE_DIR: &str = "releases";
const RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(15),
];
const DAILY_STATUS_HOUR_UTC: u32 = 18;
const STATE_DIR: &str = "state";
const STATE_SYNC_INTERVAL: Duration = Duration::from_secs(5);
const STATE_SYNC_ROWS: u64 = 1_000;
const TARGET_CHUNK_BYTES: u64 = 128 * 1024 * 1024;
const DASHBOARD_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct RuntimeConfig {
    completed_dir: PathBuf,
    state_dir: PathBuf,
    log_dir: PathBuf,
    release_dir: PathBuf,
    api_url: String,
    ntfy_base: Option<String>,
    publish_interval: Duration,
    retry_delays: [Duration; 3],
    require_zenodo_token: bool,
    install_ctrlc: bool,
}

impl RuntimeConfig {
    fn production() -> Self {
        Self {
            completed_dir: PathBuf::from(COMPLETED_DIR),
            state_dir: PathBuf::from(STATE_DIR),
            log_dir: PathBuf::from(LOG_DIR),
            release_dir: PathBuf::from(RELEASE_DIR),
            api_url: api::DEFAULT_API_URL.to_string(),
            ntfy_base: Some(NTFY_BASE.to_string()),
            publish_interval: PUBLISH_INTERVAL,
            retry_delays: RETRY_DELAYS,
            require_zenodo_token: true,
            install_ctrlc: true,
        }
    }
}

#[derive(Parser)]
#[command(name = "npc-labeler")]
#[command(about = "Stream PubChem SMILES through the NPClassifier API")]
struct Args {
    /// Path to CID-SMILES input file (.gz or plain).
    #[arg(long, default_value = "CID-SMILES.gz")]
    input: String,
}

#[derive(Clone, Copy)]
struct RuntimeCounts {
    total: u64,
    successful: u64,
    invalid: u64,
    failed: u64,
}

impl RuntimeCounts {
    #[inline]
    fn processed(self) -> u64 {
        self.successful + self.invalid + self.failed
    }

    #[inline]
    fn pending(self) -> u64 {
        self.total.saturating_sub(self.processed())
    }
}

enum RowOutcome {
    Success(ApiResponse),
    Invalid,
    Failed {
        attempt: u8,
        kind: String,
        message: String,
    },
    Interrupted,
}

fn main() {
    dotenvy::dotenv().ok();

    let args = Args::parse();
    if let Err(error) = run(&args) {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run(args: &Args) -> io::Result<()> {
    run_with_config(args, &RuntimeConfig::production())
}

#[allow(clippy::too_many_lines)]
fn run_with_config(args: &Args, config: &RuntimeConfig) -> io::Result<()> {
    let zenodo_token = zenodo_token_from_env(config.require_zenodo_token)?;
    let input_path = Path::new(&args.input);
    let total_rows = count_input_rows(input_path)?;

    let mut chunk_index = ChunkIndex::open(&config.state_dir.join("chunks.jsonl"))?;
    cleanup_completed_dir(&config.completed_dir, &chunk_index)?;

    let mut state = StateStore::open(&config.state_dir, total_rows as usize)?;
    let done_rows = state.rebuild_done_from_chunks(chunk_index.records())?;
    let mut counts = RuntimeCounts {
        total: u64::from(total_rows),
        successful: done_rows,
        invalid: state.count_invalid(),
        failed: state.count_failed(),
    };

    if counts.processed() >= counts.total {
        print_summary(counts);
        return Ok(());
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    if config.install_ctrlc {
        install_ctrlc_handler(&shutdown)?;
    }

    let agent = ureq::AgentBuilder::new()
        .timeout_read(Duration::from_secs(15))
        .timeout_write(Duration::from_secs(5))
        .timeout_connect(Duration::from_secs(10))
        .build();

    let ntfy_url = config
        .ntfy_base
        .as_ref()
        .map(|base| format!("{base}/{}", uuid::Uuid::new_v4()));
    let mut ui = Ui::new(ntfy_url.clone().unwrap_or_default());
    let terminal = ui.enter_terminal();

    let mut writer = ChunkWriter::new(
        &config.completed_dir,
        TARGET_CHUNK_BYTES,
        chunk_index.next_chunk_id(),
    )?;
    let mut failure_log = FailureLogger::open(&config.log_dir)?;

    if let Some(ref url) = ntfy_url {
        eprintln!("[ntfy] subscribe for updates: {url}");
    }
    notify(
        &agent,
        ntfy_url.as_deref(),
        &format!(
            "npc-labeler started: {}/{} processed",
            counts.processed(),
            counts.total
        ),
    );

    let mut last_dashboard = Instant::now();
    let mut last_publish = Instant::now();
    let mut last_state_sync = Instant::now();
    let mut last_notified_pct = percent(counts.processed(), counts.total);
    let mut last_daily_status_date = None;
    let mut terminal_updates_since_sync = 0_u64;
    let mut line_index: LineIndex = 0;
    let use_default_api = config.api_url == api::DEFAULT_API_URL;

    let mut reader = open_input_reader(input_path)?;
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        if last_publish.elapsed() >= config.publish_interval {
            publish_to_zenodo(
                &mut writer,
                &mut state,
                &mut chunk_index,
                counts,
                config,
                zenodo_token.as_deref(),
                &agent,
                ntfy_url.as_deref(),
            )?;
            last_publish = Instant::now();
        }

        let Some((cid, smiles)) = parse_input_line(&line) else {
            continue;
        };

        let current_line = line_index;
        line_index = line_index
            .checked_add(1)
            .ok_or_else(|| io::Error::other("line index overflow"))?;

        if state.is_terminal(current_line) {
            continue;
        }

        ui.note_current(cid, &smiles);
        match classify_with_retry(
            &agent,
            &config.api_url,
            use_default_api,
            &config.retry_delays,
            cid,
            smiles,
            &mut ui,
            &shutdown,
        ) {
            RowOutcome::Success(response) => {
                let has_labels = has_labels(&response);
                writer.append(current_line, cid, smiles, response)?;
                counts.successful += 1;
                terminal_updates_since_sync += 1;
                if has_labels {
                    ui.note_classified(cid);
                } else {
                    ui.note_empty(cid);
                }
            }
            RowOutcome::Invalid => {
                state.mark_invalid(current_line);
                counts.invalid += 1;
                terminal_updates_since_sync += 1;
                ui.note_invalid(cid);
            }
            RowOutcome::Failed {
                attempt,
                kind,
                message,
            } => {
                state.mark_failed(current_line);
                failure_log.log(current_line, cid, smiles, &kind, &message, attempt)?;
                counts.failed += 1;
                terminal_updates_since_sync += 1;
                ui.note_error(cid, &message);
            }
            RowOutcome::Interrupted => break,
        }

        if terminal_updates_since_sync >= STATE_SYNC_ROWS
            || last_state_sync.elapsed() >= STATE_SYNC_INTERVAL
        {
            sync_runtime_state(&mut writer, &mut state, &mut chunk_index)?;
            terminal_updates_since_sync = 0;
            last_state_sync = Instant::now();
        }

        maybe_notify_daily_status(
            &agent,
            ntfy_url.as_deref(),
            counts,
            &mut last_daily_status_date,
        );

        if last_dashboard.elapsed() >= DASHBOARD_INTERVAL {
            ui.render();
            let pct = percent(counts.processed(), counts.total);
            if pct > last_notified_pct {
                notify(
                    &agent,
                    ntfy_url.as_deref(),
                    &format!("{pct}% -- {}/{}", counts.processed(), counts.total),
                );
                last_notified_pct = pct;
            }
            last_dashboard = Instant::now();
        }
    }

    if !shutdown.load(Ordering::Relaxed) && line_index != total_rows {
        return Err(io::Error::other(format!(
            "counted {total_rows} valid rows during prepass but streamed {line_index}"
        )));
    }

    sync_runtime_state(&mut writer, &mut state, &mut chunk_index)?;
    let _ = writer.seal_current(&mut state, &mut chunk_index)?;

    drop(terminal);

    notify(
        &agent,
        ntfy_url.as_deref(),
        &format!(
            "npc-labeler finished: {}/{} processed",
            counts.processed(),
            counts.total
        ),
    );
    print_summary(counts);

    Ok(())
}

fn classify_with_retry(
    agent: &ureq::Agent,
    api_url: &str,
    use_default_api: bool,
    retry_delays: &[Duration; 3],
    cid: i32,
    smiles: &str,
    ui: &mut Ui,
    shutdown: &AtomicBool,
) -> RowOutcome {
    let mut attempt = 1_u8;
    loop {
        let result = if use_default_api {
            api::classify(agent, smiles)
        } else {
            api::classify_at(agent, api_url, smiles)
        };
        match result {
            Ok(response) => return RowOutcome::Success(response),
            Err(ClassifyError::InvalidSmiles) => return RowOutcome::Invalid,
            Err(error) => {
                let Some(delay) = retry_delays.get((attempt - 1) as usize) else {
                    return RowOutcome::Failed {
                        attempt,
                        kind: error.kind().to_string(),
                        message: error.message(),
                    };
                };

                if matches!(&error, ClassifyError::RateLimit) {
                    ui.note_rate_limit(cid);
                } else {
                    ui.note_error(cid, &format!("{error}; retrying in {}s", delay.as_secs()));
                }

                if sleep_with_shutdown(*delay, shutdown) {
                    return RowOutcome::Interrupted;
                }
                attempt += 1;
            }
        }
    }
}

#[inline]
fn count_input_rows(path: &Path) -> io::Result<LineIndex> {
    let mut total = 0_u32;
    let mut reader = open_input_reader(path)?;
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        if parse_input_line(&line).is_some() {
            total = total
                .checked_add(1)
                .ok_or_else(|| io::Error::other("input has more than u32::MAX valid rows"))?;
        }
    }
    Ok(total)
}

#[inline]
fn has_labels(response: &ApiResponse) -> bool {
    !response.class_results.is_empty()
        || !response.superclass_results.is_empty()
        || !response.pathway_results.is_empty()
}

fn install_ctrlc_handler(shutdown: &Arc<AtomicBool>) -> io::Result<()> {
    let signal = shutdown.clone();
    ctrlc::set_handler(move || {
        if signal.load(Ordering::Relaxed) {
            eprintln!("\nForce exit.");
            std::process::exit(1);
        }
        eprintln!("\nShutdown requested, finishing current request...");
        signal.store(true, Ordering::SeqCst);
    })
    .map_err(|error| io::Error::other(error.to_string()))
}

fn notify(agent: &ureq::Agent, ntfy_url: Option<&str>, message: &str) {
    let Some(ntfy_url) = ntfy_url else {
        return;
    };
    if let Err(error) = agent.post(ntfy_url).send_string(message) {
        eprintln!("[ntfy] failed to send notification: {error}");
    }
}

fn open_input_reader(path: &Path) -> io::Result<Box<dyn BufRead>> {
    let file = File::open(path)?;
    if path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("gz"))
    {
        return Ok(Box::new(BufReader::with_capacity(
            8 * 1024 * 1024,
            GzDecoder::new(file),
        )));
    }
    Ok(Box::new(BufReader::with_capacity(8 * 1024 * 1024, file)))
}

#[inline]
fn parse_input_line(line: &str) -> Option<(i32, &str)> {
    let (cid, smiles) = line.split_once('\t')?;
    let cid = cid.parse::<i32>().ok()?;
    Some((cid, smiles.trim()))
}

#[inline]
fn percent(done: u64, total: u64) -> u64 {
    if total == 0 {
        return 100;
    }
    ((done as f64 / total as f64) * 100.0) as u64
}

fn print_summary(counts: RuntimeCounts) {
    println!("\n=== Summary ===");
    println!("Total:      {}", counts.total);
    println!("Successful: {}", counts.successful);
    println!("Invalid:    {}", counts.invalid);
    println!("Failed:     {}", counts.failed);
    println!("Pending:    {}", counts.pending());
}

fn zenodo_token_from_env(require_token: bool) -> io::Result<Option<String>> {
    match std::env::var("ZENODO_TOKEN") {
        Ok(token) if !token.trim().is_empty() => Ok(Some(token)),
        _ if require_token => Err(io::Error::other(
            "ZENODO_TOKEN is not set; refusing to start without a Zenodo access token",
        )),
        _ => Ok(None),
    }
}

fn should_send_daily_status(now_utc: DateTime<Utc>, last_sent_date: Option<NaiveDate>) -> bool {
    now_utc.hour() >= DAILY_STATUS_HOUR_UTC && Some(now_utc.date_naive()) != last_sent_date
}

fn daily_status_message(counts: RuntimeCounts) -> String {
    format!(
        "18:00 UTC status: handled {}/{} samples | failures={} | successful={} | invalid={} | pending={}",
        counts.processed(),
        counts.total,
        counts.failed,
        counts.successful,
        counts.invalid,
        counts.pending()
    )
}

fn maybe_notify_daily_status(
    agent: &ureq::Agent,
    ntfy_url: Option<&str>,
    counts: RuntimeCounts,
    last_sent_date: &mut Option<NaiveDate>,
) {
    let now_utc = Utc::now();
    if !should_send_daily_status(now_utc, *last_sent_date) {
        return;
    }
    notify(agent, ntfy_url, &daily_status_message(counts));
    *last_sent_date = Some(now_utc.date_naive());
}

fn zenodo_release_complete_message(doi: &str, counts: RuntimeCounts) -> String {
    format!(
        "Zenodo release complete: {doi} | handled {}/{} samples | failures={} | successful={} | invalid={}",
        counts.processed(),
        counts.total,
        counts.failed,
        counts.successful,
        counts.invalid
    )
}

#[allow(clippy::too_many_arguments)]
fn publish_to_zenodo(
    writer: &mut ChunkWriter,
    state: &mut StateStore,
    chunk_index: &mut ChunkIndex,
    counts: RuntimeCounts,
    config: &RuntimeConfig,
    zenodo_token: Option<&str>,
    agent: &ureq::Agent,
    ntfy_url: Option<&str>,
) -> io::Result<()> {
    let Some(zenodo_token) = zenodo_token else {
        eprintln!("[zenodo] ZENODO_TOKEN not set, skipping publish");
        return Ok(());
    };

    sync_runtime_state(writer, state, chunk_index)?;
    let _ = writer.seal_current(state, chunk_index)?;

    let release = build_release(
        &config.completed_dir,
        &config.release_dir,
        chunk_index,
        counts.successful,
        counts.invalid,
        counts.failed,
    )?;

    match zenodo::publish(
        zenodo_token,
        &release.output_path,
        &release.manifest_path,
        counts.successful,
        counts.invalid,
        counts.failed,
    ) {
        Ok(doi) => {
            notify(
                agent,
                ntfy_url,
                &format!("Published to Zenodo: {doi} ({} rows)", counts.successful),
            );
            notify(
                agent,
                ntfy_url,
                &zenodo_release_complete_message(&doi, counts),
            );
            std::fs::remove_file(&release.output_path)?;
            std::fs::remove_file(&release.manifest_path)?;
        }
        Err(error) => {
            eprintln!("[zenodo] publish failed: {error}");
            notify(agent, ntfy_url, &format!("Zenodo publish FAILED: {error}"));
        }
    }

    Ok(())
}

fn sleep_with_shutdown(duration: Duration, shutdown: &AtomicBool) -> bool {
    let started = Instant::now();
    while started.elapsed() < duration {
        if shutdown.load(Ordering::Relaxed) {
            return true;
        }
        let remaining = duration.saturating_sub(started.elapsed());
        thread::sleep(remaining.min(Duration::from_millis(200)));
    }
    shutdown.load(Ordering::Relaxed)
}

fn sync_runtime_state(
    writer: &mut ChunkWriter,
    state: &mut StateStore,
    chunk_index: &mut ChunkIndex,
) -> io::Result<()> {
    let active_size = writer.sync_active()?;
    state.sync_terminal()?;
    if writer.should_rotate_for_size(active_size) {
        let _ = writer.seal_current(state, chunk_index)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockHttpServer, MockResponse, TestDir};
    use chrono::TimeZone;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::fs::read_to_string;
    use std::io::Write;
    use std::sync::{Mutex, OnceLock};
    use zstd::stream::read::Decoder;

    #[test]
    fn parse_input_line_rejects_invalid_rows() {
        assert_eq!(parse_input_line("123\tCCO"), Some((123, "CCO")));
        assert_eq!(parse_input_line("nope\tCCO"), None);
        assert_eq!(parse_input_line("123"), None);
    }

    #[test]
    fn counts_only_valid_rows_in_plain_and_gzip_inputs() {
        let temp_dir = TestDir::new("main");
        let plain_path = temp_dir.path().join("CID-SMILES.txt");
        let gzip_path = temp_dir.path().join("CID-SMILES.gz");
        let payload = "1\tCCO\ninvalid\n2\tCCC\nbad\tDDD\n3\t\n";

        std::fs::write(&plain_path, payload).expect("write plain input");

        let gzip_file = File::create(&gzip_path).expect("create gzip input");
        let mut encoder = GzEncoder::new(gzip_file, Compression::default());
        encoder
            .write_all(payload.as_bytes())
            .expect("write gzip payload");
        encoder.finish().expect("finish gzip payload");

        assert_eq!(count_input_rows(&plain_path).expect("count plain rows"), 3);
        assert_eq!(count_input_rows(&gzip_path).expect("count gzip rows"), 3);
    }

    #[test]
    fn run_with_config_processes_stream_end_to_end() {
        let temp_dir = TestDir::new("run");
        let input_path = temp_dir.path().join("CID-SMILES.txt");
        std::fs::write(&input_path, "1\tCCO\n2\tCCC\n3\tDDD\n4\tEEE\n").expect("write input file");

        let server = MockHttpServer::spawn(vec![
            MockResponse::json(
                "200 OK",
                r#"{"class_results":["lipid"],"superclass_results":[],"pathway_results":[],"isglycoside":false}"#,
            ),
            MockResponse::json(
                "200 OK",
                r#"{"class_results":[],"superclass_results":[],"pathway_results":[],"isglycoside":false}"#,
            ),
            MockResponse::empty("500 Internal Server Error"),
            MockResponse::empty("503 Service Unavailable"),
            MockResponse::empty("503 Service Unavailable"),
            MockResponse::empty("503 Service Unavailable"),
            MockResponse::empty("503 Service Unavailable"),
        ]);

        let config = RuntimeConfig {
            completed_dir: temp_dir.path().join("completed"),
            state_dir: temp_dir.path().join("state"),
            log_dir: temp_dir.path().join("logs"),
            release_dir: temp_dir.path().join("releases"),
            api_url: server.url("/classify"),
            ntfy_base: None,
            publish_interval: Duration::from_hours(168),
            retry_delays: [
                Duration::from_millis(0),
                Duration::from_millis(0),
                Duration::from_millis(0),
            ],
            require_zenodo_token: false,
            install_ctrlc: false,
        };
        let args = Args {
            input: input_path.to_string_lossy().into_owned(),
        };

        run_with_config(&args, &config).expect("run streaming pipeline");

        let chunk_path = config.completed_dir.join("part-000001.jsonl.zst");
        let decoder = Decoder::new(File::open(&chunk_path).expect("open chunk")).expect("decoder");
        let lines = BufReader::new(decoder)
            .lines()
            .collect::<Result<Vec<_>, _>>()
            .expect("read completed records");
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"cid\":1"));
        assert!(lines[1].contains("\"cid\":2"));

        let chunks =
            read_to_string(config.state_dir.join("chunks.jsonl")).expect("read chunk index");
        assert!(chunks.contains("part-000001.jsonl.zst"));

        let mut state = StateStore::open(&config.state_dir, 4).expect("open state");
        let chunk_index =
            ChunkIndex::open(&config.state_dir.join("chunks.jsonl")).expect("open chunk index");
        assert_eq!(
            state
                .rebuild_done_from_chunks(chunk_index.records())
                .expect("rebuild done"),
            2
        );
        assert_eq!(state.count_invalid(), 1);
        assert_eq!(state.count_failed(), 1);

        let failure_log =
            read_to_string(config.log_dir.join("failures.log")).expect("read failure log");
        assert!(failure_log.contains("\"cid\":4"));
        assert!(failure_log.contains("\"attempt\":4"));
        assert_eq!(server.requests().len(), 7);
    }

    #[test]
    fn runtime_helpers_cover_counts_and_percent() {
        let counts = RuntimeCounts {
            total: 10,
            successful: 3,
            invalid: 2,
            failed: 1,
        };
        assert_eq!(counts.processed(), 6);
        assert_eq!(counts.pending(), 4);
        assert_eq!(percent(0, 0), 100);
        assert_eq!(percent(5, 10), 50);
        assert!(has_labels(&ApiResponse {
            class_results: vec!["lipid".to_string()],
            superclass_results: Vec::new(),
            pathway_results: Vec::new(),
            isglycoside: false,
        }));
        assert!(!has_labels(&ApiResponse {
            class_results: Vec::new(),
            superclass_results: Vec::new(),
            pathway_results: Vec::new(),
            isglycoside: false,
        }));
        let config = RuntimeConfig::production();
        assert_eq!(config.completed_dir, PathBuf::from(COMPLETED_DIR));
        assert_eq!(config.state_dir, PathBuf::from(STATE_DIR));
        assert_eq!(config.log_dir, PathBuf::from(LOG_DIR));
        assert_eq!(config.release_dir, PathBuf::from(RELEASE_DIR));
        assert_eq!(config.api_url, api::DEFAULT_API_URL);
        assert!(config.ntfy_base.is_some());
        assert!(config.require_zenodo_token);
        assert!(config.install_ctrlc);
    }

    #[test]
    fn daily_status_and_zenodo_messages_include_expected_state() {
        let counts = RuntimeCounts {
            total: 10,
            successful: 3,
            invalid: 2,
            failed: 1,
        };
        let before_cutoff = Utc.with_ymd_and_hms(2026, 3, 26, 17, 59, 59).unwrap();
        let after_cutoff = Utc.with_ymd_and_hms(2026, 3, 26, 18, 0, 0).unwrap();
        let next_day = Utc.with_ymd_and_hms(2026, 3, 27, 18, 0, 0).unwrap();

        assert!(!should_send_daily_status(before_cutoff, None));
        assert!(should_send_daily_status(after_cutoff, None));
        assert!(!should_send_daily_status(
            after_cutoff,
            Some(after_cutoff.date_naive())
        ));
        assert!(should_send_daily_status(
            next_day,
            Some(after_cutoff.date_naive())
        ));

        let daily_status = daily_status_message(counts);
        assert!(daily_status.contains("handled 6/10 samples"));
        assert!(daily_status.contains("failures=1"));
        assert!(daily_status.contains("pending=4"));

        let zenodo_message = zenodo_release_complete_message("10.1234/mock", counts);
        assert!(zenodo_message.contains("Zenodo release complete: 10.1234/mock"));
        assert!(zenodo_message.contains("handled 6/10 samples"));
        assert!(zenodo_message.contains("successful=3"));
    }

    #[test]
    fn zenodo_token_requirement_is_enforced() {
        let _guard = env_lock();
        let previous = std::env::var("ZENODO_TOKEN").ok();
        unsafe {
            std::env::remove_var("ZENODO_TOKEN");
        }

        let error = zenodo_token_from_env(true).expect_err("missing token should fail");
        assert!(error.to_string().contains("ZENODO_TOKEN is not set"));
        assert!(
            zenodo_token_from_env(false)
                .expect("optional token")
                .is_none()
        );

        unsafe {
            std::env::set_var("ZENODO_TOKEN", "token-value");
        }
        assert_eq!(
            zenodo_token_from_env(true).expect("required token"),
            Some("token-value".to_string())
        );

        if let Some(previous) = previous {
            unsafe {
                std::env::set_var("ZENODO_TOKEN", previous);
            }
        } else {
            unsafe {
                std::env::remove_var("ZENODO_TOKEN");
            }
        }
    }

    #[test]
    fn classify_with_retry_returns_failed_after_retry_budget_exhausted() {
        let server = MockHttpServer::spawn(vec![
            MockResponse::empty("503 Service Unavailable"),
            MockResponse::empty("503 Service Unavailable"),
            MockResponse::empty("503 Service Unavailable"),
            MockResponse::empty("503 Service Unavailable"),
        ]);
        let agent = ureq::AgentBuilder::new().build();
        let shutdown = AtomicBool::new(false);
        let mut ui = Ui::test_noninteractive();

        let outcome = classify_with_retry(
            &agent,
            &server.url("/classify"),
            false,
            &[Duration::from_millis(0); 3],
            77,
            "CCO",
            &mut ui,
            &shutdown,
        );

        match outcome {
            RowOutcome::Failed {
                attempt,
                kind,
                message,
            } => {
                assert_eq!(attempt, 4);
                assert_eq!(kind, "server_error");
                assert!(message.contains("HTTP 503"));
            }
            _ => panic!("expected failed outcome"),
        }
        assert_eq!(server.requests().len(), 4);
    }

    #[test]
    fn classify_with_retry_returns_interrupted_when_shutdown_is_requested() {
        let server = MockHttpServer::spawn(vec![MockResponse::empty("429 Too Many Requests")]);
        let agent = ureq::AgentBuilder::new().build();
        let shutdown = AtomicBool::new(true);
        let mut ui = Ui::test_noninteractive();

        let outcome = classify_with_retry(
            &agent,
            &server.url("/classify"),
            false,
            &[
                Duration::from_millis(1),
                Duration::from_millis(0),
                Duration::from_millis(0),
            ],
            88,
            "CCO",
            &mut ui,
            &shutdown,
        );

        assert!(matches!(outcome, RowOutcome::Interrupted));
    }

    #[test]
    fn run_with_config_returns_early_when_everything_is_already_terminal() {
        let temp_dir = TestDir::new("run-noop");
        let input_path = temp_dir.path().join("CID-SMILES.txt");
        std::fs::write(&input_path, "1\tCCO\n").expect("write input");

        let state_dir = temp_dir.path().join("state");
        let completed_dir = temp_dir.path().join("completed");
        let mut state = StateStore::open(&state_dir, 1).expect("open state");
        let mut chunk_index =
            ChunkIndex::open(&state_dir.join("chunks.jsonl")).expect("open index");
        let mut writer =
            ChunkWriter::new(&completed_dir, 1024, chunk_index.next_chunk_id()).expect("writer");
        writer
            .append(
                0,
                1,
                "CCO",
                ApiResponse {
                    class_results: vec!["lipid".to_string()],
                    superclass_results: Vec::new(),
                    pathway_results: Vec::new(),
                    isglycoside: false,
                },
            )
            .expect("append");
        let _ = writer
            .seal_current(&mut state, &mut chunk_index)
            .expect("seal")
            .expect("chunk");

        let args = Args {
            input: input_path.to_string_lossy().into_owned(),
        };
        let config = RuntimeConfig {
            completed_dir,
            state_dir,
            log_dir: temp_dir.path().join("logs"),
            release_dir: temp_dir.path().join("releases"),
            api_url: "http://127.0.0.1:9/classify".to_string(),
            ntfy_base: None,
            publish_interval: Duration::from_hours(168),
            retry_delays: [Duration::from_millis(0); 3],
            require_zenodo_token: false,
            install_ctrlc: false,
        };

        run_with_config(&args, &config).expect("no-op run");
    }

    #[test]
    fn notify_none_and_sleep_with_shutdown_cover_short_circuit_paths() {
        let agent = ureq::AgentBuilder::new().build();
        notify(&agent, None, "ignored");
        notify(&agent, Some("http://127.0.0.1:9/ntfy"), "will fail locally");

        let shutdown = AtomicBool::new(true);
        assert!(sleep_with_shutdown(Duration::from_millis(1), &shutdown));
    }

    #[test]
    fn notify_posts_successfully_and_sleep_returns_false_without_shutdown() {
        let server = MockHttpServer::spawn(vec![MockResponse::empty("200 OK")]);
        let agent = ureq::AgentBuilder::new().build();

        notify(&agent, Some(&server.url("/ntfy")), "hello world");

        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/ntfy");
        assert_eq!(requests[0].body, b"hello world");

        let shutdown = AtomicBool::new(false);
        assert!(!sleep_with_shutdown(Duration::ZERO, &shutdown));
    }

    #[test]
    fn run_wrapper_returns_early_for_empty_input() {
        let _guard = env_lock();
        let temp_dir = TestDir::new("run-wrapper");
        let previous_dir = std::env::current_dir().expect("current dir");
        let previous_token = std::env::var("ZENODO_TOKEN").ok();
        let input_path = temp_dir.path().join("empty.txt");
        std::fs::write(&input_path, "").expect("write empty input");

        std::env::set_current_dir(temp_dir.path()).expect("enter temp dir");
        unsafe {
            std::env::set_var("ZENODO_TOKEN", "token-value");
        }
        let args = Args {
            input: input_path.to_string_lossy().into_owned(),
        };
        let result = run(&args);
        std::env::set_current_dir(previous_dir).expect("restore current dir");

        if let Some(previous_token) = previous_token {
            unsafe {
                std::env::set_var("ZENODO_TOKEN", previous_token);
            }
        } else {
            unsafe {
                std::env::remove_var("ZENODO_TOKEN");
            }
        }

        result.expect("run wrapper");
    }

    #[test]
    fn run_with_config_sends_notifications_and_checks_publish_interval() {
        let _guard = env_lock();
        let previous = std::env::var("ZENODO_TOKEN").ok();
        unsafe {
            std::env::remove_var("ZENODO_TOKEN");
        }

        let temp_dir = TestDir::new("run-ntfy");
        let input_path = temp_dir.path().join("CID-SMILES.txt");
        std::fs::write(&input_path, "1\tCCO\n").expect("write input");

        let api_server = MockHttpServer::spawn(vec![MockResponse::json(
            "200 OK",
            r#"{"class_results":["lipid"],"superclass_results":[],"pathway_results":[],"isglycoside":false}"#,
        )]);
        let ntfy_server = MockHttpServer::spawn(vec![
            MockResponse::empty("200 OK"),
            MockResponse::empty("200 OK"),
            MockResponse::empty("200 OK"),
        ]);

        let config = RuntimeConfig {
            completed_dir: temp_dir.path().join("completed"),
            state_dir: temp_dir.path().join("state"),
            log_dir: temp_dir.path().join("logs"),
            release_dir: temp_dir.path().join("releases"),
            api_url: api_server.url("/classify"),
            ntfy_base: Some(ntfy_server.url("/topic")),
            publish_interval: Duration::ZERO,
            retry_delays: [Duration::from_millis(0); 3],
            require_zenodo_token: false,
            install_ctrlc: false,
        };
        let args = Args {
            input: input_path.to_string_lossy().into_owned(),
        };

        run_with_config(&args, &config).expect("run with notifications");

        let requests = ntfy_server.requests();
        assert!(requests.len() >= 2);
        let bodies: Vec<_> = requests
            .iter()
            .map(|request| String::from_utf8_lossy(&request.body).into_owned())
            .collect();
        assert!(requests.iter().all(|request| request.method == "POST"));
        assert!(
            bodies
                .iter()
                .any(|body| body.contains("npc-labeler started"))
        );
        assert!(
            bodies
                .iter()
                .any(|body| body.contains("npc-labeler finished"))
        );

        if let Some(previous) = previous {
            unsafe {
                std::env::set_var("ZENODO_TOKEN", previous);
            }
        }
    }

    #[test]
    fn sync_runtime_state_seals_when_chunk_is_over_target_size() {
        let temp_dir = TestDir::new("sync-runtime");
        let state_dir = temp_dir.path().join("state");
        let completed_dir = temp_dir.path().join("completed");
        let mut state = StateStore::open(&state_dir, 2).expect("open state");
        let mut chunk_index =
            ChunkIndex::open(&state_dir.join("chunks.jsonl")).expect("open index");
        let mut writer =
            ChunkWriter::new(&completed_dir, 1, chunk_index.next_chunk_id()).expect("open writer");

        writer
            .append(
                0,
                1,
                "CCO",
                ApiResponse {
                    class_results: vec!["lipid".to_string()],
                    superclass_results: Vec::new(),
                    pathway_results: Vec::new(),
                    isglycoside: false,
                },
            )
            .expect("append");

        sync_runtime_state(&mut writer, &mut state, &mut chunk_index).expect("sync runtime state");
        assert_eq!(chunk_index.records().len(), 1);
        assert!(completed_dir.join("part-000001.jsonl.zst").exists());
    }

    #[test]
    fn publish_to_zenodo_skips_when_token_is_missing() {
        let _guard = env_lock();
        let previous = std::env::var("ZENODO_TOKEN").ok();
        unsafe {
            std::env::remove_var("ZENODO_TOKEN");
        }

        let temp_dir = TestDir::new("publish-skip");
        let state_dir = temp_dir.path().join("state");
        let completed_dir = temp_dir.path().join("completed");
        let mut state = StateStore::open(&state_dir, 2).expect("open state");
        let mut chunk_index =
            ChunkIndex::open(&state_dir.join("chunks.jsonl")).expect("open index");
        let mut writer = ChunkWriter::new(&completed_dir, 1024, chunk_index.next_chunk_id())
            .expect("open writer");
        let config = RuntimeConfig {
            completed_dir,
            state_dir,
            log_dir: temp_dir.path().join("logs"),
            release_dir: temp_dir.path().join("releases"),
            api_url: "http://127.0.0.1:9/classify".to_string(),
            ntfy_base: None,
            publish_interval: Duration::from_hours(168),
            retry_delays: [Duration::from_millis(0); 3],
            require_zenodo_token: false,
            install_ctrlc: false,
        };
        let counts = RuntimeCounts {
            total: 1,
            successful: 0,
            invalid: 0,
            failed: 0,
        };
        let agent = ureq::AgentBuilder::new().build();

        publish_to_zenodo(
            &mut writer,
            &mut state,
            &mut chunk_index,
            counts,
            &config,
            None,
            &agent,
            None,
        )
        .expect("publish skip");

        if let Some(previous) = previous {
            unsafe {
                std::env::set_var("ZENODO_TOKEN", previous);
            }
        }
    }

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock env mutex")
    }
}
