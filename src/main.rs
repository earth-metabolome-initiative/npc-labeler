mod api;
mod db;
mod export;
mod models;
mod schema;
mod ui;
mod zenodo;

use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::Parser;
use diesel::RunQueryDsl;
use flate2::read::GzDecoder;

use api::ClassifyError;
use models::{ClassificationUpdate, NewClassification};
use ui::Ui;

const MAX_ATTEMPTS: i32 = 3;
const NTFY_BASE: &str = "https://ntfy.sh";
const DASHBOARD_INTERVAL: Duration = Duration::from_secs(2);
const PUBLISH_INTERVAL: Duration = Duration::from_hours(168); // 7 days

#[derive(Parser)]
#[command(name = "npc-labeler")]
#[command(about = "Classify PubChem SMILES using the NPClassifier API")]
struct Args {
    /// Path to CID-SMILES input file (.gz or plain). Omit to resume from existing DB.
    #[arg(long)]
    input: Option<String>,

    /// Path to `SQLite` database
    #[arg(long, default_value = "classifications.sqlite")]
    db: String,
}

fn load_smiles(conn: &mut diesel::SqliteConnection, input_path: &str) {
    eprintln!("Loading SMILES from {input_path}...");
    let file = match std::fs::File::open(input_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error: cannot open {input_path}: {e}");
            std::process::exit(1);
        }
    };

    let reader: Box<dyn BufRead> = if std::path::Path::new(input_path)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        Box::new(BufReader::with_capacity(
            8 * 1024 * 1024,
            GzDecoder::new(file),
        ))
    } else {
        Box::new(BufReader::with_capacity(8 * 1024 * 1024, file))
    };

    let mut batch: Vec<(i32, String)> = Vec::with_capacity(10_000);
    let mut total_lines: u64 = 0;
    let mut inserted: u64 = 0;

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Warning: skipping unreadable line: {e}");
                continue;
            }
        };
        let Some((cid_str, smiles)) = line.split_once('\t') else {
            continue;
        };
        let Ok(cid) = cid_str.parse::<i32>() else {
            continue;
        };

        batch.push((cid, smiles.trim().to_string()));
        total_lines += 1;

        if batch.len() >= 10_000 {
            flush_batch(conn, &batch, &mut inserted);
            batch.clear();
            if total_lines.is_multiple_of(100_000) {
                eprintln!("[load] {total_lines} lines read, {inserted} inserted");
            }
        }
    }

    if !batch.is_empty() {
        flush_batch(conn, &batch, &mut inserted);
    }

    eprintln!("[load] done: {total_lines} lines read, {inserted} inserted");
}

fn flush_batch(conn: &mut diesel::SqliteConnection, batch: &[(i32, String)], inserted: &mut u64) {
    let rows: Vec<NewClassification> = batch
        .iter()
        .map(|(cid, smiles)| NewClassification {
            cid: *cid,
            smiles: smiles.as_str(),
            status: "pending",
        })
        .collect();
    match db::bulk_insert(conn, &rows) {
        Ok(n) => *inserted += n as u64,
        Err(e) => eprintln!("Warning: batch insert error: {e}"),
    }
}

fn make_update(result: Result<api::ApiResponse, ClassifyError>) -> (ClassificationUpdate, bool) {
    let now = Utc::now().naive_utc();
    match result {
        Ok(resp) => {
            let has_results = !resp.class_results.is_empty()
                || !resp.superclass_results.is_empty()
                || !resp.pathway_results.is_empty();
            let update = ClassificationUpdate {
                class_results: Some(serde_json::to_string(&resp.class_results).unwrap()),
                superclass_results: Some(serde_json::to_string(&resp.superclass_results).unwrap()),
                pathway_results: Some(serde_json::to_string(&resp.pathway_results).unwrap()),
                isglycoside: Some(resp.isglycoside),
                status: if has_results { "classified" } else { "empty" }.to_string(),
                classified_at: Some(now),
                ..Default::default()
            };
            (update, true)
        }
        Err(ClassifyError::InvalidSmiles) => {
            let update = ClassificationUpdate {
                status: "invalid".to_string(),
                last_error: Some("Invalid SMILES (HTTP 500)".to_string()),
                classified_at: Some(now),
                ..Default::default()
            };
            (update, true)
        }
        Err(ClassifyError::RateLimit) => {
            let update = ClassificationUpdate {
                last_error: Some("Rate limited (HTTP 429)".to_string()),
                ..Default::default()
            };
            (update, false)
        }
        Err(e) => {
            let msg = match &e {
                ClassifyError::ServerError(code) => format!("Server error (HTTP {code})"),
                ClassifyError::ParseError(detail) => format!("JSON parse error: {detail}"),
                ClassifyError::NetworkError(msg) => format!("Network error: {msg}"),
                _ => unreachable!(),
            };
            let update = ClassificationUpdate {
                status: "failed".to_string(),
                last_error: Some(msg),
                ..Default::default()
            };
            (update, true)
        }
    }
}

fn notify(agent: &ureq::Agent, ntfy_url: &str, message: &str) {
    if let Err(e) = agent.post(ntfy_url).send_string(message) {
        eprintln!("[ntfy] failed to send notification: {e}");
    }
}

fn publish_to_zenodo(conn: &mut diesel::SqliteConnection, agent: &ureq::Agent, ntfy_url: &str) {
    let zenodo_token = match std::env::var("ZENODO_TOKEN") {
        Ok(t) if !t.is_empty() => t,
        _ => {
            eprintln!("[zenodo] ZENODO_TOKEN not set, skipping publish");
            return;
        }
    };

    let total = db::count_total(conn);
    let classified = db::count_by_status(conn, "classified");
    let empty = db::count_by_status(conn, "empty");
    let done = classified + empty;

    let parquet_path = "classifications.parquet";
    eprintln!("[publish] exporting {done} rows to {parquet_path}...");
    export::export_parquet(conn, parquet_path);

    eprintln!("[publish] uploading to Zenodo...");
    match zenodo::publish(&zenodo_token, parquet_path, classified, empty, total) {
        Ok(doi) => {
            notify(
                agent,
                ntfy_url,
                &format!("Published to Zenodo: {doi} ({done}/{total})"),
            );
        }
        Err(e) => {
            eprintln!("[zenodo] publish failed: {e}");
            notify(agent, ntfy_url, &format!("Zenodo publish FAILED: {e}"));
        }
    }

    let _ = std::fs::remove_file(parquet_path);
}

#[allow(clippy::too_many_lines)]
fn main() {
    dotenvy::dotenv().ok();

    let args = Args::parse();
    let mut conn = db::initialize(&args.db);

    if let Some(ref input) = args.input {
        load_smiles(&mut conn, input);
    }

    let total = db::count_total(&mut conn);
    let pending = db::count_by_status(&mut conn, "pending");

    if pending == 0 {
        eprintln!("DB: {total} total, 0 pending. Nothing to do.");
        return;
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let s = shutdown.clone();
        ctrlc::set_handler(move || {
            if s.load(Ordering::Relaxed) {
                eprintln!("\nForce exit.");
                std::process::exit(1);
            }
            eprintln!("\nShutdown requested, finishing current request...");
            s.store(true, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");
    }

    let agent = ureq::AgentBuilder::new()
        .timeout_read(Duration::from_secs(15))
        .timeout_write(Duration::from_secs(5))
        .timeout_connect(Duration::from_secs(10))
        .build();

    let ntfy_topic = uuid::Uuid::new_v4().to_string();
    let ntfy_url = format!("{NTFY_BASE}/{ntfy_topic}");

    let mut ui = Ui::new(ntfy_url.clone());

    eprintln!("[ntfy] subscribe for updates: {ntfy_url}");
    notify(
        &agent,
        &ntfy_url,
        &format!("npc-labeler started: {pending}/{total} pending"),
    );

    let terminal = ui.enter_terminal();

    // Main classification loop with periodic Zenodo publish
    let mut consecutive_db_errors: u32 = 0;
    let initial_done = total - pending;
    let mut last_notified_pct: u64 = (initial_done as f64 / total as f64 * 100.0) as u64;
    let mut last_dashboard = Instant::now();
    let mut last_publish = Instant::now();
    let start = Instant::now();
    let mut classified_this_pass: u64 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Weekly publish check
        if last_publish.elapsed() >= PUBLISH_INTERVAL {
            publish_to_zenodo(&mut conn, &agent, &ntfy_url);
            last_publish = Instant::now();
        }

        let Some((cid, smiles)) = db::get_next_pending(&mut conn) else {
            break;
        };

        ui.note_current(cid, &smiles);

        let result = api::classify(&agent, &smiles);

        match &result {
            Ok(_) => {}
            Err(ClassifyError::RateLimit) => ui.note_rate_limit(cid),
            Err(ClassifyError::InvalidSmiles) => ui.note_invalid(cid),
            Err(e) => ui.note_error(cid, &format!("{e:?}")),
        }

        let (update, increment_attempts) = make_update(result);
        let status = update.status.clone();

        if let Err(e) = db::update_one(&mut conn, cid, update, increment_attempts) {
            consecutive_db_errors += 1;
            ui.note_error(cid, &format!("DB error: {e}"));
            if consecutive_db_errors >= 10 {
                break;
            }
            let _ = diesel::sql_query(
                "UPDATE classifications SET status = 'failed', last_error = 'DB write error', attempts = attempts + 1 WHERE cid = ?"
            )
            .bind::<diesel::sql_types::Integer, _>(cid)
            .execute(&mut conn);
            continue;
        }

        consecutive_db_errors = 0;

        match status.as_str() {
            "classified" => ui.note_classified(cid),
            "empty" => ui.note_empty(cid),
            _ => {}
        }

        if increment_attempts {
            classified_this_pass += 1;
        }

        if last_dashboard.elapsed() >= DASHBOARD_INTERVAL {
            ui.render(&mut conn);
            last_dashboard = Instant::now();

            let done = initial_done + classified_this_pass as i64;
            let pct = done as f64 / total as f64 * 100.0;
            let pct_int = pct as u64;
            if pct_int > last_notified_pct {
                let rate = classified_this_pass as f64 / start.elapsed().as_secs_f64();
                notify(
                    &agent,
                    &ntfy_url,
                    &format!("{pct_int}% -- {done}/{total} ({rate:.1}/s)"),
                );
                last_notified_pct = pct_int;
            }
        }

        if classified_this_pass > 0 && classified_this_pass.is_multiple_of(10_000) {
            db::wal_checkpoint(&mut conn);
        }
    }

    drop(terminal);

    // Retry failed rows
    if !shutdown.load(Ordering::Relaxed) {
        for round in 1..MAX_ATTEMPTS {
            let reset = db::reset_failed_for_retry(&mut conn, MAX_ATTEMPTS);
            if reset == 0 {
                break;
            }
            eprintln!("--- Retry round {round}: {reset} failed rows ---");
            // Simple retry loop without dashboard
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let Some((cid, smiles)) = db::get_next_pending(&mut conn) else {
                    break;
                };
                let result = api::classify(&agent, &smiles);
                let (update, inc) = make_update(result);
                if let Err(e) = db::update_one(&mut conn, cid, update, inc) {
                    eprintln!("DB error on retry CID {cid}: {e}");
                }
            }
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
        }
    }

    db::wal_checkpoint(&mut conn);

    let done = total - db::count_by_status(&mut conn, "pending");
    notify(
        &agent,
        &ntfy_url,
        &format!("npc-labeler finished: {done}/{total} done"),
    );

    print_summary(&mut conn, total);
}

fn print_summary(conn: &mut diesel::SqliteConnection, total: i64) {
    let classified = db::count_by_status(conn, "classified");
    let empty = db::count_by_status(conn, "empty");
    let invalid = db::count_by_status(conn, "invalid");
    let failed = db::count_by_status(conn, "failed");
    let pending = db::count_by_status(conn, "pending");

    println!("\n=== Summary ===");
    println!("Total:      {total}");
    println!("Classified: {classified}");
    println!("Empty:      {empty}");
    println!("Invalid:    {invalid}");
    println!("Failed:     {failed}");
    println!("Pending:    {pending}");
}
