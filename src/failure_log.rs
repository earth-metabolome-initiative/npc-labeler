use std::fs::{File, OpenOptions, create_dir_all, metadata, remove_file, rename};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::Serialize;
#[cfg(test)]
use std::fs::read_to_string;

use crate::state::LineIndex;

const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;
const MAX_ROTATED_FILES: u8 = 5;

#[derive(Serialize)]
struct FailureRecord<'a> {
    ts: String,
    line: LineIndex,
    cid: i32,
    smiles: &'a str,
    kind: &'a str,
    message: &'a str,
    attempt: u8,
}

pub struct FailureLogger {
    log_dir: PathBuf,
    active_path: PathBuf,
    max_log_bytes: u64,
    max_rotated_files: u8,
    writer: BufWriter<File>,
}

impl FailureLogger {
    pub fn open(log_dir: &Path) -> io::Result<Self> {
        Self::open_with_limits(log_dir, MAX_LOG_BYTES, MAX_ROTATED_FILES)
    }

    fn open_with_limits(
        log_dir: &Path,
        max_log_bytes: u64,
        max_rotated_files: u8,
    ) -> io::Result<Self> {
        create_dir_all(log_dir)?;
        let active_path = log_dir.join("failures.log");
        let writer = open_writer(&active_path)?;
        Ok(Self {
            log_dir: log_dir.to_path_buf(),
            active_path,
            max_log_bytes,
            max_rotated_files,
            writer,
        })
    }

    pub fn log(
        &mut self,
        line: LineIndex,
        cid: i32,
        smiles: &str,
        kind: &str,
        message: &str,
        attempt: u8,
    ) -> io::Result<()> {
        let record = FailureRecord {
            ts: Utc::now().to_rfc3339(),
            line,
            cid,
            smiles,
            kind,
            message,
            attempt,
        };
        serde_json::to_writer(&mut self.writer, &record)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        self.rotate_if_needed()
    }

    fn rotate_if_needed(&mut self) -> io::Result<()> {
        let current_size = metadata(&self.active_path)?.len();
        if current_size <= self.max_log_bytes {
            return Ok(());
        }

        self.writer.flush()?;

        let oldest = self.rotated_path(self.max_rotated_files);
        if oldest.exists() {
            remove_file(&oldest)?;
        }

        for suffix in (1..self.max_rotated_files).rev() {
            let src = self.rotated_path(suffix);
            if src.exists() {
                rename(&src, self.rotated_path(suffix + 1))?;
            }
        }

        if self.active_path.exists() {
            rename(&self.active_path, self.rotated_path(1))?;
        }

        self.writer = open_writer(&self.active_path)?;
        Ok(())
    }

    fn rotated_path(&self, suffix: u8) -> PathBuf {
        self.log_dir.join(format!("failures.{suffix}.log"))
    }
}

fn open_writer(path: &Path) -> io::Result<BufWriter<File>> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(BufWriter::new(file))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, remove_dir_all};
    use uuid::Uuid;

    #[test]
    fn rotates_failure_logs_when_threshold_is_exceeded() {
        let temp_dir = TestDir::new("failure-log");
        let log_dir = temp_dir.path().join("logs");
        create_dir_all(&log_dir).expect("create log dir");
        let mut logger = FailureLogger::open_with_limits(&log_dir, 1, 2).expect("open logger");

        for attempt in 1..=4 {
            logger
                .log(
                    42,
                    99,
                    "CCCCCCCCCCCCCCCCCCCC",
                    "server_error",
                    "mock error for rotation",
                    attempt,
                )
                .expect("write failure record");
        }

        let active = read_to_string(log_dir.join("failures.log")).expect("read active log");
        let rotated = read_to_string(log_dir.join("failures.1.log")).expect("read rotated log");
        assert!(active.is_empty());
        assert!(rotated.contains("\"attempt\":4"));
        assert!(!log_dir.join("failures.3.log").exists());
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!("npc-labeler-{label}-{}", Uuid::new_v4()));
            create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = remove_dir_all(&self.path);
        }
    }
}
