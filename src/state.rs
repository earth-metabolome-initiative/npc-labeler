use std::fs::{File, OpenOptions, create_dir_all};
use std::io;
use std::path::Path;

use memmap2::MmapMut;

use crate::output::ChunkRecord;

pub type LineIndex = u32;

pub struct StateStore {
    done: MmapBitVec,
    invalid: MmapBitVec,
    failed: MmapBitVec,
    done_dirty: bool,
    invalid_dirty: bool,
    failed_dirty: bool,
}

impl StateStore {
    pub fn open(state_dir: &Path, total_rows: usize) -> io::Result<Self> {
        create_dir_all(state_dir)?;
        let done = MmapBitVec::open(&state_dir.join("done.bitvec"), total_rows, true)?;
        let invalid = MmapBitVec::open(&state_dir.join("invalid.bitvec"), total_rows, true)?;
        let failed = MmapBitVec::open(&state_dir.join("failed.bitvec"), total_rows, true)?;

        Ok(Self {
            done,
            invalid,
            failed,
            done_dirty: false,
            invalid_dirty: false,
            failed_dirty: false,
        })
    }

    pub fn rebuild_done_from_chunks(&mut self, chunks: &[ChunkRecord]) -> io::Result<u64> {
        self.done.clear_all();
        for chunk in chunks {
            for line in chunk.first_line..=chunk.last_line {
                let index = line as usize;
                if !self.invalid.get(index) && !self.failed.get(index) {
                    self.done.set(index);
                }
            }
        }
        self.done.flush()?;
        self.done_dirty = false;
        Ok(self.done.count_ones())
    }

    #[inline]
    pub fn is_terminal(&self, line: LineIndex) -> bool {
        let index = line as usize;
        self.done.get(index) || self.invalid.get(index) || self.failed.get(index)
    }

    #[inline]
    pub fn mark_invalid(&mut self, line: LineIndex) {
        self.invalid.set(line as usize);
        self.invalid_dirty = true;
    }

    #[inline]
    pub fn mark_failed(&mut self, line: LineIndex) {
        self.failed.set(line as usize);
        self.failed_dirty = true;
    }

    #[inline]
    pub fn mark_done_batch(&mut self, lines: &[LineIndex]) {
        for &line in lines {
            self.done.set(line as usize);
        }
        self.done_dirty |= !lines.is_empty();
    }

    pub fn sync_terminal(&mut self) -> io::Result<()> {
        if self.invalid_dirty {
            self.invalid.flush()?;
            self.invalid_dirty = false;
        }
        if self.failed_dirty {
            self.failed.flush()?;
            self.failed_dirty = false;
        }
        Ok(())
    }

    pub fn sync_done(&mut self) -> io::Result<()> {
        if self.done_dirty {
            self.done.flush()?;
            self.done_dirty = false;
        }
        Ok(())
    }

    pub fn count_invalid(&self) -> u64 {
        self.invalid.count_ones()
    }

    pub fn count_failed(&self) -> u64 {
        self.failed.count_ones()
    }
}

struct MmapBitVec {
    file: File,
    map: MmapMut,
}

impl MmapBitVec {
    fn open(path: &Path, total_bits: usize, allow_recreate: bool) -> io::Result<Self> {
        let byte_len = total_bits.div_ceil(8).max(1);
        let existed = path.exists();
        let mut recreate = false;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let current_len = file.metadata()?.len();
        if current_len == 0 {
            file.set_len(byte_len as u64)?;
        } else if current_len != byte_len as u64 {
            if allow_recreate {
                file.set_len(byte_len as u64)?;
                recreate = true;
            } else if existed {
                return Err(io::Error::other(format!(
                    "{} has size {}, expected {}",
                    path.display(),
                    current_len,
                    byte_len
                )));
            }
        }

        let mut map = unsafe { MmapMut::map_mut(&file)? };
        if recreate {
            map.fill(0);
            map.flush()?;
            file.sync_data()?;
        }
        Ok(Self { file, map })
    }

    fn clear_all(&mut self) {
        self.map.fill(0);
    }

    fn count_ones(&self) -> u64 {
        self.map
            .iter()
            .map(|byte| u64::from(byte.count_ones()))
            .sum()
    }

    #[inline]
    fn get(&self, index: usize) -> bool {
        let byte = index / 8;
        let mask = 1_u8 << (index % 8);
        self.map[byte] & mask != 0
    }

    #[inline]
    fn set(&mut self, index: usize) {
        let byte = index / 8;
        let mask = 1_u8 << (index % 8);
        self.map[byte] |= mask;
    }

    fn flush(&mut self) -> io::Result<()> {
        self.map.flush()?;
        self.file.sync_data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{OpenOptions, create_dir_all, remove_dir_all};
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn rebuilds_done_bits_without_overwriting_invalid_or_failed() {
        let temp_dir = TestDir::new("state");
        let mut state = StateStore::open(temp_dir.path(), 16).expect("open state");
        state.mark_invalid(1);
        state.mark_failed(3);
        state.sync_terminal().expect("sync terminal state");

        let chunks = vec![
            ChunkRecord {
                created_at: "2026-03-26T00:00:00Z".to_string(),
                filename: "part-000001.jsonl.zst".to_string(),
                first_line: 0,
                last_line: 2,
                row_count: 3,
                bytes: 12,
                sha256: "abc".to_string(),
            },
            ChunkRecord {
                created_at: "2026-03-26T00:00:00Z".to_string(),
                filename: "part-000002.jsonl.zst".to_string(),
                first_line: 3,
                last_line: 4,
                row_count: 2,
                bytes: 12,
                sha256: "def".to_string(),
            },
        ];

        let done_rows = state
            .rebuild_done_from_chunks(&chunks)
            .expect("rebuild done bits");

        assert_eq!(done_rows, 3);
        assert!(state.is_terminal(0));
        assert!(state.is_terminal(1));
        assert!(state.is_terminal(2));
        assert!(state.is_terminal(3));
        assert!(state.is_terminal(4));
        assert_eq!(state.count_invalid(), 1);
        assert_eq!(state.count_failed(), 1);
    }

    #[test]
    fn recreates_wrong_sized_bitvec_files_when_allowed() {
        let temp_dir = TestDir::new("state-recreate");
        let path = temp_dir.path().join("done.bitvec");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .expect("create mismatched file");
        file.set_len(64).expect("seed wrong size");
        drop(file);

        let bitvec = MmapBitVec::open(&path, 8, true).expect("recreate bitvec");
        assert_eq!(bitvec.file.metadata().expect("metadata").len(), 1);
        assert_eq!(bitvec.count_ones(), 0);
    }

    #[test]
    fn rejects_wrong_sized_bitvec_files_when_recreate_is_disabled() {
        let temp_dir = TestDir::new("state-error");
        let path = temp_dir.path().join("done.bitvec");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .expect("create mismatched file");
        file.set_len(64).expect("seed wrong size");
        drop(file);

        let error = match MmapBitVec::open(&path, 8, false) {
            Ok(_) => panic!("expected size mismatch"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("expected 1"));
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
