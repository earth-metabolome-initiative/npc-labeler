use std::collections::HashSet;
use std::fs::{File, OpenOptions, create_dir_all, read_dir, remove_file, rename};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zstd::stream::write::Encoder;

use crate::api::ApiResponse;
use crate::state::{LineIndex, StateStore};

const COMPLETED_FILENAME: &str = "completed.jsonl.zst";
const MANIFEST_FILENAME: &str = "manifest.json";
const DATASET_SCHEMA_VERSION: u32 = 1;
const MANIFEST_VERSION: u32 = 1;
const ZSTD_LEVEL: i32 = 3;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChunkRecord {
    pub created_at: String,
    pub filename: String,
    pub first_line: LineIndex,
    pub last_line: LineIndex,
    pub row_count: u64,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Serialize)]
struct CompletedRecord {
    cid: i32,
    smiles: String,
    class_results: Vec<String>,
    superclass_results: Vec<String>,
    pathway_results: Vec<String>,
    isglycoside: bool,
}

#[derive(Serialize)]
struct ManifestChunk<'a> {
    filename: &'a str,
    row_count: u64,
    bytes: u64,
    sha256: &'a str,
}

#[derive(Serialize)]
struct Manifest<'a> {
    #[serde(rename = "manifest_version")]
    version: u32,
    #[serde(rename = "dataset_schema_version")]
    schema_version: u32,
    created_at: String,
    pubchem_total: u64,
    output_filename: &'a str,
    output_bytes: u64,
    output_sha256: &'a str,
    successful_rows: u64,
    invalid_rows: u64,
    failed_rows: u64,
    chunks: Vec<ManifestChunk<'a>>,
}

pub struct ReleaseArtifacts {
    pub output_path: PathBuf,
    pub manifest_path: PathBuf,
}

pub struct ChunkIndex {
    writer: BufWriter<File>,
    records: Vec<ChunkRecord>,
}

impl ChunkIndex {
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }

        let mut records = Vec::new();
        if path.exists() {
            let reader = BufReader::new(File::open(path)?);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let record = serde_json::from_str::<ChunkRecord>(&line)
                    .map_err(|error| io::Error::other(error.to_string()))?;
                records.push(record);
            }
        }

        let writer = BufWriter::new(OpenOptions::new().create(true).append(true).open(path)?);

        Ok(Self { writer, records })
    }

    pub fn append(&mut self, record: ChunkRecord) -> io::Result<()> {
        serde_json::to_writer(&mut self.writer, &record)
            .map_err(|error| io::Error::other(error.to_string()))?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.records.push(record);
        Ok(())
    }

    pub fn next_chunk_id(&self) -> u32 {
        self.records.len() as u32 + 1
    }

    pub fn records(&self) -> &[ChunkRecord] {
        &self.records
    }
}

pub struct ChunkWriter {
    completed_dir: PathBuf,
    target_bytes: u64,
    next_chunk_id: u32,
    encoder: Option<Encoder<'static, BufWriter<File>>>,
    current_filename: Option<String>,
    current_tmp_path: Option<PathBuf>,
    pending_lines: Vec<LineIndex>,
    first_line: Option<LineIndex>,
    last_line: Option<LineIndex>,
    row_count: u64,
}

impl ChunkWriter {
    pub fn new(completed_dir: &Path, target_bytes: u64, next_chunk_id: u32) -> io::Result<Self> {
        create_dir_all(completed_dir)?;
        Ok(Self {
            completed_dir: completed_dir.to_path_buf(),
            target_bytes,
            next_chunk_id,
            encoder: None,
            current_filename: None,
            current_tmp_path: None,
            pending_lines: Vec::new(),
            first_line: None,
            last_line: None,
            row_count: 0,
        })
    }

    pub fn append(
        &mut self,
        line: LineIndex,
        cid: i32,
        smiles: &str,
        response: ApiResponse,
    ) -> io::Result<()> {
        self.ensure_active()?;
        let encoder = self
            .encoder
            .as_mut()
            .ok_or_else(|| io::Error::other("missing active chunk encoder"))?;

        let record = CompletedRecord {
            cid,
            smiles: smiles.to_string(),
            class_results: response.class_results,
            superclass_results: response.superclass_results,
            pathway_results: response.pathway_results,
            isglycoside: response.isglycoside,
        };
        serde_json::to_writer(&mut *encoder, &record)
            .map_err(|error| io::Error::other(error.to_string()))?;
        encoder.write_all(b"\n")?;

        if self.first_line.is_none() {
            self.first_line = Some(line);
        }
        self.last_line = Some(line);
        self.pending_lines.push(line);
        self.row_count += 1;
        Ok(())
    }

    pub fn sync_active(&mut self) -> io::Result<u64> {
        let Some(encoder) = self.encoder.as_mut() else {
            return Ok(0);
        };
        encoder.flush()?;
        let writer = encoder.get_mut();
        writer.flush()?;
        writer.get_mut().sync_data()?;
        writer.get_ref().metadata().map(|metadata| metadata.len())
    }

    #[inline]
    pub fn should_rotate_for_size(&self, active_size: u64) -> bool {
        self.row_count > 0 && active_size >= self.target_bytes
    }

    pub fn seal_current(
        &mut self,
        state: &mut StateStore,
        chunk_index: &mut ChunkIndex,
    ) -> io::Result<Option<ChunkRecord>> {
        if self.row_count == 0 {
            return Ok(None);
        }

        let filename = self
            .current_filename
            .take()
            .ok_or_else(|| io::Error::other("missing active chunk filename"))?;
        let tmp_path = self
            .current_tmp_path
            .take()
            .ok_or_else(|| io::Error::other("missing active chunk tmp path"))?;
        let final_path = self.completed_dir.join(&filename);

        let encoder = self
            .encoder
            .take()
            .ok_or_else(|| io::Error::other("missing active chunk encoder"))?;
        let mut writer = encoder.finish()?;
        writer.flush()?;
        writer.get_mut().sync_data()?;

        rename(&tmp_path, &final_path)?;
        let bytes = final_path.metadata()?.len();
        let sha256 = sha256_file(&final_path)?;

        state.sync_terminal()?;

        let record = ChunkRecord {
            created_at: Utc::now().to_rfc3339(),
            filename,
            first_line: self
                .first_line
                .ok_or_else(|| io::Error::other("missing first line for chunk"))?,
            last_line: self
                .last_line
                .ok_or_else(|| io::Error::other("missing last line for chunk"))?,
            row_count: self.row_count,
            bytes,
            sha256,
        };
        chunk_index.append(record.clone())?;
        state.mark_done_batch(&self.pending_lines);
        state.sync_done()?;

        self.pending_lines.clear();
        self.first_line = None;
        self.last_line = None;
        self.row_count = 0;
        self.next_chunk_id += 1;

        Ok(Some(record))
    }

    fn ensure_active(&mut self) -> io::Result<()> {
        if self.encoder.is_some() {
            return Ok(());
        }

        let filename = format!("part-{:06}.jsonl.zst", self.next_chunk_id);
        let tmp_path = self.completed_dir.join(format!("{filename}.tmp"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        let writer = BufWriter::with_capacity(1024 * 1024, file);
        let encoder = Encoder::new(writer, ZSTD_LEVEL)?;

        self.current_filename = Some(filename);
        self.current_tmp_path = Some(tmp_path);
        self.encoder = Some(encoder);
        Ok(())
    }
}

pub fn cleanup_completed_dir(completed_dir: &Path, chunk_index: &ChunkIndex) -> io::Result<()> {
    create_dir_all(completed_dir)?;
    let indexed: HashSet<&str> = chunk_index
        .records()
        .iter()
        .map(|record| record.filename.as_str())
        .collect();

    for entry in read_dir(completed_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let path = entry.path();
        if name.ends_with(".tmp")
            || (name.ends_with(".jsonl.zst") && !indexed.contains(name.as_ref()))
        {
            remove_file(path)?;
        }
    }
    Ok(())
}

pub fn cleanup_release_staging(release_dir: &Path) -> io::Result<()> {
    create_dir_all(release_dir)?;
    for filename in [COMPLETED_FILENAME, MANIFEST_FILENAME] {
        let path = release_dir.join(filename);
        if path.exists() {
            remove_file(path)?;
        }
    }
    Ok(())
}

pub fn build_release(
    completed_dir: &Path,
    release_dir: &Path,
    chunk_index: &ChunkIndex,
    pubchem_total: u64,
    successful_rows: u64,
    invalid_rows: u64,
    failed_rows: u64,
) -> io::Result<ReleaseArtifacts> {
    cleanup_release_staging(release_dir)?;

    let output_path = release_dir.join(COMPLETED_FILENAME);
    let manifest_path = release_dir.join(MANIFEST_FILENAME);
    let mut output = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&output_path)?,
    );
    let mut hasher = Sha256::new();
    let mut total_bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];

    for record in chunk_index.records() {
        let chunk_path = completed_dir.join(&record.filename);
        let mut chunk = BufReader::new(File::open(&chunk_path)?);
        loop {
            let read = chunk.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            output.write_all(&buffer[..read])?;
            hasher.update(&buffer[..read]);
            total_bytes += read as u64;
        }
    }

    output.flush()?;
    output.get_ref().sync_data()?;
    let output_sha256 = format!("{:x}", hasher.finalize());

    let chunks = chunk_index
        .records()
        .iter()
        .map(|record| ManifestChunk {
            filename: &record.filename,
            row_count: record.row_count,
            bytes: record.bytes,
            sha256: &record.sha256,
        })
        .collect();
    let manifest = Manifest {
        version: MANIFEST_VERSION,
        schema_version: DATASET_SCHEMA_VERSION,
        created_at: Utc::now().to_rfc3339(),
        pubchem_total,
        output_filename: COMPLETED_FILENAME,
        output_bytes: total_bytes,
        output_sha256: &output_sha256,
        successful_rows,
        invalid_rows,
        failed_rows,
        chunks,
    };
    let mut manifest_writer = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&manifest_path)?,
    );
    serde_json::to_writer(&mut manifest_writer, &manifest)
        .map_err(|error| io::Error::other(error.to_string()))?;
    manifest_writer.flush()?;
    manifest_writer.get_ref().sync_data()?;

    Ok(ReleaseArtifacts {
        output_path,
        manifest_path,
    })
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, read_to_string, remove_dir_all, write};
    use uuid::Uuid;
    use zstd::stream::read::Decoder;

    #[test]
    fn seals_chunks_marks_done_bits_and_builds_release() {
        let temp_dir = TestDir::new("output");
        let completed_dir = temp_dir.path().join("completed");
        let state_dir = temp_dir.path().join("state");
        let release_dir = temp_dir.path().join("releases");
        let chunks_path = state_dir.join("chunks.jsonl");

        let mut state = StateStore::open(&state_dir, 8).expect("open state");
        let mut chunk_index = ChunkIndex::open(&chunks_path).expect("open chunk index");
        let mut writer = ChunkWriter::new(&completed_dir, 1024 * 1024, chunk_index.next_chunk_id())
            .expect("open chunk writer");

        state.mark_invalid(1);
        state.sync_terminal().expect("sync invalid state");
        writer
            .append(0, 100, "CCO", mock_response("lipid"))
            .expect("append first record");
        writer
            .append(2, 101, "CCC", mock_response(""))
            .expect("append second record");
        let chunk = writer
            .seal_current(&mut state, &mut chunk_index)
            .expect("seal chunk")
            .expect("sealed record");

        assert_eq!(chunk.first_line, 0);
        assert_eq!(chunk.last_line, 2);
        assert_eq!(chunk.row_count, 2);
        assert_eq!(
            state
                .rebuild_done_from_chunks(chunk_index.records())
                .expect("rebuild done"),
            2
        );
        assert!(state.is_terminal(0));
        assert!(state.is_terminal(1));
        assert!(state.is_terminal(2));
        assert_eq!(chunk_index.records().len(), 1);

        let release = build_release(&completed_dir, &release_dir, &chunk_index, 8, 2, 1, 0)
            .expect("build release");
        let merged = read_zstd_lines(&release.output_path);
        assert_eq!(merged.len(), 2);
        assert!(merged[0].contains("\"cid\":100"));
        assert!(merged[1].contains("\"cid\":101"));

        let manifest = read_to_string(&release.manifest_path).expect("read manifest");
        assert!(manifest.contains("\"manifest_version\":1"));
        assert!(manifest.contains("\"dataset_schema_version\":1"));
        assert!(manifest.contains("\"pubchem_total\":8"));
        assert!(manifest.contains("\"successful_rows\":2"));
        assert!(manifest.contains(&chunk.filename));
    }

    #[test]
    fn cleanup_completed_dir_removes_unindexed_files() {
        let temp_dir = TestDir::new("cleanup");
        let completed_dir = temp_dir.path().join("completed");
        let state_dir = temp_dir.path().join("state");
        create_dir_all(&completed_dir).expect("create completed dir");
        create_dir_all(&state_dir).expect("create state dir");

        let kept = completed_dir.join("part-000001.jsonl.zst");
        let stale = completed_dir.join("part-000002.jsonl.zst");
        let tmp = completed_dir.join("part-000003.jsonl.zst.tmp");
        write(&kept, b"kept").expect("write kept chunk");
        write(&stale, b"stale").expect("write stale chunk");
        write(&tmp, b"tmp").expect("write tmp chunk");

        let chunks_path = state_dir.join("chunks.jsonl");
        let mut chunk_index = ChunkIndex::open(&chunks_path).expect("open chunk index");
        chunk_index
            .append(ChunkRecord {
                created_at: Utc::now().to_rfc3339(),
                filename: "part-000001.jsonl.zst".to_string(),
                first_line: 0,
                last_line: 0,
                row_count: 1,
                bytes: 4,
                sha256: "hash".to_string(),
            })
            .expect("append chunk record");

        cleanup_completed_dir(&completed_dir, &chunk_index).expect("cleanup completed dir");

        assert!(kept.exists());
        assert!(!stale.exists());
        assert!(!tmp.exists());
    }

    #[test]
    fn chunk_writer_noop_paths_and_release_cleanup_are_stable() {
        let temp_dir = TestDir::new("output-noop");
        let completed_dir = temp_dir.path().join("completed");
        let state_dir = temp_dir.path().join("state");
        let release_dir = temp_dir.path().join("releases");

        let mut state = StateStore::open(&state_dir, 1).expect("open state");
        let mut chunk_index =
            ChunkIndex::open(&state_dir.join("chunks.jsonl")).expect("open chunk index");
        let mut writer =
            ChunkWriter::new(&completed_dir, 1024, chunk_index.next_chunk_id()).expect("writer");

        assert_eq!(writer.sync_active().expect("sync active"), 0);
        assert!(!writer.should_rotate_for_size(0));
        assert!(
            writer
                .seal_current(&mut state, &mut chunk_index)
                .expect("seal current")
                .is_none()
        );

        create_dir_all(&release_dir).expect("create release dir");
        write(release_dir.join(COMPLETED_FILENAME), b"old-output").expect("write staged output");
        write(release_dir.join(MANIFEST_FILENAME), b"old-manifest").expect("write staged manifest");

        cleanup_release_staging(&release_dir).expect("cleanup release staging");
        assert!(!release_dir.join(COMPLETED_FILENAME).exists());
        assert!(!release_dir.join(MANIFEST_FILENAME).exists());
    }

    #[test]
    fn chunk_index_open_skips_blank_lines() {
        let temp_dir = TestDir::new("chunk-index");
        let chunks_path = temp_dir.path().join("chunks.jsonl");
        write(
            &chunks_path,
            format!(
                "\n{}\n\n",
                serde_json::to_string(&ChunkRecord {
                    created_at: Utc::now().to_rfc3339(),
                    filename: "part-000001.jsonl.zst".to_string(),
                    first_line: 0,
                    last_line: 0,
                    row_count: 1,
                    bytes: 4,
                    sha256: "hash".to_string(),
                })
                .expect("serialize chunk record")
            ),
        )
        .expect("write chunk index");

        let chunk_index = ChunkIndex::open(&chunks_path).expect("open chunk index");
        assert_eq!(chunk_index.records().len(), 1);
        assert_eq!(chunk_index.records()[0].filename, "part-000001.jsonl.zst");
    }

    fn mock_response(label: &str) -> ApiResponse {
        ApiResponse {
            class_results: if label.is_empty() {
                Vec::new()
            } else {
                vec![label.to_string()]
            },
            superclass_results: Vec::new(),
            pathway_results: Vec::new(),
            isglycoside: false,
        }
    }

    fn read_zstd_lines(path: &Path) -> Vec<String> {
        let file = File::open(path).expect("open merged release");
        let decoder = Decoder::new(file).expect("create decoder");
        let reader = BufReader::new(decoder);
        reader
            .lines()
            .collect::<Result<Vec<_>, _>>()
            .expect("read merged lines")
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
