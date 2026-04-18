from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Mapping, Optional, cast

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

MANIFEST_FILENAME = "manifest.json"
VOCABULARY_FILENAME = "vocabulary.json"
DATASET_SCHEMA_VERSION = 5
MANIFEST_VERSION = 6
PARQUET_CHUNK_ROWS = 10_000_000

ROWS_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("cid", pa.int64()),
        pa.field("smiles", pa.string()),
        pa.field("pathway_ids", pa.list_(pa.uint16())),
        pa.field("superclass_ids", pa.list_(pa.uint16())),
        pa.field("class_ids", pa.list_(pa.uint16())),
        pa.field("isglycoside", pa.bool_()),
        pa.field("parse_failed", pa.bool_()),
        pa.field("rdkit_failed", pa.bool_()),
        pa.field("other_failure", pa.bool_()),
        pa.field("error_message", pa.string()),
    ]
)

PATHWAY_VECTOR_COLUMN = "pathway_prediction_vector"
SUPERCLASS_VECTOR_COLUMN = "superclass_prediction_vector"
CLASS_VECTOR_COLUMN = "class_prediction_vector"
VECTOR_COLUMN_NAMES = [
    PATHWAY_VECTOR_COLUMN,
    SUPERCLASS_VECTOR_COLUMN,
    CLASS_VECTOR_COLUMN,
]

ROWS_STAGING_SUFFIX = ".rows.parquet"
VECTOR_STAGING_SUFFIXES = {
    PATHWAY_VECTOR_COLUMN: ".pathway-vectors.f16.bin",
    SUPERCLASS_VECTOR_COLUMN: ".superclass-vectors.f16.bin",
    CLASS_VECTOR_COLUMN: ".class-vectors.f16.bin",
}
VECTOR_COMPLETED_SUFFIXES = {
    PATHWAY_VECTOR_COLUMN: ".pathway-vectors.f16.zst",
    SUPERCLASS_VECTOR_COLUMN: ".superclass-vectors.f16.zst",
    CLASS_VECTOR_COLUMN: ".class-vectors.f16.zst",
}
VECTOR_MANIFEST_FIELDS = {
    PATHWAY_VECTOR_COLUMN: "pathway_vector",
    SUPERCLASS_VECTOR_COLUMN: "superclass_vector",
    CLASS_VECTOR_COLUMN: "class_vector",
}
VECTOR_DTYPE = np.dtype("<f2")

ROWS_COLUMNS = [
    "cid",
    "smiles",
    "pathway_ids",
    "superclass_ids",
    "class_ids",
    "isglycoside",
    "parse_failed",
    "rdkit_failed",
    "other_failure",
    "error_message",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class ChunkRecord:
    created_at: str
    filename: str
    first_row: int
    last_row: int
    row_count: int
    bytes: int
    sha256: str
    pathway_vector_filename: str
    pathway_vector_bytes: int
    pathway_vector_sha256: str
    pathway_vector_shape: List[int]
    superclass_vector_filename: str
    superclass_vector_bytes: int
    superclass_vector_sha256: str
    superclass_vector_shape: List[int]
    class_vector_filename: str
    class_vector_bytes: int
    class_vector_sha256: str
    class_vector_shape: List[int]

    @classmethod
    def from_payload(cls, payload: Dict[str, object]) -> "ChunkRecord":
        return cls(
            created_at=_require_str(payload, "created_at"),
            filename=_require_str(payload, "filename"),
            first_row=_require_int(payload, "first_row"),
            last_row=_require_int(payload, "last_row"),
            row_count=_require_int(payload, "row_count"),
            bytes=_require_int(payload, "bytes"),
            sha256=_require_str(payload, "sha256"),
            pathway_vector_filename=_get_str(payload, "pathway_vector_filename", ""),
            pathway_vector_bytes=_get_int(payload, "pathway_vector_bytes", 0),
            pathway_vector_sha256=_get_str(payload, "pathway_vector_sha256", ""),
            pathway_vector_shape=_get_int_list(payload, "pathway_vector_shape"),
            superclass_vector_filename=_get_str(payload, "superclass_vector_filename", ""),
            superclass_vector_bytes=_get_int(payload, "superclass_vector_bytes", 0),
            superclass_vector_sha256=_get_str(payload, "superclass_vector_sha256", ""),
            superclass_vector_shape=_get_int_list(payload, "superclass_vector_shape"),
            class_vector_filename=_get_str(payload, "class_vector_filename", ""),
            class_vector_bytes=_get_int(payload, "class_vector_bytes", 0),
            class_vector_sha256=_get_str(payload, "class_vector_sha256", ""),
            class_vector_shape=_get_int_list(payload, "class_vector_shape"),
        )

    def completed_filenames(self) -> List[str]:
        filenames = [self.filename]
        for sidecar in (
            self.pathway_vector_filename,
            self.superclass_vector_filename,
            self.class_vector_filename,
        ):
            if sidecar:
                filenames.append(sidecar)
        return filenames


class ChunkIndex:
    def __init__(self, path: Path, records: List[ChunkRecord]) -> None:
        self.path = path
        self.records = records

    @classmethod
    def open(cls, path: Path) -> "ChunkIndex":
        records: List[ChunkRecord] = []
        if path.exists():
            for line in path.read_text().splitlines():
                if not line.strip():
                    continue
                payload = json.loads(line)
                records.append(ChunkRecord.from_payload(payload))
        return cls(path=path, records=records)

    def append(self, record: ChunkRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record.__dict__, sort_keys=True))
            handle.write("\n")
        self.records.append(record)

    def next_chunk_id(self) -> int:
        return len(self.records) + 1


def cleanup_completed_dir(completed_dir: Path, chunk_index: ChunkIndex) -> None:
    completed_dir.mkdir(parents=True, exist_ok=True)
    indexed = {
        filename
        for record in chunk_index.records
        for filename in record.completed_filenames()
    }
    for path in completed_dir.iterdir():
        if not path.is_file():
            continue
        if path.name.endswith(".tmp"):
            path.unlink()
            continue
        if (
            (path.name.endswith(".parquet") or path.name.endswith(".zst"))
            and path.name not in indexed
        ):
            path.unlink()


def cleanup_staging_dir(staging_dir: Path) -> None:
    if staging_dir.exists():
        shutil.rmtree(staging_dir)


def cleanup_release_staging(release_dir: Path) -> None:
    release_dir.mkdir(parents=True, exist_ok=True)
    path = release_dir / MANIFEST_FILENAME
    if path.exists():
        path.unlink()


def _require_str(payload: Dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError("expected string payload for {0}".format(key))
    return value


def _require_int(payload: Dict[str, object], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise ValueError("expected integer payload for {0}".format(key))
    return value


def _get_str(payload: Dict[str, object], key: str, default: str) -> str:
    value = payload.get(key, default)
    if not isinstance(value, str):
        raise ValueError("expected string payload for {0}".format(key))
    return value


def _get_int(payload: Dict[str, object], key: str, default: int) -> int:
    value = payload.get(key, default)
    if not isinstance(value, int):
        raise ValueError("expected integer payload for {0}".format(key))
    return value


def _get_int_list(payload: Dict[str, object], key: str) -> List[int]:
    value = payload.get(key, [])
    if not isinstance(value, list) or not all(isinstance(item, int) for item in value):
        raise ValueError("expected integer list payload for {0}".format(key))
    return cast(List[int], value)


def _staging_part_path(
    chunk_dir: Path, chunk_id: int, part_id: int, suffix: str
) -> Path:
    filename = "batch-{0:06d}-{1:06d}{2}".format(chunk_id, part_id, suffix)
    return chunk_dir / filename


def _project_rows_record(record: Dict[str, object]) -> Dict[str, object]:
    return {column: record.get(column) for column in ROWS_COLUMNS}


def _vector_row(
    record: Dict[str, object], column_name: str, width: int
) -> np.ndarray:
    values = record.get(column_name)
    if values is None:
        return np.full((width,), np.nan, dtype=VECTOR_DTYPE)
    vector = np.asarray(values, dtype=VECTOR_DTYPE)
    if vector.shape != (width,):
        raise ValueError(
            "expected {0} values for {1}, got {2}".format(
                width, column_name, vector.shape
            )
        )
    return vector


def _write_rows_table(
    path: Path, schema: pa.Schema, records: List[Dict[str, object]]
) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, tmp_path, compression="zstd")
    tmp_path.replace(path)


def _write_vector_matrix(
    path: Path,
    *,
    column_name: str,
    width: int,
    records: List[Dict[str, object]],
) -> None:
    matrix = np.empty((len(records), width), dtype=VECTOR_DTYPE)
    for index, record in enumerate(records):
        matrix[index] = _vector_row(record, column_name, width)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(matrix.tobytes(order="C"))
    tmp_path.replace(path)


def write_staging_part(
    staging_dir: Path,
    chunk_id: int,
    part_id: int,
    records: List[Dict[str, object]],
    *,
    vector_widths: Dict[str, int],
) -> Dict[str, Path]:
    chunk_dir = staging_dir / "chunk-{0:06d}".format(chunk_id)
    chunk_dir.mkdir(parents=True, exist_ok=True)
    paths = {"rows": _staging_part_path(chunk_dir, chunk_id, part_id, ROWS_STAGING_SUFFIX)}
    _write_rows_table(
        paths["rows"],
        ROWS_PARQUET_SCHEMA,
        [_project_rows_record(record) for record in records],
    )
    for column_name in VECTOR_COLUMN_NAMES:
        paths[column_name] = _staging_part_path(
            chunk_dir, chunk_id, part_id, VECTOR_STAGING_SUFFIXES[column_name]
        )
        _write_vector_matrix(
            paths[column_name],
            column_name=column_name,
            width=vector_widths[column_name],
            records=records,
        )
    return paths


def _merge_completed_rows(
    *,
    chunk_dir: Path,
    completed_dir: Path,
    chunk_id: int,
    schema: pa.Schema,
) -> Path:
    filename = "part-{0:06d}.parquet".format(chunk_id)
    tmp_path = completed_dir / (filename + ".tmp")
    final_path = completed_dir / filename

    part_paths = sorted(chunk_dir.glob("*{0}".format(ROWS_STAGING_SUFFIX)))
    with pq.ParquetWriter(tmp_path, schema, compression="zstd") as writer:
        for part_path in part_paths:
            writer.write_table(pq.ParquetFile(part_path).read())

    tmp_path.replace(final_path)
    return final_path


def _merge_completed_vector_stream(
    *,
    chunk_dir: Path,
    completed_dir: Path,
    chunk_id: int,
    column_name: str,
) -> Path:
    filename = "part-{0:06d}{1}".format(chunk_id, VECTOR_COMPLETED_SUFFIXES[column_name])
    tmp_path = completed_dir / (filename + ".tmp")
    final_path = completed_dir / filename

    cctx = zstd.ZstdCompressor(level=19)
    part_paths = sorted(chunk_dir.glob("*{0}".format(VECTOR_STAGING_SUFFIXES[column_name])))
    with tmp_path.open("wb") as handle:
        with cctx.stream_writer(handle, closefd=False) as writer:
            for part_path in part_paths:
                with part_path.open("rb") as part_handle:
                    shutil.copyfileobj(part_handle, writer, length=1024 * 1024)

    tmp_path.replace(final_path)
    return final_path


def finalize_chunk(
    *,
    completed_dir: Path,
    staging_dir: Path,
    chunk_id: int,
    first_row: int,
    last_row: int,
    row_count: int,
    vector_widths: Dict[str, int],
) -> ChunkRecord:
    chunk_dir = staging_dir / "chunk-{0:06d}".format(chunk_id)
    if not chunk_dir.exists():
        raise FileNotFoundError(chunk_dir)

    completed_dir.mkdir(parents=True, exist_ok=True)
    rows_path = _merge_completed_rows(
        chunk_dir=chunk_dir,
        completed_dir=completed_dir,
        chunk_id=chunk_id,
        schema=ROWS_PARQUET_SCHEMA,
    )
    pathway_vectors_path = _merge_completed_vector_stream(
        chunk_dir=chunk_dir,
        completed_dir=completed_dir,
        chunk_id=chunk_id,
        column_name=PATHWAY_VECTOR_COLUMN,
    )
    superclass_vectors_path = _merge_completed_vector_stream(
        chunk_dir=chunk_dir,
        completed_dir=completed_dir,
        chunk_id=chunk_id,
        column_name=SUPERCLASS_VECTOR_COLUMN,
    )
    class_vectors_path = _merge_completed_vector_stream(
        chunk_dir=chunk_dir,
        completed_dir=completed_dir,
        chunk_id=chunk_id,
        column_name=CLASS_VECTOR_COLUMN,
    )
    shutil.rmtree(chunk_dir)
    return ChunkRecord(
        created_at=utc_now(),
        filename=rows_path.name,
        first_row=first_row,
        last_row=last_row,
        row_count=row_count,
        bytes=rows_path.stat().st_size,
        sha256=sha256_file(rows_path),
        pathway_vector_filename=pathway_vectors_path.name,
        pathway_vector_bytes=pathway_vectors_path.stat().st_size,
        pathway_vector_sha256=sha256_file(pathway_vectors_path),
        pathway_vector_shape=[row_count, vector_widths[PATHWAY_VECTOR_COLUMN]],
        superclass_vector_filename=superclass_vectors_path.name,
        superclass_vector_bytes=superclass_vectors_path.stat().st_size,
        superclass_vector_sha256=sha256_file(superclass_vectors_path),
        superclass_vector_shape=[row_count, vector_widths[SUPERCLASS_VECTOR_COLUMN]],
        class_vector_filename=class_vectors_path.name,
        class_vector_bytes=class_vectors_path.stat().st_size,
        class_vector_sha256=sha256_file(class_vectors_path),
        class_vector_shape=[row_count, vector_widths[CLASS_VECTOR_COLUMN]],
    )


def write_vocabulary(release_dir: Path, vocabulary: Dict[str, List[str]]) -> Path:
    release_dir.mkdir(parents=True, exist_ok=True)
    path = release_dir / VOCABULARY_FILENAME
    path.write_text(json.dumps(vocabulary, indent=2, sort_keys=True) + "\n")
    return path


def build_release_manifest(
    *,
    release_dir: Path,
    chunk_index: ChunkIndex,
    pubchem_total: int,
    successful_rows: int,
    parse_failed_rows: int,
    rdkit_failed_rows: int,
    other_failed_rows: int,
    source_info: Mapping[str, object],
    weights_info: Mapping[str, object],
    vocabulary_path: Path,
    checkpoint_rows: int,
    chunk_rows: int,
    max_rows: Optional[int],
    vector_widths: Dict[str, int],
) -> Path:
    cleanup_release_staging(release_dir)
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "created_at": utc_now(),
        "pubchem_total": pubchem_total,
        "checkpoint_rows": checkpoint_rows,
        "chunk_rows": chunk_rows,
        "max_rows": max_rows,
        "successful_rows": successful_rows,
        "parse_failed_rows": parse_failed_rows,
        "rdkit_failed_rows": rdkit_failed_rows,
        "other_failed_rows": other_failed_rows,
        "failed_rows": parse_failed_rows + rdkit_failed_rows + other_failed_rows,
        "processed_rows": (
            successful_rows
            + parse_failed_rows
            + rdkit_failed_rows
            + other_failed_rows
        ),
        "vocabulary_filename": vocabulary_path.name,
        "source": source_info,
        "weights": weights_info,
        "format": {
            "rows_type": "parquet",
            "alignment": (
                "Each chunk writes one main rows file and three vector sidecar files. "
                "All four files have identical row order and row count within the chunk."
            ),
            "rows_schema": {
                "cid": "int64",
                "smiles": "string",
                "pathway_ids": "list<uint16>",
                "superclass_ids": "list<uint16>",
                "class_ids": "list<uint16>",
                "isglycoside": "bool|null",
                "parse_failed": "bool",
                "rdkit_failed": "bool",
                "other_failure": "bool",
                "error_message": "string|null",
            },
            "vector_sidecars": {
                "encoding": "raw row-major little-endian float16 matrix",
                "compression": "zstd",
                "failed_row_encoding": (
                    "Rows flagged as failures in the main parquet are encoded as all-NaN "
                    "vectors in each sidecar."
                ),
                "columns": {
                    PATHWAY_VECTOR_COLUMN: {
                        "dtype": "float16",
                        "width": vector_widths[PATHWAY_VECTOR_COLUMN],
                        "filename_suffix": VECTOR_COMPLETED_SUFFIXES[PATHWAY_VECTOR_COLUMN],
                    },
                    SUPERCLASS_VECTOR_COLUMN: {
                        "dtype": "float16",
                        "width": vector_widths[SUPERCLASS_VECTOR_COLUMN],
                        "filename_suffix": VECTOR_COMPLETED_SUFFIXES[SUPERCLASS_VECTOR_COLUMN],
                    },
                    CLASS_VECTOR_COLUMN: {
                        "dtype": "float16",
                        "width": vector_widths[CLASS_VECTOR_COLUMN],
                        "filename_suffix": VECTOR_COMPLETED_SUFFIXES[CLASS_VECTOR_COLUMN],
                    },
                },
            },
        },
        "chunks": [
            {
                "filename": record.filename,
                "first_row": record.first_row,
                "last_row": record.last_row,
                "row_count": record.row_count,
                "bytes": record.bytes,
                "sha256": record.sha256,
                "pathway_vector_file": {
                    "filename": record.pathway_vector_filename,
                    "bytes": record.pathway_vector_bytes,
                    "sha256": record.pathway_vector_sha256,
                    "shape": record.pathway_vector_shape,
                    "dtype": "float16",
                    "compression": "zstd",
                },
                "superclass_vector_file": {
                    "filename": record.superclass_vector_filename,
                    "bytes": record.superclass_vector_bytes,
                    "sha256": record.superclass_vector_sha256,
                    "shape": record.superclass_vector_shape,
                    "dtype": "float16",
                    "compression": "zstd",
                },
                "class_vector_file": {
                    "filename": record.class_vector_filename,
                    "bytes": record.class_vector_bytes,
                    "sha256": record.class_vector_sha256,
                    "shape": record.class_vector_shape,
                    "dtype": "float16",
                    "compression": "zstd",
                },
            }
            for record in chunk_index.records
        ],
    }
    manifest_path = release_dir / MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest_path
