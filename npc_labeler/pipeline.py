from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from npc_labeler.downloads import (
    PUBCHEM_CID_SMILES_URL,
    SourceInfo,
    WeightsInfo,
    ensure_model_weights,
    ensure_pubchem_input,
)
from npc_labeler.model import (
    ClassificationRuntimeError,
    InvalidSmilesError,
    NPClassifier,
    RdkitPipelineError,
)
from npc_labeler.output import (
    PARQUET_CHUNK_ROWS,
    ChunkIndex,
    build_release_manifest,
    cleanup_completed_dir,
    cleanup_staging_dir,
    finalize_chunk,
    write_staging_part,
    write_vocabulary,
)
from npc_labeler.state import RunState


def _parse_pubchem_line(raw_line: str) -> Optional[Tuple[int, str]]:
    parts = raw_line.rstrip("\n").split("\t", 1)
    if len(parts) != 2:
        return None
    cid_text, smiles = parts
    try:
        cid = int(cid_text)
    except ValueError:
        return None
    return cid, smiles.strip()


@dataclass
class RunConfig:
    work_dir: Path
    pubchem_input_path: Path
    materialized_input_path: Path
    weights_dir: Path
    completed_dir: Path
    state_dir: Path
    release_dir: Path
    checkpoint_rows: int
    inference_batch_rows: int
    max_rows: Optional[int]
    download_pubchem: bool
    pubchem_url: str = PUBCHEM_CID_SMILES_URL
    chunk_rows: int = PARQUET_CHUNK_ROWS


@dataclass
class RunCounts:
    successful_rows: int
    parse_failed_rows: int
    rdkit_failed_rows: int
    other_failed_rows: int

    @property
    def processed_rows(self) -> int:
        return (
            self.successful_rows
            + self.parse_failed_rows
            + self.rdkit_failed_rows
            + self.other_failed_rows
        )

    @property
    def failed_rows(self) -> int:
        return self.parse_failed_rows + self.rdkit_failed_rows + self.other_failed_rows


def _read_batch(
    handle,
    *,
    current_row: int,
    checkpoint_rows: int,
    max_rows: Optional[int],
) -> Tuple[List[Tuple[int, int, str]], int]:
    tasks: List[Tuple[int, int, str]] = []
    batch_end_offset = handle.tell()
    while len(tasks) < checkpoint_rows:
        if max_rows is not None and current_row + len(tasks) >= max_rows:
            break
        raw_line = handle.readline()
        if not raw_line:
            break
        batch_end_offset = handle.tell()
        parsed = _parse_pubchem_line(raw_line.decode("utf-8", errors="replace"))
        if parsed is None:
            continue
        cid, smiles = parsed
        tasks.append((current_row + len(tasks), cid, smiles))
    return tasks, batch_end_offset


def _progress_line(
    *,
    counts: RunCounts,
    target_rows: int,
    started_at: float,
    checkpoint_rows: int,
    next_chunk_id: int,
    rows_in_chunk: int,
    chunk_rows: int,
) -> str:
    elapsed = max(time.time() - started_at, 1e-6)
    rate = counts.processed_rows / elapsed
    remaining = max(target_rows - counts.processed_rows, 0)
    eta_seconds = remaining / rate if rate > 0 else 0.0
    eta_minutes = eta_seconds / 60.0
    return (
        "handled {processed}/{target} rows | success={success} parse_failed={parse_failed} "
        "rdkit_failed={rdkit_failed} other_failed={other_failed} | chunk={chunk_id} "
        "rows_in_chunk={rows_in_chunk}/{chunk_rows} | checkpoint_rows={checkpoint_rows} "
        "| rate={rate:.1f}/s | eta={eta:.1f} min"
    ).format(
        processed=counts.processed_rows,
        target=target_rows,
        success=counts.successful_rows,
        parse_failed=counts.parse_failed_rows,
        rdkit_failed=counts.rdkit_failed_rows,
        other_failed=counts.other_failed_rows,
        chunk_id=next_chunk_id,
        rows_in_chunk=rows_in_chunk,
        chunk_rows=chunk_rows,
        checkpoint_rows=checkpoint_rows,
        rate=rate,
        eta=eta_minutes,
    )


def _failure_record(
    cid: int,
    smiles: str,
    *,
    parse_failed: bool,
    rdkit_failed: bool,
    other_failure: bool,
    error_message: str,
) -> Dict[str, object]:
    return {
        "cid": cid,
        "smiles": smiles,
        "pathway_ids": None,
        "superclass_ids": None,
        "class_ids": None,
        "isglycoside": None,
        "pathway_prediction_vector": None,
        "superclass_prediction_vector": None,
        "class_prediction_vector": None,
        "parse_failed": parse_failed,
        "rdkit_failed": rdkit_failed,
        "other_failure": other_failure,
        "error_message": error_message,
    }


def run_pipeline(config: RunConfig) -> Dict[str, object]:
    if config.chunk_rows % config.checkpoint_rows != 0:
        raise ValueError(
            "checkpoint_rows must divide chunk_rows exactly; got {0} and {1}".format(
                config.checkpoint_rows, config.chunk_rows
            )
        )
    if config.inference_batch_rows <= 0:
        raise ValueError(
            "inference_batch_rows must be positive; got {0}".format(
                config.inference_batch_rows
            )
        )

    for path in (
        config.work_dir,
        config.completed_dir,
        config.state_dir,
        config.release_dir,
        config.weights_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)

    staging_dir = config.state_dir / "staging"
    cleanup_staging_dir(staging_dir)

    weights_info: WeightsInfo = ensure_model_weights(config.weights_dir)
    source_info: SourceInfo = ensure_pubchem_input(
        pubchem_input_path=config.pubchem_input_path,
        materialized_input_path=config.materialized_input_path,
        allow_download=config.download_pubchem,
        pubchem_url=config.pubchem_url,
    )

    classifier = NPClassifier.from_weights_dir(config.weights_dir)
    weights_info["rdkit_version"] = classifier.rdkit_version

    state_path = config.state_dir / "run-state.json"
    chunk_index = ChunkIndex.open(config.state_dir / "chunks.jsonl")
    cleanup_completed_dir(config.completed_dir, chunk_index)

    if state_path.exists():
        state = RunState.load(state_path)
        state.validate_against(
            input_path=config.pubchem_input_path,
            materialized_input_path=config.materialized_input_path,
            pubchem_total=source_info["pubchem_total"],
            checkpoint_rows=config.checkpoint_rows,
            chunk_rows=config.chunk_rows,
            max_rows=config.max_rows,
        )
    else:
        state = RunState.create(
            input_path=config.pubchem_input_path,
            materialized_input_path=config.materialized_input_path,
            pubchem_total=source_info["pubchem_total"],
            checkpoint_rows=config.checkpoint_rows,
            chunk_rows=config.chunk_rows,
            max_rows=config.max_rows,
        )

    state.next_chunk_id = chunk_index.next_chunk_id()
    counts = RunCounts(
        successful_rows=state.successful_rows,
        parse_failed_rows=state.parse_failed_rows,
        rdkit_failed_rows=state.rdkit_failed_rows,
        other_failed_rows=state.other_failed_rows,
    )

    target_rows = (
        min(source_info["pubchem_total"], config.max_rows)
        if config.max_rows is not None
        else source_info["pubchem_total"]
    )
    vocabulary_path = write_vocabulary(config.release_dir, classifier.vocabulary_payload())
    vector_widths = classifier.vector_widths()

    if state.next_row >= target_rows:
        print("state already covers {0} rows, refreshing manifest only".format(target_rows))
        manifest_path = build_release_manifest(
            release_dir=config.release_dir,
            chunk_index=chunk_index,
            pubchem_total=source_info["pubchem_total"],
            successful_rows=counts.successful_rows,
            parse_failed_rows=counts.parse_failed_rows,
            rdkit_failed_rows=counts.rdkit_failed_rows,
            other_failed_rows=counts.other_failed_rows,
            source_info=source_info,
            weights_info=weights_info,
            vocabulary_path=vocabulary_path,
            checkpoint_rows=config.checkpoint_rows,
            chunk_rows=config.chunk_rows,
            max_rows=config.max_rows,
            vector_widths=vector_widths,
        )
        return {"manifest_path": manifest_path, "vocabulary_path": vocabulary_path}

    started_at = time.time()
    current_row = state.next_row
    current_offset = state.next_offset
    current_chunk_id = state.next_chunk_id
    rows_in_chunk = 0
    chunk_first_row: Optional[int] = None
    next_part_id = 1

    print(
        "starting at finalized chunk boundary row {0} using {1}".format(
            current_row, config.materialized_input_path
        )
    )
    with config.materialized_input_path.open("rb") as handle:
        handle.seek(state.next_offset)
        while current_row < target_rows:
            tasks, batch_end_offset = _read_batch(
                handle,
                current_row=current_row,
                checkpoint_rows=config.checkpoint_rows,
                max_rows=config.max_rows,
            )
            if not tasks:
                break

            records: List[Optional[Dict[str, object]]] = [None] * len(tasks)
            prepared_rows = []
            for task_index, (_row_index, cid, smiles) in enumerate(tasks):
                try:
                    prepared_rows.append(
                        (task_index, classifier.prepare_record(cid=cid, smiles=smiles))
                    )
                except InvalidSmilesError as error:
                    records[task_index] = _failure_record(
                        cid,
                        smiles,
                        parse_failed=True,
                        rdkit_failed=False,
                        other_failure=False,
                        error_message=str(error),
                    )
                    counts.parse_failed_rows += 1
                except RdkitPipelineError as error:
                    records[task_index] = _failure_record(
                        cid,
                        smiles,
                        parse_failed=False,
                        rdkit_failed=True,
                        other_failure=False,
                        error_message=str(error),
                    )
                    counts.rdkit_failed_rows += 1
                except ClassificationRuntimeError as error:
                    records[task_index] = _failure_record(
                        cid,
                        smiles,
                        parse_failed=False,
                        rdkit_failed=False,
                        other_failure=True,
                        error_message=str(error),
                    )
                    counts.other_failed_rows += 1
                except Exception as error:  # noqa: BLE001
                    records[task_index] = _failure_record(
                        cid,
                        smiles,
                        parse_failed=False,
                        rdkit_failed=False,
                        other_failure=True,
                        error_message=str(error),
                    )
                    counts.other_failed_rows += 1

            for batch_start in range(0, len(prepared_rows), config.inference_batch_rows):
                batch_items = prepared_rows[
                    batch_start : batch_start + config.inference_batch_rows
                ]
                batch_prepared = [prepared_record for _, prepared_record in batch_items]
                try:
                    batch_records = classifier.classify_prepared_batch(batch_prepared)
                    if len(batch_records) != len(batch_items):
                        raise RuntimeError(
                            "classifier returned {0} records for {1} prepared rows".format(
                                len(batch_records), len(batch_items)
                            )
                        )
                    for (task_index, _prepared_record), record in zip(
                        batch_items, batch_records
                    ):
                        records[task_index] = record
                        counts.successful_rows += 1
                except Exception:
                    for task_index, prepared_record in batch_items:
                        try:
                            records[task_index] = classifier.classify_prepared_record(
                                prepared_record
                            )
                            counts.successful_rows += 1
                        except ClassificationRuntimeError as error:
                            records[task_index] = _failure_record(
                                prepared_record.cid,
                                prepared_record.smiles,
                                parse_failed=False,
                                rdkit_failed=False,
                                other_failure=True,
                                error_message=str(error),
                            )
                            counts.other_failed_rows += 1
                        except Exception as error:  # noqa: BLE001
                            records[task_index] = _failure_record(
                                prepared_record.cid,
                                prepared_record.smiles,
                                parse_failed=False,
                                rdkit_failed=False,
                                other_failure=True,
                                error_message=str(error),
                            )
                            counts.other_failed_rows += 1

            finalized_records: List[Dict[str, object]] = []
            for task_index, maybe_record in enumerate(records):
                if maybe_record is None:
                    _row_index, cid, smiles = tasks[task_index]
                    resolved_record = _failure_record(
                        cid,
                        smiles,
                        parse_failed=False,
                        rdkit_failed=False,
                        other_failure=True,
                        error_message="missing classification result",
                    )
                    counts.other_failed_rows += 1
                else:
                    resolved_record = maybe_record
                finalized_records.append(resolved_record)

            if chunk_first_row is None:
                chunk_first_row = tasks[0][0]
            write_staging_part(
                staging_dir=staging_dir,
                chunk_id=current_chunk_id,
                part_id=next_part_id,
                records=finalized_records,
                vector_widths=vector_widths,
            )
            next_part_id += 1
            rows_in_chunk += len(finalized_records)
            current_row += len(finalized_records)
            current_offset = batch_end_offset

            if rows_in_chunk == config.chunk_rows:
                chunk_record = finalize_chunk(
                    completed_dir=config.completed_dir,
                    staging_dir=staging_dir,
                    chunk_id=current_chunk_id,
                    first_row=chunk_first_row if chunk_first_row is not None else 0,
                    last_row=current_row - 1,
                    row_count=rows_in_chunk,
                    vector_widths=vector_widths,
                )
                chunk_index.append(chunk_record)
                current_chunk_id += 1
                rows_in_chunk = 0
                chunk_first_row = None
                next_part_id = 1
                state.update(
                    next_row=current_row,
                    next_offset=current_offset,
                    successful_rows=counts.successful_rows,
                    parse_failed_rows=counts.parse_failed_rows,
                    rdkit_failed_rows=counts.rdkit_failed_rows,
                    other_failed_rows=counts.other_failed_rows,
                    next_chunk_id=current_chunk_id,
                )
                state.save(state_path)
                print(
                    "finalized {0} at row {1}".format(
                        chunk_record.filename, chunk_record.last_row
                    )
                )

            print(
                _progress_line(
                    counts=counts,
                    target_rows=target_rows,
                    started_at=started_at,
                    checkpoint_rows=config.checkpoint_rows,
                    next_chunk_id=current_chunk_id,
                    rows_in_chunk=rows_in_chunk,
                    chunk_rows=config.chunk_rows,
                )
            )

    if rows_in_chunk > 0:
        chunk_record = finalize_chunk(
            completed_dir=config.completed_dir,
            staging_dir=staging_dir,
            chunk_id=current_chunk_id,
            first_row=chunk_first_row if chunk_first_row is not None else 0,
            last_row=current_row - 1,
            row_count=rows_in_chunk,
            vector_widths=vector_widths,
        )
        chunk_index.append(chunk_record)
        current_chunk_id += 1
        state.update(
            next_row=current_row,
            next_offset=current_offset,
            successful_rows=counts.successful_rows,
            parse_failed_rows=counts.parse_failed_rows,
            rdkit_failed_rows=counts.rdkit_failed_rows,
            other_failed_rows=counts.other_failed_rows,
            next_chunk_id=current_chunk_id,
        )
        state.save(state_path)
        print("finalized trailing chunk {0}".format(chunk_record.filename))

    manifest_path = build_release_manifest(
        release_dir=config.release_dir,
        chunk_index=chunk_index,
        pubchem_total=source_info["pubchem_total"],
        successful_rows=counts.successful_rows,
        parse_failed_rows=counts.parse_failed_rows,
        rdkit_failed_rows=counts.rdkit_failed_rows,
        other_failed_rows=counts.other_failed_rows,
        source_info=source_info,
        weights_info=weights_info,
        vocabulary_path=vocabulary_path,
        checkpoint_rows=config.checkpoint_rows,
        chunk_rows=config.chunk_rows,
        max_rows=config.max_rows,
        vector_widths=vector_widths,
    )
    print("release metadata ready at {0}".format(manifest_path))
    return {
        "manifest_path": manifest_path,
        "vocabulary_path": vocabulary_path,
        "chunk_count": len(chunk_index.records),
    }
