from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

STATE_VERSION = 2


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RunState:
    version: int
    created_at: str
    updated_at: str
    input_path: str
    materialized_input_path: str
    pubchem_total: int
    checkpoint_rows: int
    chunk_rows: int
    max_rows: Optional[int]
    next_row: int
    next_offset: int
    successful_rows: int
    parse_failed_rows: int
    rdkit_failed_rows: int
    other_failed_rows: int
    next_chunk_id: int

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

    @classmethod
    def create(
        cls,
        input_path: Path,
        materialized_input_path: Path,
        pubchem_total: int,
        checkpoint_rows: int,
        chunk_rows: int,
        max_rows: Optional[int],
    ) -> "RunState":
        now = utc_now()
        return cls(
            version=STATE_VERSION,
            created_at=now,
            updated_at=now,
            input_path=str(input_path),
            materialized_input_path=str(materialized_input_path),
            pubchem_total=pubchem_total,
            checkpoint_rows=checkpoint_rows,
            chunk_rows=chunk_rows,
            max_rows=max_rows,
            next_row=0,
            next_offset=0,
            successful_rows=0,
            parse_failed_rows=0,
            rdkit_failed_rows=0,
            other_failed_rows=0,
            next_chunk_id=1,
        )

    def update(
        self,
        *,
        next_row: int,
        next_offset: int,
        successful_rows: int,
        parse_failed_rows: int,
        rdkit_failed_rows: int,
        other_failed_rows: int,
        next_chunk_id: int,
    ) -> None:
        self.next_row = next_row
        self.next_offset = next_offset
        self.successful_rows = successful_rows
        self.parse_failed_rows = parse_failed_rows
        self.rdkit_failed_rows = rdkit_failed_rows
        self.other_failed_rows = other_failed_rows
        self.next_chunk_id = next_chunk_id
        self.updated_at = utc_now()

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(self)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        tmp_path.replace(path)

    @classmethod
    def load(cls, path: Path) -> "RunState":
        payload = json.loads(path.read_text())
        return cls(**payload)

    def validate_against(
        self,
        *,
        input_path: Path,
        materialized_input_path: Path,
        pubchem_total: int,
        checkpoint_rows: int,
        chunk_rows: int,
        max_rows: Optional[int],
    ) -> None:
        mismatches = []
        if self.version != STATE_VERSION:
            mismatches.append(
                "state schema version changed; remove state/run-state.json to restart cleanly"
            )
        if self.input_path != str(input_path):
            mismatches.append("input path changed")
        if self.materialized_input_path != str(materialized_input_path):
            mismatches.append("materialized input path changed")
        if self.pubchem_total != pubchem_total:
            mismatches.append("PubChem total changed")
        if self.checkpoint_rows != checkpoint_rows:
            mismatches.append("checkpoint_rows changed")
        if self.chunk_rows != chunk_rows:
            mismatches.append("chunk_rows changed")
        if self.max_rows != max_rows:
            mismatches.append("max_rows changed")
        if mismatches:
            raise ValueError("; ".join(mismatches))
