import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import zstandard as zstd

from npc_labeler.output import (
    ChunkIndex,
    finalize_chunk,
    write_staging_part,
)


class OutputLayoutTest(unittest.TestCase):
    def test_finalize_chunk_writes_rows_and_vector_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            completed_dir = root / "completed"
            staging_dir = root / "staging"
            chunk_index = ChunkIndex.open(root / "state" / "chunks.jsonl")

            write_staging_part(
                staging_dir=staging_dir,
                chunk_id=1,
                part_id=1,
                vector_widths={
                    "pathway_prediction_vector": 2,
                    "superclass_prediction_vector": 2,
                    "class_prediction_vector": 2,
                },
                records=[
                    {
                        "cid": 1,
                        "smiles": "CCO",
                        "pathway_ids": [1],
                        "superclass_ids": [2],
                        "class_ids": [3],
                        "isglycoside": False,
                        "pathway_prediction_vector": [0.5, 0.25],
                        "superclass_prediction_vector": [0.125, 0.75],
                        "class_prediction_vector": [0.0625, 0.875],
                        "parse_failed": False,
                        "rdkit_failed": False,
                        "other_failure": False,
                        "error_message": None,
                    },
                    {
                        "cid": 2,
                        "smiles": "bad",
                        "pathway_ids": None,
                        "superclass_ids": None,
                        "class_ids": None,
                        "isglycoside": None,
                        "pathway_prediction_vector": None,
                        "superclass_prediction_vector": None,
                        "class_prediction_vector": None,
                        "parse_failed": True,
                        "rdkit_failed": False,
                        "other_failure": False,
                        "error_message": "parse failed",
                    },
                ],
            )

            chunk = finalize_chunk(
                completed_dir=completed_dir,
                staging_dir=staging_dir,
                chunk_id=1,
                first_row=0,
                last_row=1,
                row_count=2,
                vector_widths={
                    "pathway_prediction_vector": 2,
                    "superclass_prediction_vector": 2,
                    "class_prediction_vector": 2,
                },
            )
            chunk_index.append(chunk)

            rows = pq.ParquetFile(completed_dir / chunk.filename).read()
            pathway_vectors = self._read_vector_matrix(
                completed_dir / chunk.pathway_vector_filename,
                rows=2,
                width=2,
            )
            superclass_vectors = self._read_vector_matrix(
                completed_dir / chunk.superclass_vector_filename,
                rows=2,
                width=2,
            )
            class_vectors = self._read_vector_matrix(
                completed_dir / chunk.class_vector_filename,
                rows=2,
                width=2,
            )

            self.assertEqual(rows.num_rows, 2)
            self.assertEqual(rows.schema.names, [
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
            ])

            self.assertEqual(pathway_vectors.shape, (2, 2))
            self.assertEqual(superclass_vectors.shape, (2, 2))
            self.assertEqual(class_vectors.shape, (2, 2))
            self.assertTrue(np.allclose(pathway_vectors[0], np.array([0.5, 0.25], dtype=np.float16)))
            self.assertTrue(np.allclose(superclass_vectors[0], np.array([0.125, 0.75], dtype=np.float16)))
            self.assertTrue(np.allclose(class_vectors[0], np.array([0.0625, 0.875], dtype=np.float16)))
            self.assertTrue(np.isnan(pathway_vectors[1]).all())
            self.assertTrue(np.isnan(superclass_vectors[1]).all())
            self.assertTrue(np.isnan(class_vectors[1]).all())

    @staticmethod
    def _read_vector_matrix(path: Path, *, rows: int, width: int) -> np.ndarray:
        dctx = zstd.ZstdDecompressor()
        with path.open("rb") as handle:
            with dctx.stream_reader(handle) as reader:
                payload = reader.read()
        matrix = np.frombuffer(payload, dtype=np.dtype("<f2"))
        return matrix.reshape(rows, width)


if __name__ == "__main__":
    unittest.main()
