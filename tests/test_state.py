import tempfile
import unittest
from pathlib import Path

from npc_labeler.state import RunState


class RunStateTest(unittest.TestCase):
    def test_round_trip_and_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_path = root / "run-state.json"
            input_path = root / "CID-SMILES.gz"
            materialized_path = root / "CID-SMILES.tsv"
            input_path.write_text("placeholder\n")
            materialized_path.write_text("1\tCCO\n")

            state = RunState.create(
                input_path=input_path,
                materialized_input_path=materialized_path,
                pubchem_total=1,
                checkpoint_rows=100,
                chunk_rows=1000,
                max_rows=None,
            )
            state.update(
                next_row=1,
                next_offset=12,
                successful_rows=1,
                parse_failed_rows=0,
                rdkit_failed_rows=0,
                other_failed_rows=0,
                next_chunk_id=2,
            )
            state.save(state_path)

            loaded = RunState.load(state_path)
            self.assertEqual(loaded.next_row, 1)
            self.assertEqual(loaded.next_offset, 12)
            self.assertEqual(loaded.successful_rows, 1)
            self.assertEqual(loaded.next_chunk_id, 2)
            loaded.validate_against(
                input_path=input_path,
                materialized_input_path=materialized_path,
                pubchem_total=1,
                checkpoint_rows=100,
                chunk_rows=1000,
                max_rows=None,
            )

    def test_validation_rejects_changed_checkpoint_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            input_path = root / "CID-SMILES.gz"
            materialized_path = root / "CID-SMILES.tsv"
            input_path.write_text("placeholder\n")
            materialized_path.write_text("1\tCCO\n")

            state = RunState.create(
                input_path=input_path,
                materialized_input_path=materialized_path,
                pubchem_total=1,
                checkpoint_rows=100,
                chunk_rows=1000,
                max_rows=None,
            )
            with self.assertRaises(ValueError):
                state.validate_against(
                    input_path=input_path,
                    materialized_input_path=materialized_path,
                    pubchem_total=1,
                    checkpoint_rows=50,
                    chunk_rows=1000,
                    max_rows=None,
                )


if __name__ == "__main__":
    unittest.main()
