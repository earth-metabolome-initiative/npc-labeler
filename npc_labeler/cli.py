from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

PUBCHEM_CID_SMILES_URL = (
    "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-SMILES.gz"
)
PARQUET_CHUNK_ROWS = 10_000_000


def _env_int(name: str, default: Optional[int]) -> Optional[int]:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value.strip() == "":
        return default
    return int(raw_value)


def _default_work_dir() -> Path:
    return Path(os.environ.get("NPC_WORK_DIR", "/work"))


def build_parser() -> argparse.ArgumentParser:
    work_dir = _default_work_dir()
    parser = argparse.ArgumentParser(
        prog="npc-labeler",
        description="Label PubChem locally with the recovered NPClassifier weights.",
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run the labeling pipeline.")
    run_parser.add_argument(
        "--work-dir",
        type=Path,
        default=work_dir,
        help="Workspace root mounted into the container.",
    )
    run_parser.add_argument(
        "--pubchem-input",
        type=Path,
        default=work_dir / "CID-SMILES.gz",
        help="Input CID-SMILES file (.gz or plain text).",
    )
    run_parser.add_argument(
        "--materialized-input",
        type=Path,
        default=work_dir / "CID-SMILES.tsv",
        help="Plain-text PubChem input used for resumable chunk-boundary processing.",
    )
    run_parser.add_argument(
        "--weights-dir",
        type=Path,
        default=work_dir / "weights",
        help="Directory holding the Zenodo NPClassifier HDF5 weights.",
    )
    run_parser.add_argument(
        "--completed-dir",
        type=Path,
        default=work_dir / "completed",
        help="Directory for finalized 10M-row Parquet chunks.",
    )
    run_parser.add_argument(
        "--state-dir",
        type=Path,
        default=work_dir / "state",
        help="Directory for resume state, chunk index, and transient staging files.",
    )
    run_parser.add_argument(
        "--release-dir",
        type=Path,
        default=work_dir / "releases",
        help="Directory for the manifest and vocabulary JSON files.",
    )
    run_parser.add_argument(
        "--checkpoint-rows",
        type=int,
        default=_env_int("NPC_CHECKPOINT_ROWS", 50000),
        help=(
            "Number of valid PubChem rows to process per in-memory mini-batch. "
            "Must divide 10,000,000 exactly."
        ),
    )
    run_parser.add_argument(
        "--max-rows",
        type=int,
        default=_env_int("NPC_MAX_ROWS", None),
        help="Optional cap for smoke runs.",
    )
    run_parser.add_argument(
        "--inference-batch-rows",
        type=int,
        default=_env_int("NPC_INFERENCE_BATCH_ROWS", 1024),
        help="Number of prepared rows to stack into each model forward pass.",
    )
    run_parser.add_argument(
        "--download-pubchem",
        action="store_true",
        help="Download the latest PubChem CID-SMILES.gz if --pubchem-input is missing.",
    )
    run_parser.add_argument(
        "--pubchem-url",
        default=os.environ.get("NPC_PUBCHEM_URL", PUBCHEM_CID_SMILES_URL),
        help="Source URL used when --download-pubchem is enabled.",
    )
    run_parser.add_argument(
        "--chunk-rows",
        type=int,
        default=PARQUET_CHUNK_ROWS,
        help="Final Parquet chunk size. Defaults to 10,000,000 rows.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    command = args.command or "run"
    if command != "run":
        parser.error("unsupported command {0!r}".format(command))

    from npc_labeler.pipeline import RunConfig, run_pipeline

    config = RunConfig(
        work_dir=args.work_dir,
        pubchem_input_path=args.pubchem_input,
        materialized_input_path=args.materialized_input,
        weights_dir=args.weights_dir,
        completed_dir=args.completed_dir,
        state_dir=args.state_dir,
        release_dir=args.release_dir,
        checkpoint_rows=args.checkpoint_rows,
        inference_batch_rows=args.inference_batch_rows,
        max_rows=args.max_rows,
        download_pubchem=args.download_pubchem,
        pubchem_url=args.pubchem_url,
        chunk_rows=args.chunk_rows,
    )
    run_pipeline(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
