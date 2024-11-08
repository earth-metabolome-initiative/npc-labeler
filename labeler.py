"""Script to retrieve the classification from the original NP Classifier."""

from typing import Dict, Optional, List, Set
from argparse import ArgumentParser, Namespace
import os
from time import time
from glob import glob
import requests
import compress_json
from matchms.importing import load_from_mgf
from cache_decorator import Cache
from fake_useragent import UserAgent
from humanize import naturaldelta
from rich.console import Console
from rich.table import Table
from rdkit.Chem.rdchem import Mol
from rdkit.Chem import (  # pylint: disable=no-name-in-module
    MolFromSmarts,
    MolFromSmiles,
    MolToSmiles,
)
from rdkit import RDLogger  # pylint: disable=no-name-in-module


simpleOrganicAtomQuery = MolFromSmarts("[!$([#1,#5,#6,#7,#8,#9,#15,#16,#17,#35,#53])]")
simpleOrganicBondQuery = MolFromSmarts("[#6]-,=,#,:[#6]")
hasCHQuery = MolFromSmarts("[C!H0]")


def simple_is_organic(mol: Mol) -> bool:
    """Check if a SMILES seems organic."""
    return (
        (not mol.HasSubstructMatch(simpleOrganicAtomQuery))
        and mol.HasSubstructMatch(hasCHQuery)
        and mol.HasSubstructMatch(simpleOrganicBondQuery)
    )


@Cache(use_source_code=False, cache_path="{cache_dir}/{_hash}.json.gz")
def get_canonical_smiles_classification(canonical_smiles: str) -> Dict:
    """Get the classifications for a given SMILES."""

    ua = UserAgent()
    header = {
        "User-Agent": str(ua.chrome),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    response = requests.get(
        "https://npclassifier.gnps2.org/classify",
        params={"smiles": canonical_smiles},
        headers=header,
        timeout=10,
    )

    if response.status_code != 200:
        print(
            f"Failed to retrieve classifications for {canonical_smiles}, "
            f" got status code {response.status_code}"
        )
        print(response.text)

    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        print(
            f"Failed to convert response to JSON for {canonical_smiles}, "
            f" got status code {response.status_code}, raw response: {response.text}"
        )

    return {}


KNOWN_COUNTS: Dict[str, int] = {"CID-SMILES.tsv": 119031918}


def labeler() -> None:
    """Retrieve the classification from the original NP Classifier."""

    # We look into the cache directory to remove empty dictionaries
    # that were created by the cache decorator
    for path in glob("cache/*.json.gz"):
        data = compress_json.load(path)
        if not data:
            print(f"Removing empty cache file {path}")
            os.remove(path)
            os.remove(f"{path}.metadata")

    parser = ArgumentParser(
        description="Retrieve the classification from the original NP Classifier."
    )
    parser.add_argument("--input", type=str, help="Path to the input file.")
    parser.add_argument(
        "--output", type=str, help="Path to the output file.", required=False
    )
    args: Namespace = parser.parse_args()

    if args.output is None:
        assert args.input is not None
        assert args.input.endswith(".mgf")
        output = args.input.replace(".mgf", ".json.gz")
    else:
        output = args.output

    if not any(
        output.endswith(extension) for extension in (".json.gz", ".json", ".json.xz")
    ):
        raise ValueError("Only JSON output files are supported.")

    if args.input.endswith(".mgf"):
        data = (
            spectrum.metadata["smiles"]
            for spectrum in load_from_mgf(args.input)
            if "smiles" in spectrum.metadata
        )
    elif args.input.endswith(".tsv"):
        data = (
            token.strip()
            for line in open(args.input, "r", encoding="utf-8")
            if line.strip() and not line.startswith("#")
            for token in line.split("\t")
            if len(token) > 1
        )
    elif args.input.endswith(".ssv"):
        data = (
            token.strip()
            for line in open(args.input, "r", encoding="utf-8")
            if line.strip() and not line.startswith("#")
            for token in line.split(" ")
            if len(token) > 1
        )
    else:
        raise ValueError("Only MGF, SSV and TSV files are supported.")

    if args.input in KNOWN_COUNTS:
        total = KNOWN_COUNTS[args.input]
    else:
        total = None

    console: Console = Console()
    last_printed = time()
    started = time()

    classified_smiles: Set[str] = set()
    failed_classifications: Set[str] = set()
    inorganics: Set[str] = set()
    classifications: List[Dict] = []
    for smiles in data:
        if time() - last_printed > 0.5:
            table: Table = Table(title="NP Classifier")
            table.add_column("Classified", justify="right")
            table.add_column("Failed", justify="right")
            table.add_column("Inorganics", justify="right")
            table.add_column("Processed", justify="right")
            table.add_column("Total", justify="right")
            table.add_column("Elapsed time", justify="right")
            table.add_column("Remaining time", justify="right")
            all_smiles_count = (
                len(classified_smiles) + len(failed_classifications) + len(inorganics)
            )
            table.add_row(
                str(len(classified_smiles)),
                str(len(failed_classifications)),
                str(len(inorganics)),
                str(all_smiles_count),
                str(total) if total is not None else "Unknown",
                naturaldelta(time() - started),
                (
                    naturaldelta(
                        (time() - started)
                        / all_smiles_count
                        * (total - all_smiles_count)
                    )
                    if total is not None
                    else "Unknown"
                ),
            )
            console.clear()
            console.print(table)

            last_printed = time()

        if smiles in classified_smiles:
            continue

        RDLogger.DisableLog("rdApp.error")  # type: ignore
        mol: Optional[Mol] = MolFromSmiles(smiles)
        RDLogger.EnableLog("rdApp.error")  # type: ignore

        if mol is None:
            failed_classifications.add(smiles)
            continue

        if not simple_is_organic(mol):
            inorganics.add(smiles)
            continue

        classification = get_canonical_smiles_classification(MolToSmiles(mol))

        if classification is not None and classification:
            classified_smiles.add(smiles)
            classification["smiles"] = smiles
            classifications.append(classification)

    compress_json.dump(classifications, output)

    print(
        f"Retrieved classifications for {len(classified_smiles)} unique SMILES, "
        f"failed to retrieve classifications for {len(failed_classifications)} unique SMILES."
    )


if __name__ == "__main__":
    labeler()
