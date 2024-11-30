"""Script to retrieve the classification from the original NP Classifier."""

from typing import Dict, Optional, List
from argparse import ArgumentParser, Namespace
import os
from multiprocessing import Pool
from time import sleep
import requests
import compress_json
from matchms.importing import load_from_mgf
from cache_decorator import Cache
from tqdm import tqdm, trange
from fake_useragent import UserAgent
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
    attempts = 10

    while attempts < 10:
        try:
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
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ConnectionError,
        ):
            attempts += 1
            print(
                f"Timeout for {canonical_smiles}, attempt {attempts} out of 10, retrying..."
            )
            for _ in trange(60):
                sleep(1)

    return {}


def _get_canonical_smiles_classification(smiles: str) -> Optional[Dict]:
    RDLogger.DisableLog("rdApp.error")  # type: ignore
    mol: Optional[Mol] = MolFromSmiles(smiles)
    RDLogger.EnableLog("rdApp.error")  # type: ignore

    if mol is None:
        return {}

    if not simple_is_organic(mol):
        return {}

    canonical_smiles = MolToSmiles(mol)

    classification = get_canonical_smiles_classification(canonical_smiles)
    classification["smiles"] = canonical_smiles
    return classification


def is_numeric(value: str) -> bool:
    """Check if a string is numeric."""
    try:
        float(value)
        return True
    except ValueError:
        return False


KNOWN_COUNTS: Dict[str, int] = {"CID-SMILES.tsv": 119031918}


def clean(path: str) -> None:
    """Remove empty cache files."""
    if path.endswith(".metadata"):
        return
    if os.path.getsize(path) > 50:
        return
    data = compress_json.load(path)
    if not data:
        print(f"Removing empty cache file {path}")
        os.remove(path)
        os.remove(f"{path}.metadata")


def labeler() -> None:
    """Retrieve the classification from the original NP Classifier."""

    parser = ArgumentParser(
        description="Retrieve the classification from the original NP Classifier."
    )
    parser.add_argument("--input", type=str, help="Path to the input file.")
    parser.add_argument(
        "--output", type=str, help="Path to the output file.", required=False
    )
    parser.add_argument(
        "--workers",
        type=int,
        help="Number of workers to use.",
        default=1,
        required=False,
    )
    args: Namespace = parser.parse_args()

    with os.scandir("cache") as it:
        # We look into the cache directory to remove empty dictionaries
        # that were created by the cache decorator
        with Pool(args.workers) as pool:
            for _ in tqdm(
                pool.imap_unordered(clean, (entry.path for entry in it), 1000),
                desc="Removing empty cache files",
            ):
                pass
            pool.close()
            pool.join()

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
            if len(token) > 1 and not is_numeric(token)
        )
    elif args.input.endswith(".ssv"):
        data = (
            token.strip()
            for line in open(args.input, "r", encoding="utf-8")
            if line.strip() and not line.startswith("#")
            for token in line.split(" ")
            if len(token) > 1 and not is_numeric(token)
        )
    else:
        raise ValueError("Only MGF, SSV and TSV files are supported.")

    if args.input in KNOWN_COUNTS:
        total = KNOWN_COUNTS[args.input]
    else:
        total = None

    classifications: List[Dict] = []

    with Pool(args.workers) as pool:
        for classification in tqdm(
            pool.imap(_get_canonical_smiles_classification, data), total=total
        ):
            if classification:
                classifications.append(classification)

    compress_json.dump(classifications, output)


if __name__ == "__main__":
    labeler()
