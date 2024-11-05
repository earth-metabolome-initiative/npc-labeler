"""Script to retrieve the classification from the original NP Classifier."""

from typing import Dict, Optional, List, Set
from argparse import ArgumentParser, Namespace
import os
from glob import glob
import requests
from tqdm.auto import tqdm
import compress_json
from matchms.importing import load_from_mgf
from cache_decorator import Cache
from fake_useragent import UserAgent
from rdkit.Chem.rdchem import Mol
from rdkit.Chem import MolFromSmiles, MolToSmiles  # pylint: disable=no-name-in-module
from rdkit import RDLogger  # pylint: disable=no-name-in-module


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


def get_smiles_classification(smiles: str) -> Optional[Dict]:
    """Get the classifications for a given SMILES."""

    RDLogger.DisableLog("rdApp.error")  # type: ignore
    mol: Optional[Mol] = MolFromSmiles(smiles)
    RDLogger.EnableLog("rdApp.error")  # type: ignore

    if mol is None:
        return None

    canonical_smiles: str = MolToSmiles(mol)

    return get_canonical_smiles_classification(canonical_smiles)


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
    parser.add_argument("--output", type=str, help="Path to the output file.")
    args: Namespace = parser.parse_args()

    if not any(
        args.output.endswith(extension)
        for extension in (".json.gz", ".json", ".json.xz")
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

    classified_smiles: Set[str] = set()
    failed_classifications: Set[str] = set()
    classifications: List[Dict] = []
    for smiles in tqdm(
        data,
        desc="Retrieving classifications",
        unit="smiles",
        dynamic_ncols=True,
        leave=False,
        total=total,
    ):
        if smiles in classified_smiles:
            continue

        classification = get_smiles_classification(smiles)

        if classification is not None and classification:
            classified_smiles.add(smiles)
            classification["smiles"] = smiles
            classifications.append(classification)
        else:
            failed_classifications.add(smiles)

    compress_json.dump(classifications, args.output)

    print(
        f"Retrieved classifications for {len(classified_smiles)} unique SMILES, "
        f"failed to retrieve classifications for {len(failed_classifications)} unique SMILES."
    )


if __name__ == "__main__":
    labeler()
