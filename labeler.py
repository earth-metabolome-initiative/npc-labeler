"""Script to retrieve the classification from the original NP Classifier."""

from typing import Dict, Optional, List
from argparse import ArgumentParser, Namespace
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
    header = {"User-Agent": str(ua.chrome)}

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


def labeler() -> None:
    """Retrieve the classification from the original NP Classifier."""
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

    classifications: List[Dict] = []
    for smiles in tqdm(
        data, desc="Retrieving classifications", unit="smiles", dynamic_ncols=True
    ):
        classification = get_smiles_classification(smiles)

        if classification is not None:
            classifications.append(classification)

    compress_json.dump(data, args.output)


if __name__ == "__main__":
    labeler()
