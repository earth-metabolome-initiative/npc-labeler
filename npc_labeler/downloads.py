from __future__ import annotations

import gzip
import hashlib
import json
import zipfile
from pathlib import Path
from typing import List, Optional, Tuple, TypedDict, cast

import requests

PUBCHEM_CID_SMILES_URL = (
    "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-SMILES.gz"
)
MODEL_ZIP_URL = "https://zenodo.org/records/5068687/files/model.zip?download=1"
MODEL_RECORD_DOI = "10.5281/zenodo.5068687"

EXPECTED_MD5 = {
    "model.zip": "7f5cf472aa970afd525267c595baf733",
    "NP_classifier_pathway_V1.hdf5": "8a566a447ebf65c2715bb3922bd8c199",
    "NP_classifier_superclass_V1.hdf5": "6abe833cc70969f2c46de2979255ba6f",
    "NP_classifier_class_V1.hdf5": "3359cc7342139ee82f1dd8bdc1499b06",
}

EXPECTED_SHA256 = {
    "model.zip": "6f0b6ae524f7797d70bbfa210600c3c0f8cc0508789800280e89510215715db4",
    "NP_classifier_pathway_V1.hdf5": "070a9c684ac303a0c86998b943e094b394a9756efd631819ff39af8641036509",
    "NP_classifier_superclass_V1.hdf5": "1287e7cfc4cf72c19e917fa7cd4da9902728e56502bedf03654ea66cef62ead1",
    "NP_classifier_class_V1.hdf5": "e602e2803cf6836ac4504ab23bd7dad3dc6fcdd3599220389f4e5a700e49385f",
}

MODEL_FILES = (
    "NP_classifier_pathway_V1.hdf5",
    "NP_classifier_superclass_V1.hdf5",
    "NP_classifier_class_V1.hdf5",
)


class WeightFileInfo(TypedDict):
    filename: str
    path: str
    md5: str
    sha256: str
    bytes: int


class WeightsInfo(TypedDict):
    doi: str
    download_url: str
    weights_dir: str
    files: List[WeightFileInfo]
    rdkit_version: str


class SourceInfo(TypedDict):
    source_path: str
    materialized_path: str
    pubchem_total: int
    source_url: str
    source_bytes: int
    source_mtime_ns: int


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


def _checksum(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ensure_checksum(path: Path, *, md5: str, sha256: str) -> None:
    actual_md5 = _checksum(path, "md5")
    actual_sha256 = _checksum(path, "sha256")
    if actual_md5 != md5:
        raise ValueError(
            "md5 mismatch for {0}: expected {1}, got {2}".format(
                path.name, md5, actual_md5
            )
        )
    if actual_sha256 != sha256:
        raise ValueError(
            "sha256 mismatch for {0}: expected {1}, got {2}".format(
                path.name, sha256, actual_sha256
            )
        )


def download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".tmp")
    print("downloading {0} -> {1}".format(url, destination))
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with tmp_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                handle.write(chunk)
    tmp_path.replace(destination)


def ensure_model_weights(weights_dir: Path) -> WeightsInfo:
    weights_dir.mkdir(parents=True, exist_ok=True)
    zip_path = weights_dir / "model.zip"

    need_zip = any(not (weights_dir / filename).exists() for filename in MODEL_FILES)
    if need_zip and not zip_path.exists():
        download_file(MODEL_ZIP_URL, zip_path)

    if zip_path.exists():
        _ensure_checksum(
            zip_path,
            md5=EXPECTED_MD5["model.zip"],
            sha256=EXPECTED_SHA256["model.zip"],
        )

    if any(not (weights_dir / filename).exists() for filename in MODEL_FILES):
        if not zip_path.exists():
            raise FileNotFoundError(
                "model.zip is missing and required HDF5 files are not present"
            )
        with zipfile.ZipFile(zip_path) as archive:
            for filename in MODEL_FILES:
                archive.extract(filename, path=weights_dir)

    files: List[WeightFileInfo] = []
    for filename in MODEL_FILES:
        path = weights_dir / filename
        if not path.exists():
            raise FileNotFoundError(path)
        _ensure_checksum(
            path,
            md5=EXPECTED_MD5[filename],
            sha256=EXPECTED_SHA256[filename],
        )
        files.append(
            {
                "filename": filename,
                "path": str(path),
                "md5": EXPECTED_MD5[filename],
                "sha256": EXPECTED_SHA256[filename],
                "bytes": path.stat().st_size,
            }
        )

    return {
        "doi": MODEL_RECORD_DOI,
        "download_url": MODEL_ZIP_URL,
        "weights_dir": str(weights_dir),
        "files": files,
        "rdkit_version": "unknown",
    }


def ensure_pubchem_input(
    *,
    pubchem_input_path: Path,
    materialized_input_path: Path,
    allow_download: bool,
    pubchem_url: str,
) -> SourceInfo:
    if not pubchem_input_path.exists():
        if not allow_download:
            raise FileNotFoundError(
                "{0} does not exist; rerun with --download-pubchem or provide a local file".format(
                    pubchem_input_path
                )
            )
        download_file(pubchem_url, pubchem_input_path)

    if pubchem_input_path.suffix == ".gz":
        return _materialize_gzip(pubchem_input_path, materialized_input_path, pubchem_url)
    return _count_plain_input(pubchem_input_path, pubchem_url)


def _meta_payload(
    *,
    source_path: Path,
    materialized_path: Path,
    pubchem_total: int,
    source_url: str,
) -> SourceInfo:
    return {
        "source_path": str(source_path),
        "materialized_path": str(materialized_path),
        "pubchem_total": pubchem_total,
        "source_url": source_url,
        "source_bytes": source_path.stat().st_size,
        "source_mtime_ns": source_path.stat().st_mtime_ns,
    }


def _meta_path(materialized_path: Path) -> Path:
    return materialized_path.with_suffix(materialized_path.suffix + ".meta.json")


def _materialize_gzip(
    source_path: Path, materialized_path: Path, source_url: str
) -> SourceInfo:
    meta_path = _meta_path(materialized_path)
    expected_meta: Optional[SourceInfo] = None
    if materialized_path.exists() and meta_path.exists():
        existing = cast(SourceInfo, json.loads(meta_path.read_text()))
        expected_meta = _meta_payload(
            source_path=source_path,
            materialized_path=materialized_path,
            pubchem_total=existing.get("pubchem_total", 0),
            source_url=source_url,
        )
        if (
            existing.get("source_path") == expected_meta["source_path"]
            and existing.get("materialized_path") == expected_meta["materialized_path"]
            and existing.get("source_url") == expected_meta["source_url"]
            and existing.get("source_bytes") == expected_meta["source_bytes"]
            and existing.get("source_mtime_ns") == expected_meta["source_mtime_ns"]
        ):
            return existing

    materialized_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = materialized_path.with_suffix(materialized_path.suffix + ".tmp")
    pubchem_total = 0
    print("materializing {0} -> {1}".format(source_path, materialized_path))
    with gzip.open(source_path, "rb") as compressed, tmp_path.open("wb") as plain:
        for raw_line in compressed:
            line = raw_line.decode("utf-8")
            if _parse_pubchem_line(line) is not None:
                pubchem_total += 1
            plain.write(raw_line)

    tmp_path.replace(materialized_path)
    payload = _meta_payload(
        source_path=source_path,
        materialized_path=materialized_path,
        pubchem_total=pubchem_total,
        source_url=source_url,
    )
    meta_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def _count_plain_input(source_path: Path, source_url: str) -> SourceInfo:
    meta_path = _meta_path(source_path)
    if meta_path.exists():
        existing = cast(SourceInfo, json.loads(meta_path.read_text()))
        if (
            existing.get("source_path") == str(source_path)
            and existing.get("materialized_path") == str(source_path)
            and existing.get("source_url") == source_url
            and existing.get("source_bytes") == source_path.stat().st_size
            and existing.get("source_mtime_ns") == source_path.stat().st_mtime_ns
        ):
            return existing

    pubchem_total = 0
    print("counting rows in {0}".format(source_path))
    with source_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if _parse_pubchem_line(line) is not None:
                pubchem_total += 1

    payload = _meta_payload(
        source_path=source_path,
        materialized_path=source_path,
        pubchem_total=pubchem_total,
        source_url=source_url,
    )
    meta_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload
