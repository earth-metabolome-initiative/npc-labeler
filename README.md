# NPC-Labeler

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14040990.svg)](https://doi.org/10.5281/zenodo.14040990)
[![CI](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml/badge.svg)](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml)

A tool to build an open-source training dataset for natural product classification by scraping the [NPClassifier](https://npclassifier.gnps2.org/) API across all of PubChem.

NPClassifier classifies natural products into pathways, superclasses, and classes. The model is not open source: the authors host it as a web service but do not release the model weights or training data. This project queries the API for all ~123M PubChem SMILES and writes successful responses directly into chunked compressed JSONL, creating a fully open dataset that can be used to train an open-source replacement.

> [!IMPORTANT]
> *We do not recommend running this tool yourself. We are already running it and publishing updated snapshots of the dataset to [Zenodo](https://doi.org/10.5281/zenodo.14040990) on a weekly cadence. The code is shared for transparency and reproducibility. If you need the classification data, please use the Zenodo dataset rather than placing additional load on the NPClassifier API.*

## Dataset format

Successful responses are written directly to sealed `jsonl.zst` chunks under `completed/`, then merged into a staged `completed.jsonl.zst` release artifact when publishing.

Each completed JSON record contains:

- `cid`
- `smiles`
- `class_results`
- `superclass_results`
- `pathway_results`
- `isglycoside`

Local runtime state is kept separately in memory-mapped bitvecs under `state/`:

- `done.bitvec`
- `invalid.bitvec`
- `failed.bitvec`
- `chunks.jsonl`

## Usage

### Build

```bash
cargo build --release
```

### First run

Download PubChem SMILES and start classifying:

```bash
curl -O https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-SMILES.gz
./target/release/npc-labeler --input CID-SMILES.gz
```

This streams the input file, skips rows that are already terminal according to the local bitvec state, and writes successful responses into chunked `jsonl.zst` output.

### Resume after interruption

Re-run with the same input file to resume from where it left off:

```bash
./target/release/npc-labeler --input CID-SMILES.gz
```

### Push notifications

On startup, the tool generates a unique [ntfy](https://ntfy.sh) topic and prints the subscribe URL. Open it on your phone or browser to receive a notification at every 1% completion milestone, plus start and finish messages.

### Options

```text
--input <file>   Path to CID-SMILES input file (.gz or plain).
```

## License

See [LICENSE](LICENSE).
