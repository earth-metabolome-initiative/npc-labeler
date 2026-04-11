# NPC-Labeler

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14040990.svg)](https://doi.org/10.5281/zenodo.14040990)
[![CI](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml/badge.svg)](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml)
![crawl status](https://img.shields.io/badge/crawl_status-5%2C480%2C150%2F123%2C500%2C000_labeled_%7C_234.1%2Fmin_%7C_ETA_~2027--03--27-blue)

NPC-Labeler builds an open dataset for natural product classification by querying the [NPClassifier](https://npclassifier.gnps2.org/) API across PubChem SMILES and writing successful results to compressed JSONL chunks.

> [!IMPORTANT]
> We do not recommend running this yourself. An ongoing crawl is already publishing weekly snapshots to [Zenodo](https://doi.org/10.5281/zenodo.14040990). If you need the data, use the Zenodo release instead of adding load to the NPClassifier API.

## Quick Start and then wait for a year

```bash
cargo build --release
curl -O https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-SMILES.gz
./target/release/npc-labeler --input CID-SMILES.gz
```

Run the same command again to resume after interruption.

Successful classifications are written to `completed/` as chunked `jsonl.zst` files. Local resume state is stored under `state/`.

On startup, the tool also prints an [ntfy](https://ntfy.sh) subscribe URL for progress notifications.
