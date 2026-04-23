# npc-labeler

[![CI](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml)
[![Main PubChem Dataset DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19701309.svg)](https://doi.org/10.5281/zenodo.19701309)
[![Distilled Teacher Splits DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19701295.svg)](https://doi.org/10.5281/zenodo.19701295)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Single-container offline labelling of the current PubChem CID-SMILES dump with the recovered NPClassifier weights and a pinned legacy RDKit.

NPClassifier is a neural-network classifier for natural products that predicts pathway, superclass, and class labels from molecular structure, and PubChem is NCBI's large public compound registry; in this repo, "run PubChem" means taking the published `CID-SMILES.gz` export and classifying it offline in chunks. The original [`mwang87/NP-Classifier`](https://github.com/mwang87/NP-Classifier) is awkward to operate reproducibly at PubChem scale, so this pipeline keeps the job simple with one container, one pinned environment, one offline pass over PubChem, 10M-row Parquet chunks for row metadata, and separate `float16` score matrices compressed with `zstd`. It was validated against the public NPClassifier API snapshot for the first 10,000 PubChem rows: for aligned PubChem CIDs, the local run matched the API-visible `smiles`, `pathway_results`, `superclass_results`, `class_results`, and `isglycoside` outputs exactly, which strongly suggests that the model and RDKit versions are correctly pinned, even though raw score vectors are not public and therefore were not compared byte-for-byte.

The published datasets are available on Zenodo as the [full PubChem annotation release](https://doi.org/10.5281/zenodo.19701309) and the [distilled teacher-splits release](https://doi.org/10.5281/zenodo.19701295). The released PubChem data is row-aligned across files. `rows-all.parquet` is the canonical table and stores `cid`, `smiles`, `pathway_ids`, `superclass_ids`, `class_ids`, `isglycoside`, `parse_failed`, `rdkit_failed`, `other_failure`, and `error_message`, with integer ids resolved through `vocabulary.json`. The merged score sidecars are dense raw row-major `float16` matrices compressed with `zstd`: `pathway-vectors-all.f16.zst` has shape `[123857431, 7]` and `superclass-vectors-all.f16.zst` has shape `[123857431, 77]`. For the compact release, `class-max-scores.parquet` stores only the scores for predicted classes as `class_scores: list<halffloat>`; row `i` is aligned with row `i` in `rows-all.parquet`, and `class_scores[i]` is aligned to `class_ids[i]` rather than to the full 687-class vocabulary.

```text
rows-all.parquet
  cid: int64
  smiles: string
  pathway_ids: list<uint16>
  superclass_ids: list<uint16>
  class_ids: list<uint16>
  isglycoside: bool
  parse_failed: bool
  rdkit_failed: bool
  other_failure: bool
  error_message: string

pathway-vectors-all.f16.zst
  float16[123857431, 7]

superclass-vectors-all.f16.zst
  float16[123857431, 77]

release/class-max-scores.parquet
  class_scores: list<halffloat>
```

```bash
docker build -t npc-labeler .
docker run --rm -it \
  -e OPENBLAS_NUM_THREADS=16 \
  -e OMP_NUM_THREADS=16 \
  -e MKL_NUM_THREADS=16 \
  -e NUMEXPR_NUM_THREADS=16 \
  -v "$PWD/work:/work" \
  npc-labeler run --download-pubchem
```
