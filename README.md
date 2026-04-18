# npc-labeler

[![CI](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml)

Single-container offline labelling of the current PubChem CID-SMILES dump with the recovered NPClassifier weights and a pinned legacy RDKit.

NPClassifier is a neural-network classifier for natural products that predicts pathway, superclass, and class labels from molecular structure, and PubChem is NCBI's large public compound registry; in this repo, "run PubChem" means taking the published `CID-SMILES.gz` export and classifying it offline in chunks. The original [`mwang87/NP-Classifier`](https://github.com/mwang87/NP-Classifier) is awkward to operate reproducibly at PubChem scale, so this pipeline keeps the job simple with one container, one pinned environment, one offline pass over PubChem, 10M-row Parquet chunks for row metadata, and separate `float16` score matrices compressed with `zstd`. It was validated against the public NPClassifier API snapshot for the first 10,000 PubChem rows: for aligned PubChem CIDs, the local run matched the API-visible `smiles`, `pathway_results`, `superclass_results`, `class_results`, and `isglycoside` outputs exactly, which strongly suggests that the model and RDKit versions are correctly pinned, even though raw score vectors are not public and therefore were not compared byte-for-byte.

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
