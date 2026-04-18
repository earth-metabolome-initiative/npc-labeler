# npc-labeler

[![CI](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/earth-metabolome-initiative/npc-labeler/actions/workflows/ci.yml)

Single-container offline labelling of the current PubChem CID-SMILES dump with the recovered NPClassifier weights and a pinned legacy RDKit.

NPClassifier is a neural-network classifier for natural products. Given a structure, it predicts labels in the NPClassifier ontology at three levels: pathway, superclass, and class.

PubChem is NCBI's large public compound registry. In this repo, "run PubChem" means taking the published `CID-SMILES.gz` export from PubChem and classifying the whole thing offline, in chunks.

This repo exists because the original [`mwang87/NP-Classifier`](https://github.com/mwang87/NP-Classifier) is awkward to operate reproducibly at PubChem scale. The open issues in that repository describe the main pain points: RDKit version changes can alter Morgan fingerprints and therefore predictions ([#64](https://github.com/mwang87/NP-Classifier/issues/64)); local runs can disagree with the website/API ([#63](https://github.com/mwang87/NP-Classifier/issues/63)); the API does not report the model version ([#59](https://github.com/mwang87/NP-Classifier/issues/59)); ontology/index metadata has been hard to consume programmatically ([#35](https://github.com/mwang87/NP-Classifier/issues/35), [#39](https://github.com/mwang87/NP-Classifier/issues/39), [#47](https://github.com/mwang87/NP-Classifier/issues/47)); returned label ordering has been confusing ([#29](https://github.com/mwang87/NP-Classifier/issues/29)); and some molecules still trigger RDKit problems ([#14](https://github.com/mwang87/NP-Classifier/issues/14)).

This pipeline keeps the job simple: one container, one pinned environment, one offline pass over PubChem, 10M-row Parquet chunks for row metadata, and separate `float16` score matrices compressed with `zstd`.

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
