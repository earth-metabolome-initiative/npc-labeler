# npc-labeler

Single-container offline PubChem labelling with the recovered NPClassifier weights and legacy RDKit.

```bash
docker build -t npc-labeler .
docker run --rm -it -v "$PWD/work:/work" npc-labeler run --download-pubchem
```
