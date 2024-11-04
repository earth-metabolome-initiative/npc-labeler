# NPC-Labeler

Utility to run the NPC APIs

## Usage

```bash
python3 labeler.py --input <input_file> --output <output_file>
```

For instance, suppose you want to classify the SMILES in the metadata of an MGF document and store it into a `classified_matchms.json.gz` file. You can do it by running:

```bash
python3 labeler.py --input matchms.mgf --output classified_matchms.json.gz
```

Similarly, for a SSV file:

```bash
python3 labeler.py --input CID-SMILES.ssv --output pubchem.json.gz
```

