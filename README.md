# NPC-Labeler

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14040990.svg)](https://doi.org/10.5281/zenodo.14040990)

Utility to run the NPC APIs to classify SMILES, plus preprocessed datasets.

## Datasets

Using this utility, we have already labelled SMILES from the following datasets which we share on Zenodo.

All GNPS MGF [are downloaded from the GNPS library](https://external.gnps2.org/gnpslibrary). The PubChem SMILES are downloaded from the [PubChem FTP](https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/).

| Dataset | JSON | CSV | Total SMILES | Partially classified SMILES |
|---------|------|--------------------------------------------------------------------------------------------------------------------|--------------|-------------------|
| GNPS    |          | 119031918    | 112486925         |
| PubChem |                                                                                                                     | 106000000    | 106000000         |

### Dataset format

The datasets are stored in a gzip-ed JSON file with the following format:

```json
[
    {
        "class_results": [
            "Cyclic peptides",
            "Microcystins"
        ],
        "superclass_results": [
            "Oligopeptides"
        ],
        "pathway_results": [
            "Amino acids and Peptides"
        ],
        "isglycoside": false,
        "smiles": "CC(C=CC1NC(=O)C(CCCN=C(N)N)NC(=O)C(C)C(C(=O)O)NC(=O)C(CC(C)C)=NC(=O)C(C)NC(=O)C(C)N(C)C(=O)CCC(C(=O)O)NC(=O)C1C)=CC(C)C(O)Cc1ccccc1"
    },
    {
        "class_results": [
            "Cyclic peptides",
            "Depsipeptides"
        ],
        "superclass_results": [
            "Oligopeptides"
        ],
        "pathway_results": [
            "Amino acids and Peptides",
            "Polyketides"
        ],
        "isglycoside": false,
        "smiles": "CC(=O)OC1c2nc(cs2)C(=O)OC(CCCC(C)(Cl)[37Cl])C(C)C(=O)OC(C(C)(C)O)c2nc(cs2)C(=O)OC1(C)C"
    }
]
```

## Usage

First, clone this repository:

```bash
git clone https://github.com/LucaCappelletti94/npc-labeler.git
```

Navigate in it and install the requirements:

```bash
cd npc-labeler
pip install -r requirements.txt
```

Then, you can run the labeler by providing the input file and the output file:

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
