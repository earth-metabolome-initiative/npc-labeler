# NPC-Labeler

Utility to run the NPC APIs to classify SMILES, plus preprocessed datasets.

## Datasets

Using this utility, we have already labelled SMILES from the following datasets which we share on Zenodo.

| Dataset                                                                     | Description                                        | Labels                                                                                                       | Total SMILES | Classified SMILES |
|-----------------------------------------------------------------------------|----------------------------------------------------|--------------------------------------------------------------------------------------------------------------|--------------|-------------------|
| [GNPS Cleaning + MatchMS](https://external.gnps2.org/gnpslibrary)           | Preprocessed MS/MS spectra from GNPS using MatchMS | [Download from Zenodo](https://zenodo.org/records/14039039/files/classified_matchms.json.gz?download=1)      | 54066        | 54059             |
| [GNPS Cleaning](https://external.gnps2.org/gnpslibrary)                     | Preprocessed MS/MS spectra from GNPS               | [Download from Zenodo](https://zenodo.org/records/14039239/files/classified_gnps_cleaned.json.gz?download=1) | 53362        | 53355             |
| [ALL_GNPS_NO_PROPOGATED](https://external.gnps2.org/gnpslibrary)            | Spectra from GNPS                                  | In progress                                                                                                  | 75745        | 41350             |
| [PubChem CID-SMILES](https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/) | CID-SMILES from PubChem                            | In progress                                                                                                  | 119031918    | 306185            |

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
