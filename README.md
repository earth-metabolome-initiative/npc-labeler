# NPC-Labeler

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14040990.svg)](https://doi.org/10.5281/zenodo.14040990)

Utility to run the NPC APIs to classify SMILES, plus preprocessed datasets.

## Datasets

Using this utility, we have already labelled SMILES from the following datasets which we share on Zenodo.

| Dataset                                                                                   | Description                                        | Labels                                                                                                                               | Total SMILES | Classified SMILES |
|-------------------------------------------------------------------------------------------|----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|--------------|-------------------|
| [GNPS Cleaning + MatchMS](https://external.gnps2.org/gnpslibrary)                         | Preprocessed MS/MS spectra from GNPS using MatchMS | [Download from Zenodo](https://zenodo.org/records/14039039/files/classified_matchms.json.gz?download=1)                              | 54066        | 54059             |
| [GNPS Cleaning](https://external.gnps2.org/gnpslibrary)                                   | Preprocessed MS/MS spectra from GNPS               | [Download from Zenodo](https://zenodo.org/records/14039239/files/classified_gnps_cleaned.json.gz?download=1)                         | 53362        | 53355             |
| [GNPS-LIBRARY](https://external.gnps2.org/gnpslibrary)                                    | Spectra from GNPS Library                          | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-LIBRARY.json.gz?download=1)                                    | 5617         | 5581              |
| [GNPS-SELLECKCHEM-FDA-PART1](https://external.gnps2.org/gnpslibrary)                      | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-SELLECKCHEM-FDA-PART1.json.gz?download=1)                      | 285          | 285               |
| [GNPS-SELLECKCHEM-FDA-PART2](https://external.gnps2.org/gnpslibrary)                      | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-SELLECKCHEM-FDA-PART2.json.gz?download=1)                      | 536          | 536               |
| [GNPS-PRESTWICKPHYTOCHEM](https://external.gnps2.org/gnpslibrary)                         | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-PRESTWICKPHYTOCHEM.json.gz?download=1)                         | 140          | 140               |
| [GNPS-NIH-CLINICALCOLLECTION1](https://external.gnps2.org/gnpslibrary)                    | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-NIH-CLINICALCOLLECTION1.json.gz?download=1)                    | 323          | 323               |
| [GNPS-NIH-CLINICALCOLLECTION2](https://external.gnps2.org/gnpslibrary)                    | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-NIH-CLINICALCOLLECTION2.json.gz?download=1)                    | 17           | 16                |
| [GNPS-NIH-NATURALPRODUCTSLIBRARY](https://external.gnps2.org/gnpslibrary)                 | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-NIH-NATURALPRODUCTSLIBRARY.json.gz?download=1)                 | 1255         | 1255              |
| [GNPS-NIH-NATURALPRODUCTSLIBRARY_ROUND2_POSITIVE](https://external.gnps2.org/gnpslibrary) | Positive spectra from GNPS                         | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-NIH-NATURALPRODUCTSLIBRARY_ROUND2_POSITIVE.json.gz?download=1) | 3616         | 3616              |
| [GNPS-NIH-NATURALPRODUCTSLIBRARY_ROUND2_NEGATIVE](https://external.gnps2.org/gnpslibrary) | Negative spectra from GNPS                         | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-NIH-NATURALPRODUCTSLIBRARY_ROUND2_NEGATIVE.json.gz?download=1) | 1464         | 1464              |
| [GNPS-NIH-SMALLMOLECULEPHARMACOLOGICALLYACTIVE](https://external.gnps2.org/gnpslibrary)   | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-NIH-SMALLMOLECULEPHARMACOLOGICALLYACTIVE.json.gz?download=1)   | 1385         | 1385              |
| [GNPS-FAULKNERLEGACY](https://external.gnps2.org/gnpslibrary)                             | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-FAULKNERLEGACY.json.gz?download=1)                             | 2            | 2                 |
| [GNPS-EMBL-MCF](https://external.gnps2.org/gnpslibrary)                                   | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-EMBL-MCF.json.gz?download=1)                                   | 331          | 331               |
| [GNPS-COLLECTIONS-PESTICIDES-POSITIVE](https://external.gnps2.org/gnpslibrary)            | Positive spectra from GNPS                         | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-COLLECTIONS-PESTICIDES-POSITIVE.json.gz?download=1)            | 171          | 171               |
| [GNPS-COLLECTIONS-PESTICIDES-NEGATIVE](https://external.gnps2.org/gnpslibrary)            | Negative spectra from GNPS                         | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-COLLECTIONS-PESTICIDES-NEGATIVE.json.gz?download=1)            | 45           | 45                |
| [MMV_POSITIVE](https://external.gnps2.org/gnpslibrary)                                    | Positive spectra from MMV                          | [Download from Zenodo](https://zenodo.org/records/14040990/files/MMV_POSITIVE.json.gz?download=1)                                    | 110          | 110               |
| [MMV_NEGATIVE](https://external.gnps2.org/gnpslibrary)                                    | Negative spectra from MMV                          | [Download from Zenodo](https://zenodo.org/records/14040990/files/MMV_NEGATIVE.json.gz?download=1)                                    | 47           | 47                |
| [LDB_POSITIVE](https://external.gnps2.org/gnpslibrary)                                    | Positive spectra from LDB                          | [Download from Zenodo](https://zenodo.org/records/14040990/files/LDB_POSITIVE.json.gz?download=1)                                    | 280          | 280               |
| [LDB_NEGATIVE](https://external.gnps2.org/gnpslibrary)                                    | Negative spectra from LDB                          | [Download from Zenodo](https://zenodo.org/records/14040990/files/LDB_NEGATIVE.json.gz?download=1)                                    | 346          | 346               |
| [GNPS-NIST14-MATCHES](https://external.gnps2.org/gnpslibrary)                             | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-NIST14-MATCHES.json.gz?download=1)                             | 1590         | 1589              |
| [GNPS-COLLECTIONS-MISC](https://external.gnps2.org/gnpslibrary)                           | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-COLLECTIONS-MISC.json.gz?download=1)                           | 6            | 5                 |
| [GNPS-MSMLS](https://external.gnps2.org/gnpslibrary)                                      | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-MSMLS.json.gz?download=1)                                      | 399          | 399               |
| [PSU-MSMLS](https://external.gnps2.org/gnpslibrary)                                       | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/PSU-MSMLS.json.gz?download=1)                                       | 367          | 367               |
| [BILELIB19](https://external.gnps2.org/gnpslibrary)                                       | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/BILELIB19.json.gz?download=1)                                       | 533          | 533               |
| [DEREPLICATOR_IDENTIFIED_LIBRARY](https://external.gnps2.org/gnpslibrary)                 | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/DEREPLICATOR_IDENTIFIED_LIBRARY.json.gz?download=1)                 | 379          | 379               |
| [PNNL-LIPIDS-POSITIVE](https://external.gnps2.org/gnpslibrary)                            | Positive spectra from PNNL                         | [Download from Zenodo](https://zenodo.org/records/14040990/files/PNNL-LIPIDS-POSITIVE.json.gz?download=1)                            | 1            | 1                 |
| [PNNL-LIPIDS-NEGATIVE](https://external.gnps2.org/gnpslibrary)                            | Negative spectra from PNNL                         | -                                                                                                                                    | 0            | 0                 |
| [MIADB](https://external.gnps2.org/gnpslibrary)                                           | Spectra from MIADB                                 | [Download from Zenodo](https://zenodo.org/records/14040990/files/MIADB.json.gz?download=1)                                           | 421          | 417               |
| [HCE-CELL-LYSATE-LIPIDS](https://external.gnps2.org/gnpslibrary)                          | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/HCE-CELL-LYSATE-LIPIDS.json.gz?download=1)                          | 92           | 92                |
| [UM-NPDC](https://external.gnps2.org/gnpslibrary)                                         | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/UM-NPDC.json.gz?download=1)                                         | 23           | 23                |
| [GNPS-NUTRI-METAB-FEM-POS](https://external.gnps2.org/gnpslibrary)                        | Positive spectra from GNPS                         | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-NUTRI-METAB-FEM-POS.json.gz?download=1)                        | 259          | 259               |
| [GNPS-NUTRI-METAB-FEM-NEG](https://external.gnps2.org/gnpslibrary)                        | Negative spectra from GNPS                         | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-NUTRI-METAB-FEM-NEG.json.gz?download=1)                        | 197          | 197               |
| [GNPS-SCIEX-LIBRARY](https://external.gnps2.org/gnpslibrary)                              | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-SCIEX-LIBRARY.json.gz?download=1)                              | 314          | 314               |
| [GNPS-IOBA-NHC](https://external.gnps2.org/gnpslibrary)                                   | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-IOBA-NHC.json.gz?download=1)                                   | 142          | 141               |
| [BERKELEY-LAB](https://external.gnps2.org/gnpslibrary)                                    | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/BERKELEY-LAB.json.gz?download=1)                                    | 4124         | 4124              |
| [IQAMDB](https://external.gnps2.org/gnpslibrary)                                          | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/IQAMDB.json.gz?download=1)                                          | 322          | 320               |
| [GNPS-SAM-SIK-KANG-LEGACY-LIBRARY](https://external.gnps2.org/gnpslibrary)                | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-SAM-SIK-KANG-LEGACY-LIBRARY.json.gz?download=1)                | 223          | 219               |
| [GNPS-D2-AMINO-LIPID-LIBRARY](https://external.gnps2.org/gnpslibrary)                     | Spectra from GNPS                                  | -                                                                                                                                    | 0            | 0                 |
| [DRUGS-OF-ABUSE-LIBRARY](https://external.gnps2.org/gnpslibrary)                          | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/DRUGS-OF-ABUSE-LIBRARY.json.gz?download=1)                          | 237          | 237               |
| [ECG-ACYL-AMIDES-C4-C24-LIBRARY](https://external.gnps2.org/gnpslibrary)                  | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/ECG-ACYL-AMIDES-C4-C24-LIBRARY.json.gz?download=1)                  | 1277         | 1277              |
| [ECG-ACYL-ESTERS-C4-C24-LIBRARY](https://external.gnps2.org/gnpslibrary)                  | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/ECG-ACYL-ESTERS-C4-C24-LIBRARY.json.gz?download=1)                  | 496          | 496               |
| [LEAFBOT](https://external.gnps2.org/gnpslibrary)                                         | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/LEAFBOT.json.gz?download=1)                                         | 299          | 299               |
| [XANTHONES-DB](https://external.gnps2.org/gnpslibrary)                                    | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/XANTHONES-DB.json.gz?download=1)                                    | 19           | 19                |
| [TUEBINGEN-NATURAL-PRODUCT-COLLECTION](https://external.gnps2.org/gnpslibrary)            | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/TUEBINGEN-NATURAL-PRODUCT-COLLECTION.json.gz?download=1)            | 343          | 342               |
| [NEO-MSMS](https://external.gnps2.org/gnpslibrary)                                        | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/NEO-MSMS.json.gz?download=1)                                        | 358          | 358               |
| [CMMC-LIBRARY](https://external.gnps2.org/gnpslibrary)                                    | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/CMMC-LIBRARY.json.gz?download=1)                                    | 3610         | 3610              |
| [PHENOLICSDB](https://external.gnps2.org/gnpslibrary)                                     | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/PHENOLICSDB.json.gz?download=1)                                     | 69           | 69                |
| [DMIM-DRUG-METABOLITE-LIBRARY](https://external.gnps2.org/gnpslibrary)                    | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/DMIM-DRUG-METABOLITE-LIBRARY.json.gz?download=1)                    | 1840         | 1840              |
| [ELIXDB-LICHEN-DATABASE](https://external.gnps2.org/gnpslibrary)                          | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/ELIXDB-LICHEN-DATABASE.json.gz?download=1)                          | 529          | 527               |
| [MSNLIB-POSITIVE](https://external.gnps2.org/gnpslibrary)                                 | Positive spectra from MSNLIB                       | [Download from Zenodo](https://zenodo.org/records/14040990/files/MSNLIB-POSITIVE.json.gz?download=1)                                 | 26571        | 26571             |
| [MSNLIB-NEGATIVE](https://external.gnps2.org/gnpslibrary)                                 | Negative spectra from MSNLIB                       | [Download from Zenodo](https://zenodo.org/records/14040990/files/MSNLIB-NEGATIVE.json.gz?download=1)                                 | 26571        | 26571             |
| [GNPS-N-ACYL-LIPIDS-MASSQL](https://external.gnps2.org/gnpslibrary)                       | Spectra from GNPS                                  | _                                                                                                                                    | 0            | 0                 |
| [MCE-DRUG](https://external.gnps2.org/gnpslibrary)                                        | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/MCE-DRUG.json.gz?download=1)                                        | 2994         | 2994              |
| [CMMC-FOOD-BIOMARKERS](https://external.gnps2.org/gnpslibrary)                            | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/CMMC-FOOD-BIOMARKERS.json.gz?download=1)                            | 182          | 182               |
| [ECRFS_DB](https://external.gnps2.org/gnpslibrary)                                        | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/ECRFS_DB.json.gz?download=1)                                        | 102          | 102               |
| [GNPS-IIMN-PROPOGATED](https://external.gnps2.org/gnpslibrary)                            | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-IIMN-PROPOGATED.json.gz?download=1)                            | 45           | 43                |
| [GNPS-SUSPECTLIST](https://external.gnps2.org/gnpslibrary)                                | Spectra from GNPS                                  | _                                                                                                                                    | 0            | 0                 |
| [GNPS-BILE-ACID-MODIFICATIONS](https://external.gnps2.org/gnpslibrary)                    | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/GNPS-BILE-ACID-MODIFICATIONS.json.gz?download=1)                    | 66           | 66                |
| [GNPS-DRUG-ANALOG](https://external.gnps2.org/gnpslibrary)                                | Spectra from GNPS                                  | -                                                                                                                                    | 0            | 0                 |
| [BMDMS-NP](https://external.gnps2.org/gnpslibrary)                                        | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/BMDMS-NP.json.gz?download=1)                                        | 2581         | 2581              |
| [MASSBANK](https://external.gnps2.org/gnpslibrary)                                        | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/MASSBANK.json.gz?download=1)                                        | 9206         | 9108              |
| [MASSBANKEU](https://external.gnps2.org/gnpslibrary)                                      | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/MASSBANKEU.json.gz?download=1)                                      | 692          | 691               |
| [MONA](https://external.gnps2.org/gnpslibrary)                                            | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/MONA.json.gz?download=1)                                            | 3151         | 3151              |
| [HMDB](https://external.gnps2.org/gnpslibrary)                                            | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/HMDB.json.gz?download=1)                                            | 748          | 748               |
| [CASMI](https://external.gnps2.org/gnpslibrary)                                           | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/CASMI.json.gz?download=1)                                           | 449          | 449               |
| [SUMNER](https://external.gnps2.org/gnpslibrary)                                          | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/SUMNER.json.gz?download=1)                                          | 261          | 259               |
| [BIRMINGHAM-UHPLC-MS-POS](https://external.gnps2.org/gnpslibrary)                         | Positive spectra from Birmingham UHPLC-MS          | [Download from Zenodo](https://zenodo.org/records/14040990/files/BIRMINGHAM-UHPLC-MS-NEG.json.gz?download=1)                         | 547          | 547               |
| [BIRMINGHAM-UHPLC-MS-NEG](https://external.gnps2.org/gnpslibrary)                         | Negative spectra from Birmingham UHPLC-MS          | [Download from Zenodo](https://zenodo.org/records/14040990/files/BIRMINGHAM-UHPLC-MS-POS.json.gz?download=1)                         | 549          | 549               |
| [ALL_GNPS_NO_PROPOGATED](https://external.gnps2.org/gnpslibrary)                          | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/ALL_GNPS_NO_PROPOGATED.json.gz?download=1)                          | 75744        | 75587             |
| [ALL_GNPS](https://external.gnps2.org/gnpslibrary)                                        | Spectra from GNPS                                  | [Download from Zenodo](https://zenodo.org/records/14040990/files/ALL_GNPS.json.gz?download=1)                                        | 75798        | 75640             |
| [PubChem CID-SMILES](https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/)               | CID-SMILES from PubChem                            | In progress                                                                                                                          | 119031918    | 11000000          |

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
