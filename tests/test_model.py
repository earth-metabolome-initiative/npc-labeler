import unittest
from importlib.util import find_spec

import numpy as np


class NPClassifierBatchingTest(unittest.TestCase):
    def test_batched_prepared_classification_matches_scalar(self) -> None:
        if find_spec("rdkit") is None:
            self.skipTest("rdkit is not available in this environment")

        try:
            from npc_labeler.model import NPClassifier, PreparedRecord
        except Exception as error:  # noqa: BLE001
            self.skipTest("classifier import failed: {0}".format(error))

        ontology = {
            "Pathway": {"pathway-a": 0, "pathway-b": 1},
            "Superclass": {"super-a": 0, "super-b": 1},
            "Class": {"class-a": 0, "class-b": 1},
            "Super_hierarchy": {
                "0": {"Pathway": [0]},
                "1": {"Pathway": [1]},
            },
            "Class_hierarchy": {
                "0": {"Pathway": [0], "Superclass": [0]},
                "1": {"Pathway": [1], "Superclass": [1]},
            },
        }
        kernel = np.array(
            [
                [1.0, 0.0],
                [0.0, 1.0],
                [0.0, 0.0],
                [0.0, 0.0],
                [0.0, 0.0],
            ],
            dtype=np.float32,
        )
        bias = np.zeros((2,), dtype=np.float32)
        models = {
            model_name: [
                (
                    "dense",
                    "dense",
                    {
                        "kernel": kernel.copy(),
                        "bias": bias.copy(),
                        "activation": "linear",
                    },
                )
            ]
            for model_name in ("SUPERCLASS", "CLASS", "PATHWAY")
        }
        classifier = NPClassifier(models=models, ontology=ontology)
        prepared_records = [
            PreparedRecord(
                cid=1,
                smiles="mol-1",
                is_glycoside=False,
                fp1=np.array([0.8, 0.0], dtype=np.float32),
                fp2=np.zeros((3,), dtype=np.float32),
            ),
            PreparedRecord(
                cid=2,
                smiles="mol-2",
                is_glycoside=True,
                fp1=np.array([0.0, 0.9], dtype=np.float32),
                fp2=np.zeros((3,), dtype=np.float32),
            ),
        ]

        expected_records = [
            classifier.classify_prepared_record(prepared_record)
            for prepared_record in prepared_records
        ]
        batched_records = classifier.classify_prepared_batch(prepared_records)

        self.assertEqual(expected_records, batched_records)


if __name__ == "__main__":
    unittest.main()
