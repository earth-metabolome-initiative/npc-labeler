from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

import h5py
import numpy as np

from npc_labeler.original import fingerprint_handler, prediction_voting

THRESHOLDS = {"SUPERCLASS": 0.3, "CLASS": 0.1, "PATHWAY": 0.5}
MODEL_FILES = {
    "PATHWAY": "NP_classifier_pathway_V1.hdf5",
    "SUPERCLASS": "NP_classifier_superclass_V1.hdf5",
    "CLASS": "NP_classifier_class_V1.hdf5",
}


class InvalidSmilesError(ValueError):
    """Raised when RDKit cannot parse a SMILES string."""


class RdkitPipelineError(RuntimeError):
    """Raised when the legacy RDKit-dependent helpers fail after parsing."""


class ClassificationRuntimeError(RuntimeError):
    """Raised when the local classifier stack fails for non-RDKit reasons."""


@dataclass
class PreparedRecord:
    cid: int
    smiles: str
    is_glycoside: bool
    fp1: np.ndarray
    fp2: np.ndarray


def _ontology_path() -> Path:
    return Path(__file__).resolve().parent / "original" / "dict" / "index_v1.json"


def _load_model(path: Path) -> List[Tuple[str, str, Dict[str, object]]]:
    with h5py.File(path, "r") as handle:
        config = json.loads(handle.attrs["model_config"])
        weights_root = handle["model_weights"]
        layers: List[Tuple[str, str, Dict[str, object]]] = []

        for layer_config in config["config"]["layers"]:
            layer_type = layer_config["class_name"]
            name = layer_config["name"]
            if layer_type == "InputLayer":
                continue
            if layer_type == "Concatenate":
                layers.append(("concat", name, {}))
                continue
            if layer_type == "Dropout":
                layers.append(("dropout", name, {}))
                continue
            if layer_type == "Dense":
                group = weights_root[name][name]
                layers.append(
                    (
                        "dense",
                        name,
                        {
                            "kernel": group["kernel:0"][()].astype(np.float32),
                            "bias": group["bias:0"][()].astype(np.float32),
                            "activation": layer_config["config"]["activation"],
                        },
                    )
                )
                continue
            if layer_type == "BatchNormalization":
                group = weights_root[name][name]
                layers.append(
                    (
                        "batch_norm",
                        name,
                        {
                            "gamma": group["gamma:0"][()].astype(np.float32),
                            "beta": group["beta:0"][()].astype(np.float32),
                            "mean": group["moving_mean:0"][()].astype(np.float32),
                            "variance": group["moving_variance:0"][()].astype(np.float32),
                            "epsilon": float(layer_config["config"].get("epsilon", 1e-3)),
                        },
                    )
                )
                continue
            raise RuntimeError("unexpected layer type {0!r} in {1}".format(layer_type, path.name))

    return layers


def _sigmoid(values: np.ndarray) -> np.ndarray:
    output = np.empty_like(values)
    positives = values >= 0
    output[positives] = 1.0 / (1.0 + np.exp(-values[positives]))
    negatives = np.exp(values[~positives])
    output[~positives] = negatives / (1.0 + negatives)
    return output


def _forward(
    layers: List[Tuple[str, str, Dict[str, object]]],
    fp1: np.ndarray,
    fp2: np.ndarray,
) -> np.ndarray:
    values = np.concatenate([fp1, fp2], axis=-1).astype(np.float32, copy=False)
    for op, _name, params in layers:
        if op == "concat":
            continue
        if op == "dense":
            kernel = cast(np.ndarray, params["kernel"])
            bias = cast(np.ndarray, params["bias"])
            activation = cast(str, params["activation"])
            values = values @ kernel + bias
            if activation == "relu":
                values = np.maximum(values, 0.0)
            elif activation == "sigmoid":
                values = _sigmoid(values)
            elif activation != "linear":
                raise RuntimeError("unsupported activation {0!r}".format(activation))
            continue
        if op == "batch_norm":
            gamma = cast(np.ndarray, params["gamma"])
            mean = cast(np.ndarray, params["mean"])
            variance = cast(np.ndarray, params["variance"])
            beta = cast(np.ndarray, params["beta"])
            epsilon = cast(float, params["epsilon"])
            values = (
                gamma
                * (values - mean)
                / np.sqrt(variance + epsilon)
                + beta
            )
            continue
        if op == "dropout":
            continue
        raise RuntimeError("unexpected op {0!r}".format(op))
    return values


class NPClassifier:
    def __init__(
        self,
        models: Dict[str, List[Tuple[str, str, Dict[str, object]]]],
        ontology: Optional[Dict[str, object]] = None,
    ) -> None:
        self.models = models
        self.ontology = cast(
            Dict[str, Any],
            ontology if ontology is not None else json.loads(_ontology_path().read_text()),
        )
        self.pathway_to_id = cast(Dict[str, int], self.ontology["Pathway"])
        self.superclass_to_id = cast(Dict[str, int], self.ontology["Superclass"])
        self.class_to_id = cast(Dict[str, int], self.ontology["Class"])
        self.super_hierarchy = cast(Dict[str, Dict[str, List[int]]], self.ontology["Super_hierarchy"])
        self.class_hierarchy = cast(Dict[str, Dict[str, List[int]]], self.ontology["Class_hierarchy"])
        self.pathway_labels = self._ordered_labels(self.pathway_to_id)
        self.superclass_labels = self._ordered_labels(self.superclass_to_id)
        self.class_labels = self._ordered_labels(self.class_to_id)
        self._vector_widths = {
            "pathway_prediction_vector": len(self.pathway_to_id),
            "superclass_prediction_vector": len(self.superclass_to_id),
            "class_prediction_vector": len(self.class_to_id),
        }
        self.rdkit_version = getattr(
            getattr(fingerprint_handler, "rdkit", None), "__version__", "unknown"
        )

    @classmethod
    def from_weights_dir(cls, weights_dir: Path) -> "NPClassifier":
        models = {}
        for model_name, filename in MODEL_FILES.items():
            models[model_name] = _load_model(weights_dir / filename)
        return cls(models=models)

    def vocabulary_payload(self) -> Dict[str, List[str]]:
        return {
            "pathway": self.pathway_labels,
            "superclass": self.superclass_labels,
            "class": self.class_labels,
        }

    def vector_widths(self) -> Dict[str, int]:
        return dict(self._vector_widths)

    @staticmethod
    def _ordered_labels(mapping: Dict[str, int]) -> List[str]:
        output = [""] * (max(mapping.values()) + 1)
        for label, index in mapping.items():
            output[index] = label
        return output

    def prepare_record(self, cid: int, smiles: str) -> PreparedRecord:
        molecule = fingerprint_handler.Chem.MolFromSmiles(smiles)
        if molecule is None:
            raise InvalidSmilesError("RDKit could not parse the SMILES string")

        try:
            is_glycoside = fingerprint_handler._isglycoside(smiles)
            if not isinstance(is_glycoside, bool):
                raise RdkitPipelineError(
                    "legacy glycoside helper returned a non-boolean value"
                )
            fp2048, fp4096 = fingerprint_handler.calculate_fingerprint(smiles, 2)
        except RdkitPipelineError:
            raise
        except Exception as error:  # noqa: BLE001
            raise RdkitPipelineError(str(error)) from error

        return PreparedRecord(
            cid=cid,
            smiles=smiles,
            is_glycoside=is_glycoside,
            fp1=np.asarray(fp2048[0], dtype=np.float32),
            fp2=np.asarray(fp4096[0], dtype=np.float32),
        )

    def predict_batch(
        self, prepared_records: List[PreparedRecord]
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        if not prepared_records:
            vector_widths = self.vector_widths()
            return (
                np.empty((0, vector_widths["superclass_prediction_vector"]), dtype=np.float32),
                np.empty((0, vector_widths["class_prediction_vector"]), dtype=np.float32),
                np.empty((0, vector_widths["pathway_prediction_vector"]), dtype=np.float32),
            )

        fp1_batch = np.stack([record.fp1 for record in prepared_records], axis=0)
        fp2_batch = np.stack([record.fp2 for record in prepared_records], axis=0)
        return (
            _forward(self.models["SUPERCLASS"], fp1_batch, fp2_batch),
            _forward(self.models["CLASS"], fp1_batch, fp2_batch),
            _forward(self.models["PATHWAY"], fp1_batch, fp2_batch),
        )

    def _pathways_from_superclasses(self, superclass_ids: List[int]) -> List[int]:
        pathway_ids = set()
        for label in superclass_ids:
            pathway_ids.update(self.super_hierarchy[str(label)]["Pathway"])
        return list(pathway_ids)

    def _pathways_from_classes(self, class_ids: List[int]) -> List[int]:
        pathway_ids = set()
        for label in class_ids:
            pathway_ids.update(self.class_hierarchy[str(label)]["Pathway"])
        return list(pathway_ids)

    def record_from_predictions(
        self,
        prepared_record: PreparedRecord,
        pred_super: np.ndarray,
        pred_class: np.ndarray,
        pred_path: np.ndarray,
    ) -> Dict[str, object]:
        try:
            n_super = list(np.flatnonzero(pred_super >= THRESHOLDS["SUPERCLASS"]))
            n_class = list(np.flatnonzero(pred_class >= THRESHOLDS["CLASS"]))
            n_path = list(np.flatnonzero(pred_path >= THRESHOLDS["PATHWAY"]))

            path_from_super = self._pathways_from_superclasses(n_super)
            path_from_class = self._pathways_from_classes(n_class)

            pathway_results, superclass_results, class_results, is_glycoside_out = (
                prediction_voting.vote_classification(
                    n_path,
                    n_class,
                    n_super,
                    pred_class,
                    pred_super,
                    path_from_class,
                    path_from_super,
                    prepared_record.is_glycoside,
                    self.ontology,
                )
            )
            if not isinstance(is_glycoside_out, bool):
                raise ClassificationRuntimeError(
                    "legacy voting returned a non-boolean glycoside flag"
                )
        except ClassificationRuntimeError:
            raise
        except Exception as error:  # noqa: BLE001
            raise ClassificationRuntimeError(str(error)) from error

        return {
            "cid": prepared_record.cid,
            "smiles": prepared_record.smiles,
            "pathway_ids": [self.pathway_to_id[label] for label in pathway_results],
            "superclass_ids": [self.superclass_to_id[label] for label in superclass_results],
            "class_ids": [self.class_to_id[label] for label in class_results],
            "isglycoside": is_glycoside_out,
            "pathway_prediction_vector": list(pred_path.astype(np.float16)),
            "superclass_prediction_vector": list(pred_super.astype(np.float16)),
            "class_prediction_vector": list(pred_class.astype(np.float16)),
            "parse_failed": False,
            "rdkit_failed": False,
            "other_failure": False,
            "error_message": None,
        }

    def classify_prepared_record(self, prepared_record: PreparedRecord) -> Dict[str, object]:
        pred_super, pred_class, pred_path = self.predict_batch([prepared_record])
        return self.record_from_predictions(
            prepared_record=prepared_record,
            pred_super=pred_super[0],
            pred_class=pred_class[0],
            pred_path=pred_path[0],
        )

    def classify_prepared_batch(
        self, prepared_records: List[PreparedRecord]
    ) -> List[Dict[str, object]]:
        if not prepared_records:
            return []

        pred_super_batch, pred_class_batch, pred_path_batch = self.predict_batch(
            prepared_records
        )
        records: List[Dict[str, object]] = []
        for index, prepared_record in enumerate(prepared_records):
            records.append(
                self.record_from_predictions(
                    prepared_record=prepared_record,
                    pred_super=pred_super_batch[index],
                    pred_class=pred_class_batch[index],
                    pred_path=pred_path_batch[index],
                )
            )
        return records

    def classify_record(self, cid: int, smiles: str) -> Dict[str, object]:
        prepared_record = self.prepare_record(cid=cid, smiles=smiles)
        return self.classify_prepared_record(prepared_record)
