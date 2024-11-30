"""Script to count how many entries are fully classified by the NP Classifier API."""

from typing import Dict, List, Union
from glob import glob
import compress_json
from tqdm.auto import tqdm
from cache_decorator import Cache
import pandas as pd

KEYS: List[str] = ["class_results", "superclass_results", "pathway_results"]


@Cache(
    use_source_code=False,
    cache_path="{cache_dir}/{_hash}.json",
    cache_dir="scripts/classified_counts",
)
def count_classified_counts(path: str) -> Dict[str, Union[int, str]]:
    """Returns the number of fully classified entries."""
    data = compress_json.load(path)
    fully_labelled_counts = 0

    for entry in tqdm(
        data,
        desc="Counting classified counts",
        unit="entry",
        total=len(data),
        leave=False,
        dynamic_ncols=True,
    ):
        if all(entry.get(key) for key in KEYS):
            fully_labelled_counts += 1

    return {
        "path": path,
        "fully_labelled_counts": fully_labelled_counts,
        "total_counts": len(data),
    }


def count_all():
    """Returns the number of fully classified entries."""
    counts = []
    for path in tqdm(
        glob("labelled_mgf/*.json.gz"),
        desc="Counting classified counts",
        unit="file",
        total=len(glob("labelled_mgf/*.json.gz")),
        leave=False,
        dynamic_ncols=True,
    ):
        counts.append(count_classified_counts(path))

    pd.DataFrame(counts).to_csv("labelled_mgf_counts.csv", index=False)


if __name__ == "__main__":
    count_all()