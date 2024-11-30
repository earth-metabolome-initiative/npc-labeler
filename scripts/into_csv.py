"""Script to convert JSON entries into CSVs."""

from glob import glob
import compress_json
from tqdm.auto import tqdm


def convert_into_csv(path: str):
    """Converts JSON entries into CSVs."""
    data = compress_json.load(path)

    if not data:
        return
    columns = list(data[0].keys())

    with open(path.replace(".json.gz", ".csv"), "w", encoding="Utf8") as file:
        file.write(",".join(columns) + "\n")

        for entry in tqdm(
            data,
            desc="Converting JSON entries into CSVs",
            unit="entry",
            total=len(data),
            leave=False,
            dynamic_ncols=True,
        ):
            for column in columns:
                value = entry.get(column)
                if isinstance(value, list):
                    value = "|".join(value)
                file.write(f"{value}")
                if column != columns[-1]:
                    file.write(",")
            file.write("\n")


def convert_into_csv_all():
    """Returns the number of fully classified entries."""
    for path in tqdm(
        glob("labelled_mgf/*.json.gz"),
        desc="Converting all JSON entries into CSVs",
        unit="file",
        total=len(glob("labelled_mgf/*.json.gz")),
        leave=False,
        dynamic_ncols=True,
    ):
        convert_into_csv(path)


if __name__ == "__main__":
    convert_into_csv_all()
