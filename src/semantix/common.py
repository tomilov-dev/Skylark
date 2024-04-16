import re
import sys
import json
import pandas as pd

from pathlib import Path
from abc import ABC, abstractmethod

SRC_DIR = Path(__file__).parent.parent
PROJECT_DIR = SRC_DIR.parent
sys.path.append(str(PROJECT_DIR))

### SHOUDN'T BE DELETED
### IMPORTS THROUGH THIS MODULE
from src.functool.interfaces import Extractor
from src.functool.measures_functool import (
    SearchMode,
    MergeMode,
    Measures,
    MeasuresGracefullExit,
)
from src.functool.cross_semantic_functool import BasicCrosser
from src.functool.words_functool import (
    LanguageRules,
    Language,
    Languages,
    WordsFuncTool,
)


def read_config(path: str | Path) -> dict:
    with open(path, "rb") as file:
        data = json.loads(file.read())
    return data


def parse_rx(
    data: pd.DataFrame,
    extract_col: str = "Regex",
    new_col_name: str = "rx_to_del",
) -> pd.DataFrame:
    data[new_col_name] = data[extract_col].str.findall(r"\(\?\=\.\*.*?\)\)+")
    data[new_col_name] = data[new_col_name].apply(
        lambda rxs: [re.sub(r"\(\?\=\.\*\(", "", rx) for rx in rxs]
    )
    data[new_col_name] = data[new_col_name].apply(
        lambda rxs: [re.sub(r"\)\)$", "", rx) for rx in rxs]
    )
    return data


def del_rx(data: pd.DataFrame, col: str) -> pd.DataFrame:
    def _del(row):
        for rx in row["rx_to_del"]:
            row["row"] = re.sub(rx, "", row["row"], flags=re.IGNORECASE)
        return row

    data["row"] = data[col].astype(str) + " "
    data = parse_rx(data)

    data = data.apply(_del, axis=1)
    data.drop("rx_to_del", axis=1, inplace=True)
    return data
