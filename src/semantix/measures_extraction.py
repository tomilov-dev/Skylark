import sys
import math
import json
import regex as re
import pandas as pd
import numpy as np

from pathlib import Path
from typing import Callable

SRC_DIR = Path(__file__).parent.parent
PROJECT_DIR = SRC_DIR.parent
sys.path.append(str(PROJECT_DIR))

from src.semantix.common import Extractor, Measures, read_config, MeasuresGracefullExit


class MeasureExtractor(Extractor):
    def __init__(
        self,
        config: dict,
        add_spaces: bool = True,
        status_callback: Callable = None,
        progress_callback: Callable = None,
    ) -> None:
        self._add_spaces_flag = add_spaces
        self.enginge = Measures(config, status_callback, progress_callback)

        self.status_callback = status_callback
        self.progress_callback = progress_callback

        self._stopped = False

    def _add_spaces(self, series: pd.Series) -> pd.Series:
        return "  " + series + "  " if self._add_spaces_flag else series

    def _del_spaces(self, series: pd.Series) -> pd.Series:
        return series.str.strip()

    def _read_config(self, config_path: str | Path) -> dict:
        """Returns config file like dict object"""

        with open(config_path, "rb") as file:
            rules = file.read()
        return json.loads(rules)

    def stop_callback(self) -> None:
        self._stopped = True
        self.enginge.stop_callback()

    def extract(
        self,
        data: pd.DataFrame,
        column: str,
        measure_name: str,
    ) -> pd.Series:
        data.loc[:, column] = data.loc[:, column].astype(str)

        data.loc[:, column] = self._add_spaces(data[column])
        output = self.enginge.extract_measure(data, column, measure_name)
        data.loc[:, column] = self._del_spaces(data[column])
        return output


class MeasuresExtractor(MeasureExtractor):
    def __init__(
        self,
        config: dict,
        add_spaces: bool = True,
        status_callback: Callable = None,
        progress_callback: Callable = None,
    ) -> None:
        super().__init__(
            config,
            add_spaces,
            status_callback,
            progress_callback,
        )

    def call_status(self, message: str) -> None:
        if self.status_callback is not None:
            self.status_callback(message)

    def extract(
        self,
        data: pd.DataFrame,
        column: str,
        delete_features_columns: bool = False,
        concat_regex: bool = True,
    ) -> pd.DataFrame:
        data[column] = self._add_spaces(data[column])

        data = self.enginge.extract_all(data, column)

        self.call_status("Объединяю регулярные выражения")
        data = (
            self.enginge.concat_regex(data, delete_features_columns)
            if concat_regex
            else data
        )

        data[column] = self._del_spaces(data[column])
        return data


class SizeExtractor(Extractor):
    def __init__(
        self,
        basic_sep: bool = True,
        custom_sep: str = r"[\/\\xх]",
        left_step: float = 10,
        right_step: float = 10,
        triple_from_double: bool = True,
        triple_from_double_pos: int = 0,
    ):
        self.basic_sep = basic_sep
        self.custom_sep = custom_sep
        self._kf1 = self._recount(left_step)
        self._kf2 = self._recount(1 / right_step)
        self.triple_from_double = triple_from_double
        self.triple_from_double_pos = triple_from_double_pos

        self._name = "_extr"

    def _show_status(self):
        print("Извлекаю размеры")

    def _extract_size_values(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        if self.basic_sep:
            sep = r"\D+"
        else:
            sep = self.custom_sep

        _int = r"\d*[.,]?\d+"
        rx_double = rf"({_int})(?:{sep})({_int})"
        rx_triple = rf"({_int})(?:{sep})({_int})(?:{sep})({_int})"

        df[self._name] = df[col].str.findall(rx_triple).str[0]
        df[self._name] = np.where(
            df[self._name].isna(),
            df[col].str.findall(rx_double).str[0],
            df[self._name],
        )

        return df

    def _triple_from_double(self, data: pd.DataFrame) -> pd.DataFrame:
        def get_values(series: pd.Series) -> pd.Series:
            if isinstance(series[self._doub], tuple):
                series["new_triple"] = (
                    series[self._doub][self.triple_from_double_pos],
                    series[self._doub][self.triple_from_double_pos],
                    series[self._doub][abs(self.triple_from_double_pos - 1)],
                )

            return series

        if self._triple_from_double:
            data[self._trip] = data[self._trip].apply(lambda x: x if x else np.nan)
            data = data.apply(get_values, axis=1)
            data[self._trip] = np.where(
                data[self._trip].isna(),
                data["new_triple"],
                data[self._trip],
            )
            data = data.drop("new_triple", axis=1)
        return data

    def _recount(self, val: float) -> float:
        if math.isclose(val, round(val)):
            val = round(val)
        return val

    def _prep_value(self, value: str, kf: float) -> float:
        value = self._recount(float(value) / kf)
        if value >= 1:
            # TODO - хардкод - может привести к ошибкам
            value = re.sub(r"\.0$", "", str(value))
        else:
            value = str(value)
        return value

    def _create_rx(self, values: tuple[int]) -> str:
        rx = ""
        kfs = [self._kf1, 1, self._kf2]

        if isinstance(values, tuple):
            for kf_ind in range(len(kfs)):
                _rx = r"\D"
                kf = kfs[kf_ind]
                for val_ind in range(len(values)):
                    val = self._prep_value(values[val_ind], kf)
                    val_end = r"\D+" if val_ind < len(values) - 1 else r"\D"
                    _rx += rf"{val}{val_end}"

                _sep = "|" if kf_ind < len(kfs) - 1 else ""
                rx += _rx + _sep

        return rx

    def _create_trip_rx(self, values: tuple[int]) -> str:
        rx = ""
        if isinstance(values, tuple):
            kfs = [self._kf1, 1, self._kf2]
            for ind in range(len(kfs)):
                kf = kfs[ind]
                v1 = self._prep_value(values[0], kf)
                v2 = self._prep_value(values[1], kf)
                v3 = self._prep_value(values[2], kf)

                _rx = rf"\D{v1}\D+{v2}\D+{v3}\D"
                _sep = "|" if ind < len(kfs) - 1 else ""
                rx += _rx + _sep
            rx = "(" + rx + ")"

        return rx

    def _create_size_rx(self, data: pd.DataFrame) -> pd.DataFrame:
        data[self._name] = data[self._name].apply(self._create_rx)
        data["Sizes"] = "(?=.*(" + data[self._name] + "))"
        return data

    def _clean_up(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.drop(
            [
                self._name,
            ],
            axis=1,
        )

        data["Sizes"] = np.where(
            data["Sizes"] == "(?=.*(|))",
            "",
            data["Sizes"],
        )

        data["Sizes"] = np.where(
            data["Sizes"] == "(?=.*())",
            "",
            data["Sizes"],
        )

        return data

    def extract(self, data: pd.DataFrame, col: str) -> pd.DataFrame:
        self._show_status()

        data = self._extract_size_values(data, col)
        # data = self._triple_from_double(data) # TODO: rework it
        data = self._create_size_rx(data)  # ["Размеры"]
        data = self._clean_up(data)

        return data
