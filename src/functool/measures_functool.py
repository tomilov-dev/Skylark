import sys
import json
import numpy as np
import regex as re
import pandas as pd

from pathlib import Path
from typing import Callable
from decimal import Decimal
from abc import abstractmethod
from typing import Tuple, List

SRC_DIR = Path(__file__).parent.parent
PROJECT_DIR = SRC_DIR.parent
sys.path.append(str(PROJECT_DIR))

from src.notation import SEMANTIC
from config.measures_config.config_parser import (
    CONFIG,
    MEASURE,
    DATA,
    UNIT,
    AUTOSEM_CONF,
)

EXCLUDE_RX_NAME_PREFIX = "Исключ. "
EXCLUDE_RX_PATTER = r"(?!.*("
EXCLUDE_RX_PATTER_CARET = r"^(?!.*("


class MeasuresGracefullExit(Exception):
    pass


class SearchMode(object):
    """Measure Search Mode
    Using for determing position of search value (\d+)

    Mode can be:
        - front : in front of Unit Symbol
        - behind : behind Unit Symbol

    Default mode:
        - behind
    """

    FRONT = "front"
    BEHIND = "behind"
    modes = {FRONT, BEHIND}

    default = BEHIND

    @classmethod
    def checkout(cls, mode: str) -> str:
        """
        Check search mode and return standardized value\n
        If the input mode have wrond value -> return default mode
        """

        mode = str(mode).lower()
        if mode not in cls.modes:
            mode = cls.default
        return mode


class MergeMode(object):
    """Measure Merge Mode
    Using for merging units to one regex

    Mode can be:
        - none : no merging
        - overall : merge all units
        - num : '1' merging one left and one right unit (ug <- mg -> g)

    Default mode:
        - overall

    """

    NONE = "none"
    OVERALL = "overall"
    modes = {OVERALL, NONE}

    default = "overall"

    @classmethod
    def checkout(cls, mode: str) -> str:
        mode = str(mode).lower()
        if mode not in cls.modes:
            if not mode.isnumeric():
                mode = cls.default
        return mode


class CommonValues(object):
    """
    Determine if unit config value should
    be determined as common measure value
    It can be common Prefix or Postfix
    """

    COMMON = "common"

    def is_common(self, value: int | str) -> bool:
        if value == self.COMMON:
            return True
        return False


class AbstractUnit(object):
    name: str
    symbol: str
    relative_weight: str
    search_mode: str
    prefix: str
    postfix: str


class Unit(AbstractUnit):
    """Abstract Measure Unit"""

    def __init__(
        self,
        unit_data: dict,
        common_prefix: str,
        common_postfix: str,
        common_max_count: str,
        special_value_search: str,
    ) -> None:
        UD = unit_data
        CV = CommonValues()

        self.name = UD[UNIT.NAME]
        self.symbol = UD[UNIT.SYMBOL]
        self.relative_weight = Decimal(str(UD[UNIT.RWEIGHT]))

        self.search_mode = (
            SearchMode.checkout(UD[UNIT.SEARCH_MODE])
            if UNIT.SEARCH_MODE in UD
            else SearchMode.default
        )

        self.prefix = (
            UD[UNIT.PREFIX] if not CV.is_common(UD[UNIT.PREFIX]) else common_prefix
        )

        self.postfix = (
            UD[UNIT.POSTFIX] if not CV.is_common(UD[UNIT.POSTFIX]) else common_postfix
        )

        self.max_count = (
            UD[UNIT.MAX_COUNT]
            if not CV.is_common(UD[UNIT.MAX_COUNT])
            else common_max_count
        )

        self._search_rx = self._make_search_rx(special_value_search)
        self.allocated_units = [self]

    def get_search_regex(self) -> str:
        return self._search_rx

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, self.__class__):
            if self.name == __value.name:
                return True
            else:
                return False
        else:
            return False

    @abstractmethod
    def _default_search(self):
        pass

    def _make_search_rx(self, special_value_search: str) -> str:
        DVS = self._default_search()  # default value search
        value_srch = DVS

        if special_value_search:
            value_srch = special_value_search

        if self.search_mode == SearchMode.BEHIND:
            rx = rf"{self.prefix}{value_srch}\s*(?:{self.symbol}){self.postfix}"
        else:
            rx = rf"{self.prefix}(?:{self.symbol})\s*{value_srch}{self.postfix}"

        return rx

    def _extract_values(self, string: pd.Series) -> list[str]:
        return re.findall(self._search_rx, string, re.IGNORECASE)

    def extract(
        self,
        extract_from: list[str],
    ) -> list[list[str]]:
        values = list(map(self._extract_values, extract_from))
        return values

    def filter_count(
        self,
        extracted_values: list[list[str]],
    ) -> list[list[str]]:
        if self.max_count != None:
            extracted_values = list(
                map(
                    lambda x: x[: self.max_count],
                    extracted_values,
                )
            )

        return extracted_values

    def add_relative(self, units: list[AbstractUnit]) -> None:
        self.allocated_units.extend(units)

    @abstractmethod
    def transform(
        self,
        extracted_values: list[list[str]],
    ) -> list[list[str]]:
        pass

    def __repr__(self) -> str:
        _return = []
        _return.append(f"name: {self.name}")
        _return.append(f"symbol: {self.symbol}")
        _return.append(f"relative_weight: {self.relative_weight}")
        _return.append(f"prefix: '{self.prefix}'")
        _return.append(f"postfix: '{self.postfix}'\n")
        return "\n".join(_return)


class StringUnit(Unit):
    def _default_search(self) -> str:
        return ""

    def _to_regex(self, values: list[str]) -> list[str]:
        if values != []:
            values = [self._search_rx]
        return values

    def transform(
        self,
        extracted_values: list[list[str]],
    ) -> list[list[str]]:
        regex_values = map(self._to_regex, extracted_values)
        regex_values = map(lambda x: "".join(x), regex_values)
        regex_values = map(lambda x: "(?=.*(" + x + "))", regex_values)
        regex_values = map(lambda x: x.replace("(?=.*())", ""), regex_values)

        return list(regex_values)


class NumericUnit(Unit):
    def _default_search(self) -> str:
        return r"\d*[.,]?\d+"

    def _extract_numeric_values(self, values: list[str]) -> list[str]:
        numeric_values = []
        for value in values:
            searched = re.search(r"\d*[.,]?\d+", value, re.IGNORECASE)
            if searched:
                numeric_values.append(Decimal(searched[0]))

        return numeric_values

    def _prepare_num(self, num: str) -> str:
        if num and not isinstance(num, list):
            num = f"{num:.20f}"
            if isinstance(num, str):
                if "." in num:
                    num = re.sub(r"0+$", "", num)
                    num = re.sub(r"\.$", "", num)
                if "." in num:
                    num = re.sub(r"[.]", r"[.,]", num)
        return num

    def _to_regex(self, numeric_values: list[str]) -> list[str]:
        regexes = []
        units: list[AbstractUnit] = self.allocated_units

        for numeric_value in numeric_values:
            rx_parts = []
            for unit in units:
                num: Decimal = numeric_value * (
                    self.relative_weight / unit.relative_weight
                )
                num = self._prepare_num(num)

                if unit.search_mode == SearchMode.BEHIND:
                    rx_part = (
                        unit.prefix
                        + num
                        + r"\s*"
                        + r"(?:"
                        + unit.symbol
                        + r")"
                        + unit.postfix
                    )

                else:
                    rx_part = (
                        unit.prefix
                        + r"(?:"
                        + unit.symbol
                        + ")"
                        + r"\s*"
                        + num
                        + unit.postfix
                    )

                rx_parts.append(rx_part)

            regexes.append("|".join(rx_parts))

        return regexes

    def transform(
        self,
        extracted_values: list[list[str]],
    ) -> list[list[str]]:
        numeric_values = map(
            self._extract_numeric_values,
            extracted_values,
        )

        regex_values = map(self._to_regex, numeric_values)
        regex_values = map(lambda x: "))(?=.*(".join(x), regex_values)
        regex_values = map(lambda x: "(?=.*(" + x + "))", regex_values)
        regex_values = map(lambda x: x.replace("(?=.*())", ""), regex_values)

        return list(regex_values)


class UnitType(object):
    NUMERIC = "numeric_unit"
    STRING = "string_unit"

    __mapper = {
        CONFIG.NUMERIC_MEASURES: NUMERIC,
        CONFIG.STRING_MEASURES: STRING,
    }

    __type = {
        NUMERIC: NumericUnit,
        STRING: StringUnit,
    }

    def __init__(self, measure_type: str) -> None:
        self._type = self.__mapper[measure_type]

    def type(self) -> Unit:
        return self.__type[self._type]


class Measure(object):
    def __init__(
        self,
        measure_name: str,
        merge_mode: str,
        measure_data: dict,
        measure_type: str,
        exclude_rx: bool = False,
    ) -> None:
        self.name = measure_name
        self.merge_mode = merge_mode
        self.measure_type = measure_type
        self.exclude_rx = exclude_rx

        self.units = self._create_units(measure_data)
        self._sort_units()
        self._allocate_relative_units()

    def __iter__(self):
        self.__i = 0
        return self

    def __next__(self) -> Unit:
        if self.__i >= len(self.units):
            raise StopIteration
        else:
            item = self.units[self.__i]
            self.__i += 1
            return item

    def __len__(self) -> int:
        return len(self.units)

    def __getitem__(self, index: int) -> Unit:
        if index >= len(self.units):
            raise IndexError()
        else:
            return self.units[index]

    def __repr__(self) -> str:
        return self.name

    def _create_units(self, measure_data: dict) -> list[Unit]:
        common_prefix = measure_data[DATA.COMMON_PREFIX]
        common_postfix = measure_data[DATA.COMMON_POSTFIX]
        common_max_count = measure_data[DATA.COMMON_MAX_COUNT]
        special_value_search = measure_data[DATA.SPECIAL_VALUE_SEARCH]

        unit_type = UnitType(self.measure_type)

        units = []
        for unit_data in measure_data[DATA.UNITS]:
            if unit_data[UNIT.USE_IT]:
                type = unit_type.type()

                units.append(
                    type(
                        unit_data,
                        common_prefix,
                        common_postfix,
                        common_max_count,
                        special_value_search,
                    )
                )

        return units

    def _sort_units(self) -> None:
        self.units.sort(key=lambda unit: unit.relative_weight)

    def _allocate_relative_units(self) -> None:
        if self.merge_mode == MergeMode.NONE:
            pass

        elif self.merge_mode == MergeMode.OVERALL:
            for unit in self.units:
                other_units = [u for u in self.units if u != unit]
                unit.add_relative(other_units)

        else:
            shift = int(self.merge_mode)
            for index in range(len(self.units)):
                unit = self.units[index]
                maxlen = len(self.units)

                left = self.units[max(0, index - shift) : index]
                right = self.units[
                    min(index + 1, maxlen) : min(index + shift + 1, maxlen)
                ]

                other_units = left + right
                other_units = [u for u in other_units if u != unit]

                unit.add_relative(other_units)

    def _make_exclude_rx(self) -> pd.DataFrame:
        behind = ""
        front = ""

        for unit in self.units:
            search_mode = unit.search_mode
            if search_mode == SearchMode.BEHIND:
                behind += unit.symbol
            else:
                front += unit.symbol

        rx = EXCLUDE_RX_PATTER_CARET
        if behind:
            rx += r"(?:[0-9][0-9]\d*|[2-9]\d*?)\s*" + "(?:" + behind + ")"

        if front:
            if behind:
                rx += "|"
            rx += r"(?:" + front + ")" + r"\s*(?:[0-9][0-9]\d*|[2-9]\d*)"

        rx += r"))"
        return rx

    def _add_exclude_rx(
        self,
        data: pd.DataFrame,
        units_names: list[str],
        new_unit_name: str,
    ) -> pd.DataFrame:
        exclude_rx = self._make_exclude_rx()

        data.loc[:, new_unit_name] = np.where(
            data[units_names].apply(lambda r: r.str.strip().eq("").all(), axis=1),
            exclude_rx,
            "",
        )

        return data

    def extract(
        self,
        data: pd.DataFrame,
        column: str,
    ) -> Tuple[pd.DataFrame, List[str]]:
        units_names = []
        extract_from = data[column].to_list()

        for unit in self.units:
            extracted_values = unit.extract(extract_from)
            extracted_values = unit.filter_count(extracted_values)
            rx_patterns = unit.transform(extracted_values)

            data.loc[:, unit.name] = rx_patterns
            units_names.append(unit.name)

        if self.exclude_rx:
            new_unit_name = EXCLUDE_RX_NAME_PREFIX + self.name
            data = self._add_exclude_rx(data, units_names, new_unit_name)
            units_names.append(new_unit_name)

        return data, units_names


class Measures(object):
    """
    Measures container class for manage and control regex creation

    Parameters
    ----------
    config : dict
        Parsed config file for usage

    """

    def __init__(
        self,
        config: dict,
        status_callback: Callable = None,
        progress_callback: Callable = None,
    ) -> None:
        self.measures = self._create_measures(config)
        self.measures_names = list(self.measures.keys())

        self.status_callback = status_callback
        self.progress_callback = progress_callback

        self.used_units_names = []

        self._stopped = False

    def __iter__(self):
        self.__iterbale = list(self.measures.keys())
        self.__i = 0
        return self

    def __next__(self) -> Measure:
        if self.__i >= len(self.measures):
            raise StopIteration
        else:
            key = self.__iterbale[self.__i]
            item = self.measures[key]
            self.__i += 1
            return item

    def __len__(self) -> int:
        return len(self.measures)

    def __getitem__(self, subscript: int | str) -> Measure:
        if isinstance(subscript, int):
            if subscript >= len(self.measures):
                raise IndexError
            else:
                key = list(self.measures.keys())[subscript]
                return self.measures[key]

        else:
            if subscript in self.measures:
                return self.measures[subscript]
            else:
                raise KeyError

    def _create_measures(self, config: dict) -> dict[str, Measure]:
        """
        Parse config and create dict with
        Measures objects accorging to parsed rules
        """

        measures_list = {}
        for MEASURE_TYPE in CONFIG.MEASURE_TYPES:
            if config[MEASURE_TYPE][CONFIG.USE_IT]:
                data = config[MEASURE_TYPE]

                for measure_record in data[CONFIG.MEASURES]:
                    if MEASURE.NAME in measure_record:
                        autosem_conf = measure_record[MEASURE.AUTOSEM]

                        if autosem_conf[AUTOSEM_CONF.USE_IT]:
                            measures_list[measure_record[MEASURE.NAME]] = Measure(
                                measure_record[MEASURE.NAME],
                                autosem_conf[AUTOSEM_CONF.MERGE_MODE],
                                measure_record[MEASURE.DATA],
                                MEASURE_TYPE,
                                autosem_conf[AUTOSEM_CONF.EXCLUDE_RX],
                            )

        return measures_list

    def stop_callback(self) -> None:
        self._stopped = True

    def _status(self, measure_name: str) -> str:
        return f"Извлекаю {measure_name}"

    def call_status(self, message: str) -> None:
        if self.status_callback is not None:
            self.status_callback(message)

    def call_progress(self, count: int, total: int) -> None:
        if self.progress_callback is not None:
            if total > 0:
                progress = int(count / total * 100)
                self.progress_callback(progress)

    def extract_measure(
        self,
        data: pd.DataFrame,
        column: str,
        measure_name: str,
    ) -> pd.Series:
        """Extract regex by measure name"""

        measure = self.measures[measure_name]
        data, units_names = measure.extract(data, column)

        extracted = data[units_names[0]]
        if len(units_names) >= 2:
            for unit_name in units_names[1:]:
                extracted += data[unit_name]

        return extracted

    def extract_all(
        self,
        data: pd.DataFrame,
        column: str,
    ) -> pd.DataFrame:
        """Extract regex for all measures"""

        count = 0
        total = len(self.measures_names)

        self.call_status("Начинаю извлечение величин")
        self.call_progress(count, total)
        for measure_name in self.measures_names:
            if self._stopped:
                raise MeasuresGracefullExit("Measures extraction was stopped")

            self.call_status(self._status(measure_name))

            measure = self.measures[measure_name]
            data, units_names = measure.extract(data, column)

            self.used_units_names.extend(units_names)

            count += 1
            self.call_progress(count, total)

        self.call_status("Закончил извлечение")
        return data

    def _concat_exlcude_rx(
        self,
        data: pd.DataFrame,
        used_units_names: list[str],
    ) -> pd.DataFrame:
        def is_empty(row: str) -> bool:
            if row == "":
                return False
            return True

        exclude = []
        for unit_name in used_units_names:
            if EXCLUDE_RX_NAME_PREFIX in unit_name:
                exclude.append(unit_name)

        for unit_name in exclude:
            used_units_names.remove(unit_name)

        data["__temp"] = ""
        for unit_name in exclude:
            data["__temp"] += data[unit_name]

        data.loc[data["__temp"].apply(is_empty), SEMANTIC.REGEX] += "^"

        for unit_name in exclude:
            data[SEMANTIC.REGEX] += data[unit_name].str.replace(
                EXCLUDE_RX_PATTER_CARET,
                EXCLUDE_RX_PATTER,
                n=1,
                regex=False,
            )

        data = data.drop(["__temp"], axis=1)
        return data, used_units_names

    def concat_regex(
        self,
        data: pd.DataFrame,
        delete_units_columns: bool = False,
    ) -> pd.DataFrame:
        data[SEMANTIC.REGEX] = ""
        used_units_names = self.used_units_names

        data, used_units_names = self._concat_exlcude_rx(data, used_units_names)

        for unit_name in self.used_units_names:
            data[SEMANTIC.REGEX] += data[unit_name]

        if delete_units_columns:
            data = data.drop(self.used_units_names, axis=1)

        return data


def read_config(path: str):
    with open(path, "rb") as file:
        data = json.loads(file.read())
    return data
