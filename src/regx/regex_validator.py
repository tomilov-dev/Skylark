import pandas as pd
import re
import numpy as np
import sys
import os
from fuzzywuzzy import fuzz


from modes import *


def upload_data(
    semantic_path: str,
    validation_path: str,
) -> tuple[pd.Series, pd.Series]:
    def upload(path: str) -> pd.Series:
        if re.search(".csv$", path):
            data = pd.read_csv(path)
        else:
            data = pd.read_excel(path)
        return data

    semantic = upload(semantic_path)
    validation = upload(validation_path)
    return semantic, validation


class RegexValidator(object):
    def __init__(
        self,
        semantic: pd.DataFrame,
        validation: pd.DataFrame,
        plus_column: str = "Плюс-слова",
        minus_column: str = "Минус-слова",
        regex_column: str = "Regex",
        validate_by: str = "Строка валидации",
        semantic_merge_by: str = "Название",
        validation_merge_by: str = "Наименование",
    ) -> None:
        self._semantic = semantic
        self._validation = validation
        self._plus_column = plus_column
        self._minus_column = minus_column
        self._regex_column = regex_column
        self._validate_by = validate_by
        self._semantic_merge_by = semantic_merge_by
        self._validation_merge_by = validation_merge_by

    def _prepare_minus(self, data: pd.DataFrame) -> pd.DataFrame:
        data["minus_rx"] = np.where(
            data[self._minus_column].isna(),
            data[self._minus_column],
            data[self._minus_column].str.replace("^\||\|$", "", regex=True),
        )
        return data

    def _validate_by_minus(self, row: pd.Series) -> bool:
        if re.search(row["minus_rx"], row[self._validate_by], flags=re.IGNORECASE):
            return 0
        else:
            return 1

    def validateByMinus(self, data: pd.DataFrame) -> pd.DataFrame:
        data["_minus_valid"] = 1
        data[self._minus_column] = data[self._minus_column].replace("", np.nan)
        not_na = list(data[data[self._minus_column].notna()].index)
        if len(not_na) > 0:
            to_valid = data.iloc[not_na][[self._validate_by, self._minus_column]]

            to_valid = self._prepare_minus(to_valid)
            data.loc[not_na, "_minus_valid"] = to_valid.apply(
                self._validate_by_minus,
                axis=1,
            )

        return data

    def _prepare_plus(self, data: pd.DataFrame) -> pd.DataFrame:
        data[self._plus_column] = data[self._plus_column].fillna("")
        data[self._plus_column] = data[self._plus_column].str.replace(
            "^\||\|$",
            "",
            regex=True,
        )

        data["plus_rx"] = data[self._plus_column].str.replace("|", "))(?=.*(")
        data["plus_rx"] = "(?=.*(" + data["plus_rx"] + "))"
        data["plus_rx"] = data["plus_rx"].replace("(?=.*())", np.nan)
        return data

    def _validate_by_plus(self, row: pd.Series) -> bool:
        if re.search(row["plus_rx"], row[self._validate_by], flags=re.IGNORECASE):
            return 1
        else:
            return 0

    def validateByPlus(self, data: pd.DataFrame) -> pd.DataFrame:
        data["_plus_valid"] = 1
        data[self._plus_column] = data[self._plus_column].replace("", np.nan)
        not_na = list(data[data[self._plus_column].notna()].index)
        if len(not_na) > 0:
            to_valid = data.iloc[not_na][[self._validate_by, self._plus_column]]

            to_valid = self._prepare_plus(to_valid)
            data.loc[not_na, "_plus_valid"] = to_valid.apply(
                self._validate_by_plus,
                axis=1,
            )

        return data

    def _validate_by_regex(self, row: pd.Series) -> bool:
        if re.match(
            row[self._regex_column], row[self._validate_by], flags=re.IGNORECASE
        ):
            return 1
        else:
            return 0

    def validateByRegex(self, data: pd.DataFrame) -> pd.DataFrame:
        data["_regex_valid"] = 1
        data[self._regex_column] = data[self._regex_column].replace("", np.nan)
        not_na = list(data[data[self._regex_column].notna()].index)
        if len(not_na) > 0:
            to_valid = data.iloc[not_na][[self._validate_by, self._regex_column]]
            data.loc[not_na, "_regex_valid"] = to_valid.apply(
                self._validate_by_regex,
                axis=1,
            )

        return data

    def _merge_data(self) -> pd.DataFrame:
        val_data = self._validation.merge(
            self._semantic[
                [
                    self._semantic_merge_by,
                    self._plus_column,
                    self._minus_column,
                    self._regex_column,
                ]
            ],
            how="left",
            right_on=self._semantic_merge_by,
            left_on=self._validation_merge_by,
        )
        val_data.index = range(len(val_data))
        return val_data

    def _drop_merged(self, val_data: pd.DataFrame) -> pd.DataFrame:
        val_data.drop(
            [
                self._plus_column,
                self._minus_column,
                self._regex_column,
            ],
            axis=1,
            inplace=True,
        )
        return val_data

    def _make_desicion(self, val_data: pd.DataFrame) -> pd.DataFrame:
        val_data["validation_mark"] = val_data[
            ["_minus_valid", "_plus_valid", "_regex_valid"]
        ].sum(axis=1)

        val_data["validation_mark"] = np.where(
            val_data["validation_mark"] == 3,
            1,
            0,
        )

        val_data["reason"] = val_data["_minus_valid"].astype(str)
        val_data["reason"] += val_data["_plus_valid"].astype(str)
        val_data["reason"] += val_data["_regex_valid"].astype(str)
        val_data.drop(
            ["_minus_valid", "_plus_valid", "_regex_valid"], axis=1, inplace=True
        )
        return val_data

    def validate(self):
        val_data = self._merge_data()

        val_data = self.validateByMinus(val_data)
        val_data = self.validateByPlus(val_data)
        val_data = self.validateByRegex(val_data)

        val_data = self._make_desicion(val_data)
        val_data = self._drop_merged(val_data)
        return val_data


class RegexValidatorPro(RegexValidator):
    def __init__(
        self,
        semantic: pd.DataFrame,
        validation: pd.DataFrame,
        plus_weight: int,
        minus_weight: int,
        regex_weight: int,
        use_fuzzy: list[FuzzyMode] = [],
        strict: list[StrictMode] = [],
        plus_column: str = "Плюс-слова",
        minus_column: str = "Минус-слова",
        regex_column: str = "Regex",
        validate_by: str = "Строка валидации",
        semantic_merge_by: str = "Название",
        validation_merge_by: str = "Название",
    ) -> None:
        self._semantic = semantic
        self._validation = validation

        self._plus_weight = plus_weight
        self._minus_weight = minus_weight
        self._regex_weight = regex_weight
        self._use_fuzzy = use_fuzzy
        self._strict = strict

        self._mode_check()

        self._plus_column = plus_column
        self._minus_column = minus_column
        self._regex_column = regex_column
        self._validate_by = validate_by
        self._semantic_merge_by = semantic_merge_by
        self._validation_merge_by = validation_merge_by

    def _mode_check(self):
        if (PlusFuzzy in self._use_fuzzy) and (PlusStrict in self._strict):
            raise ValueError("You can't use PlusFuzzy and PlusStrict together")
        if (MinusFuzzy in self._use_fuzzy) and (MinusStrict in self._strict):
            raise ValueError("You can't use MinusFuzzy and MinusStrict together")

    def _parse_rx(self) -> pd.DataFrame:
        self._semantic[self._regex_column] = self._semantic[
            self._regex_column
        ].str.findall(r"\(\?\=\.\*.*?\)\)+")
        return self._semantic

    def _merge_data(self) -> pd.DataFrame:
        self._semantic[self._minus_column] = self._semantic[
            self._minus_column
        ].str.split("|")
        self._semantic[self._plus_column] = self._semantic[self._plus_column].str.split(
            "|"
        )

        self._semantic = self._parse_rx()
        return super()._merge_data()

    def _scorer(
        self,
        pattern: str,
        string: str,
        opposite: bool,
    ) -> bool:
        score = True if re.search(pattern, string, re.IGNORECASE) else False
        if opposite:
            return not score
        return score

    def _fuzzy_scorer(
        self,
        pattern: str,
        string: str,
        opposite: str,
    ) -> float:
        score = fuzz.partial_ratio(pattern, string) / 100
        if opposite:
            return 1 - score
        return score

    def _choose_scorer(self, mode):
        if isinstance(mode(), FuzzyOn):
            return self._fuzzy_scorer
        return self._scorer

    def _validate(
        self,
        row: pd.Series,
        pattern_column: str,
        opposite: bool,
        mode: FuzzyMode,
    ) -> int:
        string = row[self._validate_by]
        scorer = self._choose_scorer(mode)
        score = [scorer(pattern, string, opposite) for pattern in row[pattern_column]]
        return sum(score)

    def validateByMinus(self, data: pd.DataFrame) -> pd.DataFrame:
        mode = FuzzyOn if MinusFuzzy in self._use_fuzzy else FuzzyOff
        data["_minus_valid"] = data.apply(
            self._validate,
            axis=1,
            args=(
                self._minus_column,
                True,
                mode,
            ),
        )
        return data

    def validateByPlus(self, data: pd.DataFrame) -> pd.DataFrame:
        mode = FuzzyOn if PlusFuzzy in self._use_fuzzy else FuzzyOff
        data["_plus_valid"] = data.apply(
            self._validate,
            axis=1,
            args=(
                self._plus_column,
                False,
                mode,
            ),
        )
        return data

    def validateByRegex(self, data: pd.DataFrame) -> pd.DataFrame:
        mode = FuzzyOff
        data["_regex_valid"] = data.apply(
            self._validate,
            axis=1,
            args=(
                self._regex_column,
                False,
                mode,
            ),
        )
        return data

    def _strict_check(
        self,
        data: pd.DataFrame,
        pattern_col: str,
        score_col: str,
    ) -> pd.DataFrame:
        data["validation_mark"] = np.where(
            data[pattern_col].str.len() != data[score_col],
            0,
            data["validation_mark"],
        )
        return data

    def _make_desicion(self, val_data: pd.DataFrame) -> pd.DataFrame:
        val_data["total"] = 0
        val_data["total"] += val_data[self._plus_column].str.len() * self._plus_weight
        val_data["total"] += val_data[self._minus_column].str.len() * self._minus_weight
        val_data["total"] += val_data[self._regex_column].str.len() * self._regex_weight

        val_data[self._plus_column] *= self._plus_weight
        val_data[self._minus_column] *= self._minus_weight
        val_data[self._regex_column] *= self._regex_weight

        val_data["validation_mark"] = (
            val_data[["_minus_valid", "_plus_valid", "_regex_valid"]].sum(axis=1)
            / val_data["total"]
        )

        if PlusStrict in self._strict:
            val_data = self._strict_check(val_data, self._plus_column, "_plus_valid")
        if MinusStrict in self._strict:
            val_data = self._strict_check(val_data, self._minus_column, "_minus_valid")
        if RegexStrict in self._strict:
            val_data = self._strict_check(val_data, self._regex_column, "_regex_valid")

        val_data.drop(
            ["_minus_valid", "_plus_valid", "_regex_valid", "total"],
            axis=1,
            inplace=True,
        )
        return val_data

    def validate(self):
        val_data = self._merge_data()

        val_data = self.validateByMinus(val_data)
        val_data = self.validateByPlus(val_data)
        val_data = self.validateByRegex(val_data)

        val_data = self._make_desicion(val_data)
        val_data = self._drop_merged(val_data)
        return val_data
