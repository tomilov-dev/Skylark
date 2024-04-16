from abc import ABC, abstractmethod
import pandas as pd
from typing import Callable
from collections import Counter
from itertools import chain
from math import sqrt, log10
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_DIR))

from src.simfyzer.tokenization import Token


class AbstactRateCounter(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def count_ratio(self):
        pass


class RateFunction(object):
    @classmethod
    def default(
        self,
        value: int,
        max_value: int,
    ) -> int:
        return value

    @classmethod
    def sqrt2(
        self,
        value: int,
        max_value: int,
    ) -> float:
        if value != 0:
            return sqrt(value)
        return value

    @classmethod
    def sqrt3(
        self,
        value: int,
        max_value: int,
    ) -> float:
        if value != 0:
            return value ** (0.33)
        return value

    @classmethod
    def sqrt4(
        self,
        value: int,
        max_value: int,
    ) -> float:
        if value != 0:
            return value ** (0.25)
        return value

    @classmethod
    def log(self, value: int, max_value: int) -> float:
        if value != 0:
            return log10(value)
        return value

    @classmethod
    def _reverse(self, value) -> float:
        try:
            if value != 0:
                value = 1 / value
            else:
                value = 0
        except Exception as ex:
            value = 0

        return value

    @classmethod
    def parabaloid(
        self,
        value: int,
        max_value: int,
    ) -> float:
        def parab_func(value) -> float:
            value = -4 * value**2 + 4 * value
            return value

        if max_value != 0:
            value = value / max_value
            value = parab_func(value)
            value = self._reverse(value)
        else:
            value = value

        return value

    @classmethod
    def map(self, func_name: str) -> callable:
        mapper = {
            "default": self.default,
            "sqrt2": self.sqrt2,
            "sqrt3": self.sqrt3,
            "sqrt4": self.sqrt4,
            "log": self.log,
            "parabaloid": self.parabaloid,
        }
        func = mapper.get(func_name, self.default)
        return func


class RateCounter(AbstactRateCounter):
    def __init__(
        self,
        min_ratio: float = 0,
        max_ratio: float = 1,
        min_appearance: int = 1,
        min_appearance_penalty: float = 0,
        rate_function: Callable = RateFunction.default,
    ) -> None:
        self.min_ratio = min_ratio
        self.max_ratio = max_ratio
        self.min_appearance = min_appearance
        self.min_appearance_penalty = min_appearance_penalty
        self.rate_function = rate_function

    def _get_tokens(
        self,
        data: pd.DataFrame,
        left_tokens: str,
        right_tokens: str,
    ) -> list[Token]:
        tokens: list[list[Token]] = (
            data[left_tokens].to_list() + data[right_tokens].to_list()
        )
        tokens: list[Token] = list(chain(*tokens))

        ## in case if passed sets of tokens
        # tokens: list[Token] = list(chain(*map(list, tokens)))
        return tokens

    def _rate_function(self, value: int, max_value: int) -> float:
        try:
            if self.rate_function is not None:
                if callable(self.rate_function):
                    value = self.rate_function(value, max_value)

            if value != 0:
                value_rate = 1 / value
            else:
                value_rate = 0

        except Exception as ex:
            print(ex)

        finally:
            return value_rate

    def _appearance_penalty(self, value: int) -> float:
        if self.min_appearance:
            if value <= self.min_appearance:
                return self.min_appearance_penalty
        return 1

    def _min_max(self, value_rate: float) -> float:
        value_rate = self.min_ratio if value_rate < self.min_ratio else value_rate
        value_rate = self.max_ratio if value_rate > self.max_ratio else value_rate
        return value_rate

    def _count_ratio(self, value: int, max_value: int) -> float:
        value_rate = self._rate_function(value, max_value)
        value_rate = self._min_max(value_rate)
        value_rate *= self._appearance_penalty(value)

        return value_rate

    def _process_ratio(self, tokens: list[Token]):
        ratio = {}
        tokens_values = [token.value for token in tokens]
        counts = Counter(tokens_values)
        max_value = counts.most_common()[0][1]

        for key, value in counts.items():
            rate = self._count_ratio(value, max_value)
            ratio[key] = rate

        return ratio

    def count_ratio(
        self,
        data: pd.DataFrame,
        left_tokens: str,
        right_tokens: str,
    ) -> dict:
        tokens = self._get_tokens(data, left_tokens, right_tokens)
        ratio = self._process_ratio(tokens)
        return ratio


class AbstractMarksCounter(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def count_marks(self):
        pass


class MarksMode(object):
    UNION = "marks_union"
    CLIENT = "marks_client"
    SOURCE = "marks_source"
    MULTIPLE = "multiple"


class MarksCounter(AbstractMarksCounter):
    def __init__(
        self,
        mode: MarksMode,
    ) -> None:
        self.mode = mode

    @property
    def validation_column(self):
        if self.mode is MarksMode.MULTIPLE:
            return MarksMode.UNION
        return self.mode

    def _find_ratio(self, tokens: set[Token]):
        tokens_rates = [
            self.ratio[token.value] * token.custom_weight for token in tokens
        ]
        return tokens_rates

    def _try_count_mark(
        self,
        intersect_rates: list[float],
        base_rates: list[float],
    ) -> float:
        try:
            mark = sum(intersect_rates) / sum(base_rates)

        except Exception as ex:
            print(ex)
            mark = 0

        finally:
            return mark

    def _count_mark(
        self,
        row: pd.Series,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> float:
        left_tokens: set[Token] = row[left_tokens_column]
        right_tokens: set[Token] = row[right_tokens_column]
        intersect = left_tokens.intersection(right_tokens)

        if self.mode is MarksMode.UNION:
            base = left_tokens.union(right_tokens)
        elif self.mode == MarksMode.CLIENT:
            base = left_tokens
        elif self.mode == MarksMode.SOURCE:
            base = right_tokens
        else:
            raise NotImplementedError("Not implemented Marks Mode")

        intersect_rates = self._find_ratio(intersect)
        base_rates = self._find_ratio(base)

        mark = self._try_count_mark(intersect_rates, base_rates)
        return mark

    def _count_multiple_marks(
        self,
        row: pd.Series,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> list[float]:
        left_tokens: set[Token] = row[left_tokens_column]
        right_tokens: set[Token] = row[right_tokens_column]
        intersect = left_tokens.intersection(right_tokens)

        bases = [
            left_tokens.union(right_tokens),
            left_tokens,
            right_tokens,
        ]

        intersect_rates = self._find_ratio(intersect)
        marks = [
            self._try_count_mark(intersect_rates, self._find_ratio(base))
            for base in bases
        ]
        return marks

    def count_marks(
        self,
        ratio: dict,
        data: pd.DataFrame,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> pd.Series:
        self.ratio = ratio

        if self.mode is MarksMode.MULTIPLE:
            marks = data.apply(
                self._count_multiple_marks,
                axis=1,
                args=(left_tokens_column, right_tokens_column),
            )

            data[MarksMode.UNION] = marks.str[0]
            data[MarksMode.CLIENT] = marks.str[1]
            data[MarksMode.SOURCE] = marks.str[2]

        else:
            data[self.mode] = data.apply(
                self._count_mark,
                axis=1,
                args=(left_tokens_column, right_tokens_column),
            )

        return data
