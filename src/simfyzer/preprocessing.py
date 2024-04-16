import pandas as pd
from abc import ABC


class AbstractPreprocessor(ABC):
    def __init__(self) -> None:
        pass

    def preprocess(self, series: pd.Series) -> pd.Series:
        pass


class Preprocessor(AbstractPreprocessor):
    def __init__(
        self,
        word_min_length: int = 0,
    ) -> None:
        self.word_min_length = word_min_length

    def _filter(self, series: pd.Series) -> pd.Series:
        return series.apply(
            lambda tokens: list(
                filter(
                    lambda token: len(token.value) >= self.word_min_length,
                    tokens,
                )
            )
        )

    def _drop_dups(self, series: pd.Series) -> pd.Series:
        return series.apply(lambda tokens: list(set(tokens)))

    def preprocess(self, series: pd.Series) -> pd.Series:
        if self.word_min_length:
            series = self._filter(series)
        series = self._drop_dups(series)

        return series
