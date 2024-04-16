from abc import ABC, abstractmethod
import pandas as pd
from nltk.tokenize import word_tokenize
from collections import namedtuple
from typing import Union
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_DIR))

from src.functool.words_functool import LanguageRules, LanguageType
from src.functool.word_extraction import WordsExtractor

WeightsRules = namedtuple("WeightRule", ["rules", "weight"])


class AbstractToken(ABC):
    def __init__(
        self,
        value: str,
        custom_weight: float,
    ) -> None:
        self.original_value = str(value)
        self.value = str(value).lower()
        self.custom_weight = custom_weight


class AbstractTokenizer(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def tokenize(self):
        pass


class Token(AbstractToken):
    """
    Token object class.
    It's a simple word with some weight.

    - value - string value (word)
    - custom_weight - custom weight of this word (don't use manual)
    """

    def __init__(
        self,
        value: str,
        custom_weight: float = -1,
    ) -> None:
        self.original_value = str(value)
        self.value = self.original_value.lower()
        self._custom_weight = custom_weight

    @property
    def custom_weight(self):
        return abs(self._custom_weight)

    def change_custom_weight(self, weight: float):
        self._custom_weight = weight

    def __eq__(self, other: Union[AbstractToken, str]) -> bool:
        if isinstance(other, AbstractToken):
            return self.value == other.value
        elif isinstance(other, str):
            return self.value == other
        else:
            raise NotImplementedError(
                "Token can be compared only with other token or string value"
            )

    def __hash__(self) -> int:
        return hash(self.value)

    def __str__(self) -> str:
        return f"<Original: {self.original_value}, Value: {self.value}, Weight: {self.custom_weight}>"

    def __repr__(self) -> str:
        return f"<Original: {self.original_value}, Value: {self.value}, Weight: {self.custom_weight}>"


class TokenTransformer(object):
    def __init__(self):
        pass

    def _get_common_weight(self, t1: Token, t2: Token) -> int:
        return max(t1.custom_weight, t2.custom_weight)

    def transform(
        self,
        main_token: Token,
        dependent_token: Token,
        change_value: bool = True,
    ) -> None:
        weight = self._get_common_weight(main_token, dependent_token)

        main_token.change_custom_weight(weight)
        dependent_token.change_custom_weight(weight)

        if change_value:
            dependent_token.value = main_token.value


class BasicTokenizer(AbstractTokenizer):
    """This class perform token's extraction by default NLTK tokenizer"""

    def _create_tokens(self, words: list[str]) -> list[Token]:
        tokens = [
            Token(
                value=word,
                custom_weight=1,
            )
            for word in words
        ]
        return tokens

    def tokenize(
        self,
        data: pd.DataFrame,
        column: str,
        token_column_name: str,
    ) -> pd.DataFrame:
        """Return the dataframe with extra column <token_col_name>"""

        data[token_column_name] = data[column].apply(word_tokenize)
        data[token_column_name] = data[token_column_name].apply(self._create_tokens)
        return data


class RegexCustomWeights(object):
    """
    This class contains config and weights for LanguageRules
    for future token's extraction

    - caps - weight for caps tokens
    - capital - weight for capital tokens
    - low - weight for low tokens
    - other - weight for other tokens
    - symbols - extra symbols for LanguageRules
    (for all types of extraction)
    - word_boundary - word boundary for LanguageRules
    (for all types of extraction)
    - custom_boundary - custom boundary for LanguageRules
    (for all types of extraction)
    """

    def __init__(
        self,
        caps: int,
        capital: int,
        low: int,
        other: int,
        symbols: str = "",
        word_boundary: bool = True,
        custom_boundary: str = "",
    ) -> None:
        self.caps = caps
        self.capital = capital
        self.low = low
        self.other = other

        self.symbols = symbols
        self.word_boundary = word_boundary
        self.custom_boundary = custom_boundary

    def get_rules(self) -> dict:
        """
        Don't change the order of the rules dictionary: caps -> capital -> low -> other.
        It should be ordered because words extracting recursively with deletion.
        The order and deletion help extract capital-words without caps-words,
        low-words without capital-words and so on.
        """
        rules = {
            "caps": WeightsRules(
                {
                    "rule_name": "caps",
                    "join_words": False,
                    "onlyUpper": True,
                    "with_numbers": True,
                    "check_letters": True,
                    "symbols": self.symbols,
                    "word_boundary": self.word_boundary,
                    "custom_boundary": self.custom_boundary,
                },
                self.caps,
            ),
            "capital": WeightsRules(
                {
                    "rule_name": "capital",
                    "join_words": False,
                    "startUpper": True,
                    "with_numbers": True,
                    "check_letters": True,
                    "symbols": self.symbols,
                    "word_boundary": self.word_boundary,
                    "custom_boundary": self.custom_boundary,
                },
                self.capital,
            ),
            "low": WeightsRules(
                {
                    "rule_name": "low",
                    "join_words": False,
                    "onlyUpper": False,
                    "with_numbers": True,
                    "check_letters": True,
                    "symbols": self.symbols,
                    "word_boundary": self.word_boundary,
                    "custom_boundary": self.custom_boundary,
                },
                self.low,
            ),
            "other": WeightsRules(
                {
                    "rule_name": "other",
                    "join_words": False,
                    "with_numbers": True,
                    "symbols": self.symbols,
                    "word_boundary": self.word_boundary,
                    "custom_boundary": self.custom_boundary,
                },
                self.other,
            ),
        }
        return rules


class RegexTokenizer(BasicTokenizer):
    def __init__(
        self,
        languages: dict[LanguageType, int],
        weights_rules: RegexCustomWeights,
    ) -> None:
        self.languages = languages
        self.weights_rules = weights_rules.get_rules()

    def create_tokens(
        self,
        data: pd.DataFrame,
        token_col_name: str,
        weights_rules: WeightsRules,
        lang_weight: int,
    ) -> pd.DataFrame:
        rows: pd.Series = data[weights_rules.rules["rule_name"]]
        tokens = rows.apply(
            lambda _words: [
                Token(
                    value=word,
                    custom_weight=weights_rules.weight * lang_weight,
                )
                for word in _words
            ]
        )

        data[token_col_name] = data[token_col_name] + tokens
        return data

    def tokenize(
        self,
        data: pd.DataFrame,
        col: str,
        token_column_name: str,
    ) -> pd.DataFrame:
        data[token_column_name] = [[] for _ in data.index]

        for language in self.languages:
            language_weight = self.languages[language]
            for rule_name in self.weights_rules.keys():
                weights_rule = self.weights_rules[rule_name].rules

                extractor = WordsExtractor(
                    LanguageRules(
                        language,
                        **weights_rule,
                    ),
                    expand_spaces=True,
                    del_founded=True,
                )

                data = extractor.extract(data, col)
                data = self.create_tokens(
                    data,
                    token_column_name,
                    self.weights_rules[rule_name],
                    language_weight,
                )
                data = data.drop(weights_rule["rule_name"], axis=1)

        return data
