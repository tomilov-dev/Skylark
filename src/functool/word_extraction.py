import nltk
import sys
import pandas as pd

from pathlib import Path

SRC_DIR = Path(__file__).parent.parent
PROJECT_DIR = SRC_DIR.parent
sys.path.append(str(PROJECT_DIR))


from src.functool.words_functool import LanguageRules, WordsFuncTool
from src.functool.interfaces import Extractor


def words_join(words: pd.Series, joiner="|") -> pd.Series:
    """Return series of joined by joiner words"""
    words = words.apply(lambda _words: joiner.join(_words))
    return words


def words_stemming(words: pd.Series, language="english") -> pd.Series:
    stemmer = nltk.stem.SnowballStemmer(language)
    words = words.apply(lambda _words: [stemmer.stem(word) for word in _words])
    return words


def words_cleaner(words: pd.Series) -> pd.Series:
    words = words.apply(lambda _words: [w.strip() for w in _words])
    return words


def words_filter(
    words: pd.Series,
    min_length: int = 0,
    max_words: int = 0,
) -> pd.Series:
    """
    min_lenght: min lenght of word (included)
    max_words: maximum count of extracted words (included)
    """

    def _filter(_words: list) -> list:
        output = list(filter(lambda x: len(x) >= min_length, _words))
        output = output if not max_words else output[:max_words]
        return output

    words = words.apply(_filter)
    return words


class WordsExtractor(Extractor):
    def __init__(
        self,
        rules: LanguageRules,
        expand_spaces: bool = False,
        del_founded: bool = False,
    ) -> None:
        self.rules = rules
        self.expand_spaces = expand_spaces
        self.del_founded = del_founded

        self.tool = WordsFuncTool()

    def _preprocess(
        self,
        words: pd.Series,
    ) -> pd.Series:
        words = words_cleaner(words)
        words = words_filter(
            words,
            min_length=self.rules.min_length,
            max_words=self.rules.max_words,
        )

        if self.rules.stemming:
            words = words_stemming(
                words,
                language=self.rules.language_name,
            )

        if self.rules.join_words:
            words = words_join(words, joiner=self.rules.joiner)
        return words

    def _extract(self, data: pd.DataFrame, col: str) -> pd.DataFrame:
        words = self.tool.extract_words(data["_rows"], self.rules)
        if self.del_founded:
            data["_rows"] = self.tool.delete_words(data["_rows"], self.rules)
            data[col] = data["_rows"].str.replace(r"[ ]+", r" ", regex=True)

        words = self._preprocess(words)
        return words

    def extract(
        self,
        data: pd.DataFrame,
        col: str,
        return_mode: str = "dataframe",
    ) -> pd.DataFrame:
        if return_mode not in ["series", "dataframe"]:
            raise ValueError("return_mode should be 'series' or 'dataframe'.")

        data["_rows"] = data[col]
        if self.expand_spaces:
            data["_rows"] = "  " + data["_rows"] + "  "
            data["_rows"] = data["_rows"].str.replace(" ", " " * 3)

        words = self._extract(data, col)
        data.drop("_rows", axis=1, inplace=True)
        if return_mode == "series":
            return words
        elif return_mode == "dataframe":
            data[self.rules.rule_name] = words
            return data


class StraightWordExtractor(Extractor):
    def __init__(
        self,
        rules: list[LanguageRules],
        straight: bool = False,
        main_rule: int = 0,
        expand_spaces: bool = False,
        del_founded: bool = False,
    ) -> None:
        if straight and not isinstance(rules, list):
            rules = [rules]

        self.rules = rules
        self.straight = straight
        self.main_rule = main_rule
        self.expand_spaces = expand_spaces
        self.del_founded = del_founded

        self.tool = WordsFuncTool()

    def _preprocess(
        self,
        words: pd.Series,
        rules: LanguageRules,
    ) -> pd.Series:
        words = words_cleaner(words)
        words = words_filter(
            words,
            min_length=rules.min_length,
            max_words=rules.max_words,
        )

        if rules.stemming:
            words = words_stemming(
                words,
                language=rules.language_name,
            )

        if rules.join_words:
            words = words_join(
                words,
                joiner=rules.joiner,
            )
        return words

    def _straight(self, data: pd.DataFrame, newcol: str) -> pd.DataFrame:
        words = self.tool.extractWordsWithMultipleLangsLetters(
            data["_rows"],
            self.rules,
        )
        data[newcol] = self._preprocess(words, self.rules[self.main_rule])
        return data

    def extract(
        self,
        data: pd.DataFrame,
        col: str,
        newcol: str = "straight",
    ) -> pd.DataFrame:
        data["_rows"] = data[col]
        if self.expand_spaces:
            data["_rows"] = "  " + data["_rows"] + "  "
            data["_rows"] = data["_rows"].str.replace(" ", " " * 3)

        data = self._straight(data, newcol)

        data.drop("_rows", axis=1, inplace=True)
        return data
