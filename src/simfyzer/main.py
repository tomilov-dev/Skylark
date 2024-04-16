import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Callable
import multiprocessing


SRC_DIR = Path(__file__).parent.parent
PROJECT_DIR = SRC_DIR.parent

sys.path.append(str(PROJECT_DIR))

from src.notation import JAKKAR, DATA
from src.simfyzer.preprocessing import Preprocessor
from src.simfyzer.fuzzy_search import FuzzySearch, FyzzySearchGracefullExit
from src.simfyzer.ratio import RateCounter, MarksCounter, MarksMode, RateFunction
from src.simfyzer.tokenization import (
    BasicTokenizer,
    TokenTransformer,
    RegexTokenizer,
    RegexCustomWeights,
    LanguageType,
)
from config.simfyzer_config.config_parser import (
    CONFIG,
    REGEX_WEIGHTS,
    LANGUAGE_WEIGHTS,
    RATIO,
)


class SimFyzerGracefullExit(Exception):
    pass


class SimFyzer(object):
    def __init__(
        self,
        tokenizer: BasicTokenizer,
        preprocessor: Preprocessor,
        fuzzy: FuzzySearch,
        rate_counter: RateCounter,
        marks_counter: MarksCounter,
        debug: bool = False,
        validation_treshold: float = 0.5,
        status_callback: Callable = None,
        progress_callback: Callable = None,
    ) -> None:
        if validation_treshold < 0 or validation_treshold > 1:
            raise ValueError("Validation treshold should be in range 0 - 1")

        self.tokenizer = tokenizer
        self.preproc = preprocessor
        self.fuzzy = fuzzy
        self.rate_counter = rate_counter
        self.marks_counter = marks_counter
        self.debug = debug
        self.validation_treshold = validation_treshold

        self.status_callback = status_callback
        self.progress_callback = progress_callback

        self.symbols_to_del = r"'\"/"

        self._process_pool = None
        self._stopped = False

    def _delete_symbols(self, series: pd.Series):
        symbols_to_del = "|".join(list(self.symbols_to_del))
        series = series.str.replace(symbols_to_del, "", regex=True)
        return series

    def _create_working_rows(
        self,
        data: pd.DataFrame,
        client_column: str,
        source_column: str,
    ) -> pd.DataFrame:
        if self._stopped:
            raise SimFyzerGracefullExit

        data[JAKKAR.CLIENT] = self._delete_symbols(data[client_column])
        data[JAKKAR.SOURCE] = self._delete_symbols(data[source_column])
        return data

    def _delete_working_rows(self, data: pd.DataFrame) -> pd.DataFrame:
        print("end validation")
        data.drop(
            [
                JAKKAR.CLIENT,
                JAKKAR.SOURCE,
                JAKKAR.CLIENT_TOKENS_COUNT,
                JAKKAR.SOURCE_TOKENS_COUNT,
            ],
            axis=1,
            inplace=True,
            errors="ignore",
        )

        if not self.debug:
            data.drop(
                [JAKKAR.CLIENT_TOKENS, JAKKAR.SOURCE_TOKENS],
                axis=1,
                inplace=True,
            )
        return data

    def _save_ratio(self) -> None:
        pd.Series(data=self.ratio).to_excel(JAKKAR.RATIO_PATH)

    def _process_tokenization(self, data: pd.DataFrame) -> pd.DataFrame:
        if self._stopped:
            raise SimFyzerGracefullExit

        print("client_tokens")
        data = self.tokenizer.tokenize(data, JAKKAR.CLIENT, JAKKAR.CLIENT_TOKENS)

        print("source_tokens")
        data = self.tokenizer.tokenize(data, JAKKAR.SOURCE, JAKKAR.SOURCE_TOKENS)

        return data

    def _make_tokens_set(self, data: pd.DataFrame) -> pd.DataFrame:
        data[JAKKAR.CLIENT_TOKENS] = data[JAKKAR.CLIENT_TOKENS].apply(set)
        data[JAKKAR.SOURCE_TOKENS] = data[JAKKAR.SOURCE_TOKENS].apply(set)
        return data

    def _process_preprocessing(self, validation: pd.DataFrame) -> pd.DataFrame:
        if self._stopped:
            raise SimFyzerGracefullExit

        validation[JAKKAR.CLIENT_TOKENS] = self.preproc.preprocess(
            validation[JAKKAR.CLIENT_TOKENS]
        )
        validation[JAKKAR.SOURCE_TOKENS] = self.preproc.preprocess(
            validation[JAKKAR.SOURCE_TOKENS]
        )
        return validation

    def _process_fuzzy(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        if self._stopped:
            raise SimFyzerGracefullExit

        print("make_fuzzy")

        try:
            data = self.fuzzy.search(
                data,
                JAKKAR.CLIENT_TOKENS,
                JAKKAR.SOURCE_TOKENS,
                self._process_pool,
                self.call_progress,
            )
            return data

        except FyzzySearchGracefullExit:
            raise FyzzySearchGracefullExit

    def _process_ratio(self, data: pd.DataFrame) -> pd.DataFrame:
        if self._stopped:
            raise SimFyzerGracefullExit

        print("make_ratio")
        ratio = self.rate_counter.count_ratio(
            data,
            JAKKAR.CLIENT_TOKENS,
            JAKKAR.SOURCE_TOKENS,
        )
        return ratio

    def _process_marks_count(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        return self.marks_counter.count_marks(
            self.ratio,
            data,
            JAKKAR.CLIENT_TOKENS,
            JAKKAR.SOURCE_TOKENS,
        )

    def _process_tokens_count(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        data[JAKKAR.CLIENT_TOKENS_COUNT] = data[JAKKAR.CLIENT_TOKENS].apply(
            lambda x: len(x)
        )
        data[JAKKAR.SOURCE_TOKENS_COUNT] = data[JAKKAR.SOURCE_TOKENS].apply(
            lambda x: len(x)
        )
        return data

    def call_status(self, message: str) -> None:
        if self.status_callback is not None:
            self.status_callback(message)

    def call_progress(self, count: int, total: int) -> None:
        if self.progress_callback is not None:
            progress = int(count / total * 100)
            self.progress_callback(progress)

    def stop_callback(self) -> None:
        self._stopped = True
        self.fuzzy.stop_callback()

    def validate(
        self,
        data: pd.DataFrame,
        client_column: str,
        source_column: str,
        process_pool: multiprocessing.Pool = None,
    ) -> pd.DataFrame:
        self._process_pool = process_pool

        self.call_status("Создаю рабочие столбцы")
        data = self._create_working_rows(data, client_column, source_column)

        self.call_status("Провожу токенизацию")
        data = self._process_tokenization(data)

        self.call_status("Предобработка данных")
        data = self._process_preprocessing(data)

        # очистка токенов-символов по типу (, ), \, . и т.д.
        # актуально для word_tokenizer
        self.call_status("Преобразование Левенштейна")
        data = self._process_fuzzy(data)

        self.call_status("Вычисляю веса токенов")
        self.ratio = self._process_ratio(data)

        self.call_status("Вычисляю оценки")
        if self._stopped:
            raise SimFyzerGracefullExit

        data = self._make_tokens_set(data)
        data = self._process_tokens_count(data)
        data = self._process_marks_count(data)

        # if self.debug:
        #     self._save_ratio()

        data[JAKKAR.VALIDATED] = np.where(
            data[self.marks_counter.validation_column] >= self.validation_treshold,
            1,
            0,
        )

        self.call_status("Закончил валидацию")
        data = self._delete_working_rows(data)
        return data


def setup_SimFyzer(
    config: dict,
    fuzzy_threshold: float,
    validation_threshold: float,
    status_callback: Callable = None,
    progress_callback: Callable = None,
) -> SimFyzer:
    regex_weights = RegexCustomWeights(
        config[CONFIG.REGEX_WEIGHTS][REGEX_WEIGHTS.CAPS],
        config[CONFIG.REGEX_WEIGHTS][REGEX_WEIGHTS.CAPITAL],
        config[CONFIG.REGEX_WEIGHTS][REGEX_WEIGHTS.LOW],
        config[CONFIG.REGEX_WEIGHTS][REGEX_WEIGHTS.OTHER],
    )

    tokenizer = RegexTokenizer(
        {
            LanguageType.RUS: config[CONFIG.LANGUAGE_WEIGHTS][LANGUAGE_WEIGHTS.RUS],
            LanguageType.ENG: config[CONFIG.LANGUAGE_WEIGHTS][LANGUAGE_WEIGHTS.ENG],
        },
        weights_rules=regex_weights,
    )

    preprocessor = Preprocessor(config[CONFIG.WORD_MIN_LEN])
    transformer = TokenTransformer()
    rate_counter = RateCounter(
        config[CONFIG.RATIO][RATIO.MIN_RATIO],
        config[CONFIG.RATIO][RATIO.MAX_RATIO],
        config[CONFIG.RATIO][RATIO.MIN_APPEARANCE],
        config[CONFIG.RATIO][RATIO.MIN_APPEARANCE_PENALTY],
        RateFunction.map(config[CONFIG.RATIO][RATIO.RATE_FUNC]),
    )
    fuzzy = FuzzySearch(fuzzy_threshold, transformer=transformer)
    marks_counter = MarksCounter(MarksMode.MULTIPLE)

    simfyzer = SimFyzer(
        tokenizer=tokenizer,
        preprocessor=preprocessor,
        fuzzy=fuzzy,
        rate_counter=rate_counter,
        marks_counter=marks_counter,
        validation_treshold=validation_threshold,
        status_callback=status_callback,
        progress_callback=progress_callback,
    )
    return simfyzer


def read_config(path: str | Path) -> dict:
    with open(path, "rb") as file:
        data = json.loads(file.read())
    return data
