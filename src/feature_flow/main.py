import sys
import json
import warnings
import multiprocessing
import regex as re
import pandas as pd

from abc import ABC, abstractmethod
from typing import Union, Set, Callable
from pathlib import Path
from tqdm import tqdm
from functools import partial

tqdm.pandas()

SRC_DIR = Path(__file__).parent.parent
PROJ_DIR = SRC_DIR.parent

sys.path.append(str(PROJ_DIR))

from src.notation import FEATURES
from src.feature_flow.feature_generator import FeatureGenerator
from src.feature_flow.feature_functool import (
    AbstractFeature,
    FeatureUnit,
    FeatureList,
    FeatureValidationMode,
    NotFoundStatus,
)

warnings.filterwarnings("ignore")


class FeatureFlowGracefullExit(Exception):
    pass


class AbstractFeatureFlow(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        pass


def findall_func(cell: str, unit: FeatureUnit) -> list[str]:
    output = re.findall(unit.regex, str(cell), re.IGNORECASE)
    return output


def preproccess_func(
    values: list[str],
    feature: AbstractFeature,
    unit: FeatureUnit,
) -> list[AbstractFeature]:
    return [feature(value, unit) for value in values]


def del_pattern_func(cell: str, unit: FeatureUnit) -> str:
    return re.sub(unit.regex, "  ", cell)


class FeatureFlow(AbstractFeatureFlow):
    def __init__(
        self,
        client_column: str,
        source_column: str,
        features_list: list[AbstractFeature],
        skip_intermediate_validated: bool = True,
        status_callback: Callable = None,
        progress_callback: Callable = None,
    ) -> None:
        self.CLIENT_NAME = client_column
        self.SOURCE_NAME = source_column

        self.skip_intermediate_validated = skip_intermediate_validated
        self.features = FeatureList(features_list)

        self.status_callback = status_callback
        self.progress_callback = progress_callback

        self._process_pool = None
        self._stopped = False

    def _data_preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        data[FEATURES.VALIDATED] = 1

        data.loc[:, FEATURES.CLIENT] = [[] for _ in range(len(data))]
        data.loc[:, FEATURES.SOURCE] = [[] for _ in range(len(data))]

        data.loc[:, FEATURES.CLIENT_NAME] = "  " + data[self.CLIENT_NAME] + "   "
        data.loc[:, FEATURES.SOURCE_NAME] = "  " + data[self.SOURCE_NAME] + "   "

        return data

    def _data_clean(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.drop(
            [
                FEATURES.CLIENT_NAME,
                FEATURES.SOURCE_NAME,
            ],
            axis=1,
        )

        return data

    def _feature_search(
        self,
        data: list[str],
        unit: FeatureUnit,
    ) -> list[list[str]]:
        func = partial(findall_func, unit=unit)

        if self._process_pool != None:
            features = self._process_pool.map(func, data)
        else:
            features = map(lambda x: func(x), data)

        return features

    def _feature_preprocess(
        self,
        data: list[list[str]],
        feature: AbstractFeature,
        unit: FeatureUnit,
    ) -> list[AbstractFeature]:
        func = partial(preproccess_func, feature=feature, unit=unit)
        features = list(map(lambda x: func(x), data))

        # if self._process_pool:
        #     features = self._process_pool.map(func, data)
        # else:
        #     features = list(map(lambda x: func(x), data))

        return features

    def _del_unit(
        self,
        data: list[str],
        unit: FeatureUnit,
    ) -> pd.Series:
        func = partial(del_pattern_func, unit=unit)

        if self._process_pool:
            data = self._process_pool.map(func, data)
        else:
            data = list(map(lambda x: func(x), data))

        return data

    def _determine_based_intersection(
        self,
        cif: set,
        sif: set,
        val_mode: FeatureValidationMode,
    ) -> int:
        if val_mode is FeatureValidationMode.MODEST:
            based = min(len(cif), len(sif))
        elif val_mode is FeatureValidationMode.CLIENT:
            based = len(cif)
        elif val_mode is FeatureValidationMode.SOURCE:
            based = len(sif)
        else:  # val_mode is FeatureValidationMode.STRICT
            based = max(len(cif), len(sif))
        return based

    def _intermediate_validation_func(
        self,
        row: list[Union[int, set]],
    ) -> list[int]:
        desicion = row[0]
        if desicion == 1:
            cif = row[1]
            sif = row[2]

            not_found_status = NotFoundStatus(
                cif,
                sif,
                self.__not_found_mode,
                self.__feature_name,
            )

            if not_found_status:
                desicion = not_found_status.desicion
                return desicion

            based = self._determine_based_intersection(cif, sif, self.__val_mode)
            intersect = cif.intersection(sif)
            desicion = desicion if len(intersect) == based else 0

        return desicion

    def _intermediate_validation(
        self,
        data: pd.DataFrame,
        feature: AbstractFeature,
        cif_massive: list[list[AbstractFeature]],
        sif_massive: list[list[AbstractFeature]],
    ) -> pd.DataFrame:
        cif_massive = map(set, cif_massive)
        sif_massive = map(set, sif_massive)

        intermediate = data[FEATURES.VALIDATED].to_list()
        massive = zip(intermediate, cif_massive, sif_massive)

        self.__feature_name = feature.NAME
        self.__val_mode = feature.VALIDATION_MODE
        self.__not_found_mode = feature.NOT_FOUND_MODE

        decisions = list(map(self._intermediate_validation_func, tqdm(massive)))
        data[FEATURES.VALIDATED] = decisions

        return data

    def _add_intermediate(
        self,
        container: list[list[AbstractFeature]],
        new_features: list[list[AbstractFeature]],
    ) -> list[list[AbstractFeature]]:
        for index in range(len(container)):
            container[index] += new_features[index]
        return container

    def call_progress(self, count: int, total: int) -> None:
        if self.progress_callback is not None:
            if total > 0:
                progress = int(count / total * 100)
                self.progress_callback(progress)

    def call_status(self, message: str) -> None:
        if self.status_callback is not None:
            self.status_callback(message)

    def _extract(self, data: pd.DataFrame) -> pd.DataFrame:
        client = data[FEATURES.CLIENT_NAME].to_list()  # data client
        source = data[FEATURES.SOURCE_NAME].to_list()  # data source

        cfeatures = [[] for _ in range(len(data))]  # client features
        sfeatures = [[] for _ in range(len(data))]  # source features

        count = 0
        total = len(self.features)

        self.call_progress(count, total)
        for feature in self.features:
            if self._stopped:
                raise FeatureFlowGracefullExit

            feature: AbstractFeature
            self.call_status(f"Извлекаю {feature.NAME}")

            CI = [[] for _ in range(len(data))]
            SI = [[] for _ in range(len(data))]

            for unit in feature.units:
                cif = self._feature_search(client, unit)
                sif = self._feature_search(source, unit)

                cif = self._feature_preprocess(cif, feature, unit)
                sif = self._feature_preprocess(sif, feature, unit)

                client = self._del_unit(client, unit)
                source = self._del_unit(source, unit)

                CI = self._add_intermediate(CI, cif)
                SI = self._add_intermediate(SI, sif)

            cfeatures = self._add_intermediate(cfeatures, CI)
            sfeatures = self._add_intermediate(sfeatures, SI)

            data = self._intermediate_validation(data, feature, CI, SI)

            count += 1
            self.call_progress(count, total)

        data[FEATURES.CLIENT] = cfeatures
        data[FEATURES.SOURCE] = sfeatures

        self.call_status("Закончил валидацию по величинам")
        return data

    def stop_callback(self) -> None:
        self._stopped = True

    def validate(
        self,
        data: pd.DataFrame,
        process_pool: multiprocessing.Pool = None,
    ) -> pd.DataFrame:
        self._process_pool = process_pool  # setup process pool

        self.call_status("Начинаю предобработку данных")
        data = self._data_preprocess(data)

        self.call_status("Начинаю валидацию по величинам")
        data = self._extract(data)

        self.call_status("Начинаю чистку данных")
        data = self._data_clean(data)

        return data


def read_config(path: str) -> dict:
    with open(path, "rb") as file:
        data = json.loads(file.read())
    return data
