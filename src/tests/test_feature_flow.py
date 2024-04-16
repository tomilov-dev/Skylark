import sys
import pytest
import time
import multiprocessing
import regex as re
import pandas as pd
from pathlib import Path


PROJECT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_DIR))

from common_test import (
    NumericDataSet,
    StringDataSet,
    CLIENT_PRODUCT,
    SOURCE_PRODUCT,
    IS_EQUAL,
    MEASURES_CONFIG,
    DEBUG,
)
from custom_data import CustomFeatureFlowData
from src.feature_flow.main import (
    FeatureFlow,
    FeatureGenerator,
    FEATURES,
)


class BaseTestFeatureFlow(object):
    debug = False
    process_pool = None

    def validator(self):
        features = FeatureGenerator().generate(MEASURES_CONFIG)
        return FeatureFlow(
            CLIENT_PRODUCT,
            SOURCE_PRODUCT,
            features,
        )

    def checkout(self, data: pd.DataFrame) -> bool:
        check = (data[IS_EQUAL] != data[FEATURES.VALIDATED]).sum() == 0
        if self.debug:
            data[DEBUG] = data[IS_EQUAL] - data[FEATURES.VALIDATED]
            data = data[data[DEBUG] != 0]
            data.to_excel("debug.xlsx", index=False)

        return check

    def run_validation_test(
        self,
        data: pd.DataFrame,
        validator: FeatureFlow,
    ):
        data = validator.validate(data, self.process_pool)
        assert self.checkout(data)


class TestFeatureFlowGenerics(BaseTestFeatureFlow):
    def test_generics_feature_validation(self):
        data = pd.concat(
            [
                # NumericDataSet.volume_data(),
                # NumericDataSet.concentration_per_dose_data(),
                NumericDataSet.all(),
                StringDataSet.all(),
            ]
        )

        self.run_validation_test(
            data,
            self.validator(),
        )


class TestFeatureFlowCustom(BaseTestFeatureFlow):
    def test_custom_feature_validation(self):
        data = CustomFeatureFlowData.get_data()
        self.run_validation_test(data, self.validator())


class FeatureFlowGenericsTestsDebug(TestFeatureFlowGenerics):
    def __init__(self) -> None:
        super().__init__()
        self.debug = True
        # self.process_pool = None
        self.process_pool = multiprocessing.Pool(8)


class FeatureVCustomTestsDebug(TestFeatureFlowCustom):
    def __init__(self) -> None:
        super().__init__()
        self.debug = True
