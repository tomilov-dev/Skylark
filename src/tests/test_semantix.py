import sys
import pytest
import regex as re
import pandas as pd
from pathlib import Path


PROJECT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_DIR))

from common_test import (
    NumericDataSet,
    StringDataSet,
    UncreationDataSet,
    CLIENT_PRODUCT,
    SOURCE_PRODUCT,
    DEBUG,
    REGEX,
    CHECKOUT,
    IS_EQUAL,
    MEASURES_CONFIG,
    DataTypes,
)
from src.semantix.measures_extraction import MeasureExtractor, MeasuresExtractor
from custom_data import CustomData, CustomUncreationData

EMPTY = "_test_empty"


class BaseTestSemantix(object):
    debug = False

    def extractor(self):
        return MeasureExtractor(MEASURES_CONFIG)

    def extract(self, row: pd.Series) -> str:
        search = re.search(row[REGEX], row[SOURCE_PRODUCT], flags=re.IGNORECASE)
        return 1 if search else 0

    def rx_matching_checkout(self, data: pd.DataFrame) -> bool:
        data[SOURCE_PRODUCT] = "   " + data[SOURCE_PRODUCT].astype(str) + "   "

        data[CHECKOUT] = data.apply(self.extract, axis=1)
        check = (data[IS_EQUAL] != data[CHECKOUT]).sum() == 0
        if self.debug and not check:
            data[DEBUG] = data[IS_EQUAL] != data[CHECKOUT]
            data = data[data[DEBUG] == True]
            data.to_excel("debug.xlsx", index=False)

        data.loc[:, SOURCE_PRODUCT] = data[SOURCE_PRODUCT].str.strip()
        return check

    def run_rx_matching_test(
        self,
        data: pd.DataFrame,
        measure_name: DataTypes,
        extractor: MeasureExtractor,
    ):
        data[REGEX] = extractor.extract(data, CLIENT_PRODUCT, measure_name)
        assert self.rx_matching_checkout(data)

    def uncreation_checkout(self, data: pd.DataFrame) -> bool:
        check = data[EMPTY].sum() == 0
        if self.debug and not check:
            data[DEBUG] = data[EMPTY] == True
            data = data[data[DEBUG] == True]
            data.to_excel("debug.xlsx", index=False)

        return check

    def run_uncreation_test(
        self,
        data: pd.DataFrame,
        measure_name: DataTypes,
        extractor: MeasureExtractor,
    ):
        def not_empty(row: str) -> bool:
            if row == "":
                return False
            return True

        data.loc[:, REGEX] = extractor.extract(data, CLIENT_PRODUCT, measure_name)
        data.loc[:, EMPTY] = data[REGEX].apply(not_empty)
        assert self.uncreation_checkout(data)


class TestSemantixGenericsRxMatching(BaseTestSemantix):
    def test_rx_matching_weight(self):
        self.run_rx_matching_test(
            NumericDataSet.weight_data(),
            DataTypes.weight,
            self.extractor(),
        )

    def test_rx_matching_volume(self):
        self.run_rx_matching_test(
            NumericDataSet.volume_data(),
            DataTypes.volume,
            self.extractor(),
        )

    def test_rx_matching_quantity(self):
        self.run_rx_matching_test(
            NumericDataSet.quantity_data(),
            DataTypes.quantity,
            self.extractor(),
        )

    def test_rx_matching_memory_capacity(self):
        self.run_rx_matching_test(
            NumericDataSet.memory_capacity_data(),
            DataTypes.memory_capacity,
            self.extractor(),
        )

    def test_rx_matching_concentration_per_dose(self):
        self.run_rx_matching_test(
            NumericDataSet.concentration_per_dose_data(),
            DataTypes.concentration_per_dose,
            self.extractor(),
        )

    def test_rx_matching_length(self):
        self.run_rx_matching_test(
            NumericDataSet.length_data(),
            DataTypes.lenght,
            self.extractor(),
        )

    def test_rx_matching_color(self):
        self.run_rx_matching_test(
            StringDataSet.color_data(),
            DataTypes.color,
            self.extractor(),
        )


class TestSemantixCustomRxMatching(BaseTestSemantix):
    def test_rx_matching_weight_custom(self):
        self.run_rx_matching_test(
            CustomData.custom_weight_data(),
            DataTypes.weight,
            self.extractor(),
        )

    def test_rx_matching_volume_custom(self):
        self.run_rx_matching_test(
            CustomData.custom_volume_data(),
            DataTypes.volume,
            self.extractor(),
        )

    def test_rx_matching_quantity_custom(self):
        self.run_rx_matching_test(
            CustomData.custom_quantity_data(),
            DataTypes.quantity,
            self.extractor(),
        )

    def test_rx_matching_memory_capacity_custom(self):
        self.run_rx_matching_test(
            CustomData.custom_memory_capacity_data(),
            DataTypes.memory_capacity,
            self.extractor(),
        )

    def test_rx_matching_concentration_per_dose_custom(self):
        self.run_rx_matching_test(
            CustomData.custom_concentration_per_dose_data(),
            DataTypes.concentration_per_dose,
            self.extractor(),
        )

    def test_rx_matching_length_custom(self):
        self.run_rx_matching_test(
            CustomData.custom_length_data(),
            DataTypes.lenght,
            self.extractor(),
        )


class SemantixGenericsRxMatchingTestsDebug(TestSemantixGenericsRxMatching):
    def __init__(self) -> None:
        super().__init__()
        self.debug = True


class SemantixCustomRxMatchingTestsDebug(TestSemantixCustomRxMatching):
    def __init__(self) -> None:
        super().__init__()
        self.debug = True


class TestSemantixUncreation(BaseTestSemantix):
    @pytest.fixture
    def dataset(self) -> UncreationDataSet:
        return UncreationDataSet()

    def test_uncreation_weight(self, dataset: UncreationDataSet):
        self.run_uncreation_test(
            dataset.get_data(DataTypes.weight),
            DataTypes.weight,
            self.extractor(),
        )

    def test_uncreation_volume(self, dataset: UncreationDataSet):
        self.run_uncreation_test(
            dataset.get_data(DataTypes.volume),
            DataTypes.volume,
            self.extractor(),
        )

    @pytest.mark.xfail
    def test_uncreation_quantity(self, dataset: UncreationDataSet):
        self.run_uncreation_test(
            dataset.get_data(DataTypes.quantity),
            DataTypes.quantity,
            self.extractor(),
        )

    def test_uncreation_memory_capacity(self, dataset: UncreationDataSet):
        self.run_uncreation_test(
            dataset.get_data(DataTypes.memory_capacity),
            DataTypes.memory_capacity,
            self.extractor(),
        )

    def test_uncreation_concentration_per_dose(self, dataset: UncreationDataSet):
        self.run_uncreation_test(
            dataset.get_data(DataTypes.concentration_per_dose),
            DataTypes.concentration_per_dose,
            self.extractor(),
        )

    def test_uncreation_lenght(self, dataset: UncreationDataSet):
        self.run_uncreation_test(
            dataset.get_data(DataTypes.lenght),
            DataTypes.lenght,
            self.extractor(),
        )

    def test_uncreation_color(self, dataset: UncreationDataSet):
        self.run_uncreation_test(
            dataset.get_data(DataTypes.color),
            DataTypes.color,
            self.extractor(),
        )


class AutosemUncreationTestsDebug(TestSemantixUncreation):
    def __init__(self) -> None:
        super().__init__()
        self.debug = True
