import sys
import itertools
import json
import random
import pandas as pd
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple


CLIENT_RECORD = "_client_record_test"
CLIENT_PRODUCT = "_client_product_test"

SOURCE_RECORD = "_source_record_test"
SOURCE_PRODUCT = "_source_product_test"

DEBUG = "DEBUG"
MEASURE_NAME = "_measure_name_test"
IS_EQUAL = "_is_equal_test"
REGEX = "_regex_test"
EXTRACTED = "_extracted_test"
CHECKOUT = "_checkout_test"

PROJECT_DIR = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_DIR / "config"
sys.path.append(str(PROJECT_DIR))


from config.measures_config.config_parser import (
    MEASURE_MAPPER,
    CONFIG,
    MEASURE,
    AUTOSEM_CONF,
)


def get_measures_config() -> dict:
    MEASURES_CONFIG_PATH = CONFIG_DIR / "measures_config" / "setups" / "main.json"
    with open(MEASURES_CONFIG_PATH, "rb") as file:
        conf = json.loads(file.read())

    # correct all numeric measures to overall merge mode
    for measure in conf[CONFIG.NUMERIC_MEASURES][CONFIG.MEASURES]:
        measure[MEASURE.AUTOSEM][AUTOSEM_CONF.MERGE_MODE] = "overall"

    return conf


def get_fuzzy_config() -> dict:
    FUZZY_CONFIG_PATH = CONFIG_DIR / "simfyzer_config" / "setups" / "main.json"
    with open(FUZZY_CONFIG_PATH, "rb") as file:
        conf = json.loads(file.read())
    return conf


class DataTypes(MEASURE_MAPPER):
    pass


class NumericMeasure(object):
    def __init__(
        self,
        relative_weight: int,
        designations: list[str],
    ):
        self.relative_weight = Decimal(str(relative_weight))
        self.designations = designations

    def __repr__(self) -> str:
        return f"{self.designations} {self.relative_weight}"


class NumericProductRecordInterface(object):
    product: str
    str_value: str
    type: DataTypes
    num_value: int
    measure: NumericMeasure


class NumericProductRecord(NumericProductRecordInterface):
    def __init__(
        self,
        product: str,
        str_value: str,
        type: DataTypes,
        num_value: int,
        measure: NumericMeasure,
    ) -> None:
        self.product = product
        self.str_value = str_value
        self.type = type
        self.num_value = num_value
        self.measure = measure

    def __hash__(self) -> str:
        return hash(self.product)

    def __repr__(self) -> str:
        return f"{self.product}"


class NumericDataGenerator(object):
    @classmethod
    def declare_records(
        cls,
        measures: list[NumericMeasure],
        product_names: list[str],
        data_type: DataTypes,
        multiplier: int = 1,
        backward: bool = True,
    ) -> list[NumericProductRecord]:
        def generate_value(str_num: str) -> str:
            value = ""
            rsp = random.choice(range(0, 4))
            spaces = "" if not rsp else " " * rsp

            if backward:
                value = f"{str_num}{spaces}{designation}"
            else:
                value = f"{designation}{spaces}{str_num}"
            return value

        records = []
        for index in range(len(measures)):
            base = measures[index].relative_weight

            for measure in measures:
                for designation in measure.designations:
                    for name in product_names:
                        numeric_value = measure.relative_weight / base
                        numeric_value *= multiplier

                        str_num = f"{numeric_value:.20f}".rstrip("0").rstrip(".")
                        str_value = generate_value(str_num)
                        product = f"{name} {str_value}"

                        record = NumericProductRecord(
                            product,
                            str_value,
                            data_type,
                            numeric_value,
                            measure,
                        )
                        records.append(record)

        return records

    @classmethod
    def determine_marks(
        cls,
        row: pd.Series,
    ) -> bool:
        record1: NumericProductRecord = row[CLIENT_RECORD]
        record2: NumericProductRecord = row[SOURCE_RECORD]

        num1 = record1.num_value / record1.measure.relative_weight
        num2 = record2.num_value / record2.measure.relative_weight

        return num1 == num2

    @classmethod
    def distribute_records(
        cls,
        records: list[NumericProductRecord],
    ) -> pd.DataFrame:
        measure_name = records[0].type

        cartesian = list(itertools.product(records, records))
        data = pd.DataFrame(data=cartesian, columns=[CLIENT_RECORD, SOURCE_RECORD])

        products = list(map(lambda x: (x[0].product, x[1].product), cartesian))
        data[[CLIENT_PRODUCT, SOURCE_PRODUCT]] = products

        data[IS_EQUAL] = data.apply(cls.determine_marks, axis=1)
        data[MEASURE_NAME] = measure_name

        return data


class NumericDataSet(object):
    generator = NumericDataGenerator

    @classmethod
    def weight_data(cls) -> pd.DataFrame:
        records = cls.generator.declare_records(
            measures=[
                NumericMeasure(
                    1000000000, ["мкг", "микрограмм", "ug", "µg", "microgram"]
                ),
                NumericMeasure(1000000, ["мг", "mg", "миллиграмм", "milligram"]),
                NumericMeasure(1000, ["г", "гр", "g", "gr", "грамм", "gram"]),
                NumericMeasure(1, ["кг", "kg", "килограмм", "kilogram"]),
            ],
            product_names=["Аспирин", "Корвалол"],
            data_type=DataTypes.weight,
        )

        data = cls.generator.distribute_records(records)
        return data

    @classmethod
    def volume_data(cls) -> pd.DataFrame:
        records = cls.generator.declare_records(
            measures=[
                NumericMeasure(
                    1000, ["мл", "ml", "миллилитр", "milliliter", "millilitre"]
                ),
                NumericMeasure(1, ["л", "l", "литров", "liter", "litre"]),
            ],
            product_names=["Вода", "Сок"],
            data_type=DataTypes.volume,
        )

        data = cls.generator.distribute_records(records)
        return data

    @classmethod
    def quantity_data(cls) -> pd.DataFrame:
        records = []

        for multiplier in [1, 10, 100]:
            backward = cls.generator.declare_records(
                measures=[
                    NumericMeasure(1, ["шт", "пач", "уп"]),
                ],
                product_names=["Вода", "Сок"],
                data_type=DataTypes.quantity,
                multiplier=multiplier,
            )

            forward = cls.generator.declare_records(
                measures=[
                    NumericMeasure(1, ["№", "N", "x"]),
                ],
                product_names=["Вода", "Сок"],
                data_type=DataTypes.quantity,
                multiplier=multiplier,
                backward=False,
            )

            records.extend(backward)
            records.extend(forward)

        data = cls.generator.distribute_records(records)
        return data

    @classmethod
    def memory_capacity_data(cls) -> pd.DataFrame:
        records = cls.generator.declare_records(
            measures=[
                NumericMeasure(1000000000, ["kb", "kilobyte", "килобайт", "кб"]),
                NumericMeasure(1000000, ["mb", "megabyte", "мегабайт", "мб"]),
                NumericMeasure(1000, ["gb", "gigabyte", "гигабайт", "гб"]),
                NumericMeasure(1, ["tb", "terabyte", "терабайт", "тб"]),
            ],
            product_names=["Айфон", "Самсунг"],
            data_type=DataTypes.memory_capacity,
        )

        data = cls.generator.distribute_records(records)
        return data

    @classmethod
    def concentration_per_dose_data(cls) -> pd.DataFrame:
        records = cls.generator.declare_records(
            measures=[
                NumericMeasure(1000000, ["мкг/доза", "мкг/сут", "мкг\доза", "мкг\сут"]),
                NumericMeasure(1000, ["мг/доза", "мг/сут", "мг\доза", "мг\сут"]),
                NumericMeasure(1, ["г/доза", "г/сут", "г\доза", "г\сут"]),
            ],
            product_names=["Аспирин", "Корвалол"],
            data_type=DataTypes.concentration_per_dose,
        )

        data = cls.generator.distribute_records(records)
        return data

    @classmethod
    def length_data(cls) -> pd.DataFrame:
        records = cls.generator.declare_records(
            measures=[
                NumericMeasure(1000000, ["мм", "миллиметр", "mm", "millimeter"]),
                NumericMeasure(100000, ["см", "сантиметр", "centimeter", "cm"]),
                NumericMeasure(1000, ["м", "метр", "m", "meter"]),
                NumericMeasure(1, ["км", "километр", "km", "kilometer"]),
            ],
            product_names=["Полотно", "Сукно"],
            data_type=DataTypes.lenght,
        )

        data = cls.generator.distribute_records(records)
        return data

    @classmethod
    def all(cls) -> pd.DataFrame:
        methods = [
            getattr(cls, method)
            for method in dir(cls)
            if callable(getattr(cls, method)) and method.endswith("data")
        ]

        dataframes: list[pd.DataFrame] = []
        for method in methods:
            dataframe = method()
            dataframes.append(dataframe)

        data = pd.concat(dataframes)
        return data


class StringMeasure(object):
    def __init__(
        self,
        designations: list[str],
    ):
        self.designations = designations
        self.hash = hash(tuple(sorted(designations)))

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            if self.hash == other.hash:
                return True
        return False

    def __repr__(self) -> str:
        return f"{self.designations}"


class StringProductRecordInterface(object):
    product: str
    str_value: str
    type: DataTypes
    measure: StringMeasure


class StringProductRecord(StringProductRecordInterface):
    def __init__(
        self,
        product: str,
        str_value: str,
        type: DataTypes,
        measure: NumericMeasure,
    ) -> None:
        self.product = product
        self.str_value = str_value
        self.type = type
        self.measure = measure

    def __hash__(self) -> str:
        return hash(self.product)

    def __repr__(self) -> str:
        return f"{self.product}"


class StringDataGenerator(object):
    @classmethod
    def declare_records(
        cls,
        measures: list[StringMeasure],
        product_names: list[str],
        data_type: DataTypes,
    ) -> list[StringProductRecord]:
        records = []
        for measure in measures:
            for designation in measure.designations:
                for name in product_names:
                    str_value = designation
                    product = f"{name} {str_value}"

                    record = StringProductRecord(
                        product,
                        str_value,
                        data_type,
                        measure,
                    )
                    records.append(record)

        return records

    @classmethod
    def determine_marks(
        cls,
        row: pd.Series,
    ) -> bool:
        record1: StringProductRecord = row[CLIENT_RECORD]
        record2: StringProductRecord = row[SOURCE_RECORD]

        return record1.measure == record2.measure

    @classmethod
    def distribute_records(
        cls,
        records: list[StringProductRecord],
    ) -> pd.DataFrame:
        measure_name = records[0].type

        cartesian = list(itertools.product(records, records))
        data = pd.DataFrame(data=cartesian, columns=[CLIENT_RECORD, SOURCE_RECORD])

        products = list(map(lambda x: (x[0].product, x[1].product), cartesian))
        data[[CLIENT_PRODUCT, SOURCE_PRODUCT]] = products

        data[IS_EQUAL] = data.apply(cls.determine_marks, axis=1)
        data[MEASURE_NAME] = measure_name

        return data


class StringDataSet(object):
    generator = StringDataGenerator

    @classmethod
    def color_data(cls) -> pd.DataFrame:
        records = cls.generator.declare_records(
            measures=[
                StringMeasure(["красн", "red"]),
                StringMeasure(["син", "blue"]),
                StringMeasure(["зелен", "green"]),
            ],
            product_names=["Пальто", "Шляпа"],
            data_type=DataTypes.color,
        )

        data = cls.generator.distribute_records(records)
        return data

    @classmethod
    def all(cls) -> pd.DataFrame:
        methods = [
            getattr(cls, method)
            for method in dir(cls)
            if callable(getattr(cls, method)) and method.endswith("data")
        ]

        dataframes: list[pd.DataFrame] = []
        for method in methods:
            dataframe = method()
            dataframes.append(dataframe)

        data = pd.concat(dataframes)
        return data


class UncreationDataSet(object):
    def __init__(self) -> None:
        numeric = NumericDataSet.all()
        string = StringDataSet.all()

        all_data = pd.concat([numeric, string], ignore_index=True)
        self.data = all_data.drop_duplicates(
            subset=[
                CLIENT_PRODUCT,
                MEASURE_NAME,
            ]
        )[
            [
                CLIENT_PRODUCT,
                MEASURE_NAME,
            ]
        ]
        self.data.index = range(len(self.data))

    def get_data(self, measure_name: DataTypes) -> pd.DataFrame:
        return self.data[self.data[MEASURE_NAME] != measure_name]


class FuzzyDataAttribute(object):
    def __init__(self, value: str, id: int) -> None:
        self.value = value
        self.id = id

    def __repr__(self) -> str:
        return f"value: {self.value}, id: {self.id}"


class FuzzyDataRecord(object):
    def __init__(
        self,
        entity: str,
        attrs: Tuple[FuzzyDataAttribute],
        extras: Tuple[str],
    ) -> None:
        self.value = self.gen_value(entity, attrs, extras)
        self.id = self.gen_id(entity, attrs)

    def gen_value(
        self,
        entity: str,
        attrs: List[FuzzyDataAttribute],
        extras: Tuple[str],
    ) -> str:
        value = entity
        value += " " + " ".join(map(lambda x: x.value, attrs))
        value += " " + " ".join(extras)
        return value

    def gen_id(
        self,
        entity: str,
        attrs: List[FuzzyDataAttribute],
    ) -> int:
        id = entity
        id += "+".join(map(lambda x: str(x.id), attrs))
        return hash(id)


class FuzzyDataGenerator(object):
    def _extract_kwargs(
        self,
        kwargs: dict,
    ) -> Tuple[List, List]:
        attributes: List[List[FuzzyDataAttribute]] = []
        extras: List[List[str]] = []

        attribute_id = 0
        for key in kwargs:
            if "attrs" in key:
                local_attributes = kwargs[key]
                list_attributes = []
                for index in range(len(local_attributes)):
                    values = local_attributes[index]

                    for value in values:
                        attribute = FuzzyDataAttribute(value, attribute_id)
                        list_attributes.append(attribute)
                    attribute_id += 1

                attributes.append(list_attributes)

            elif "extras" in key:
                extras.append(kwargs[key])

        return attributes, extras

    def _generate_records(
        self,
        entities: List[str],
        attributes: List[List[str]],
        extras: List[List[str]],
    ) -> List[FuzzyDataRecord]:
        records: List[FuzzyDataRecord] = []

        attrs_prod = list(itertools.product(*attributes))
        extras_prod = list(itertools.product(*extras))
        others = list(itertools.product(attrs_prod, extras_prod))

        for entity in entities:
            for attrs, extras in others:
                record = FuzzyDataRecord(entity, attrs, extras)
                records.append(record)

        return records

    def generate(
        self,
        entities: list[str],
        k: int,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Attributes: list[list[str]] = attrs{int} - affects marks
        Extras: list[str] = extras{int} - doesn't affect marks
        """
        data = pd.DataFrame()

        attributes, extras = self._extract_kwargs(kwargs)
        records = self._generate_records(entities, attributes, extras)
        records_product = list(itertools.product(records, records))
        records_product = list(
            map(
                lambda x: (
                    x[0].value,
                    x[1].value,
                    x[0].id == x[1].id,
                ),
                records_product,
            )
        )

        equal_records = list(filter(lambda x: x[2] == True, records_product))
        unequal_records = list(filter(lambda x: x[2] == False, records_product))

        equal_records = random.sample(equal_records, k=min(k / 2, len(equal_records)))
        k_residue = len(equal_records)
        unequal_records = random.sample(unequal_records, k=k_residue)

        records = equal_records + unequal_records
        data[CLIENT_PRODUCT] = list(map(lambda x: x[0], records))
        data[SOURCE_PRODUCT] = list(map(lambda x: x[1], records))
        data[IS_EQUAL] = list(map(lambda x: x[2], records))

        return data


class FuzzyDataSet(object):
    generator: FuzzyDataGenerator = FuzzyDataGenerator()

    @classmethod
    def small(self):
        entities = [
            "Яблоко",
            "Апельсин",
            "Груша",
            "Грейпфрут",
            "Виноград",
            "Помидор",
            "Огурец",
        ]
        attrs1 = [
            ["красный"],
            ["зеленый"],
            ["коричневый"],
            ["черный"],
            ["оранжевый"],
            ["фиолетовый"],
            ["розовый"],
        ]
        attrs2 = [
            ["египетский"],
            ["российский"],
            ["французский"],
            ["израильский"],
            ["китайский"],
            ["абхазский"],
            ["укранский"],
            ["японский"],
        ]
        extras1 = ["спелые", "свежие", "вкусные"]

        data = self.generator.generate(
            entities,
            k=10000,
            attrs1=attrs1,
            attrs2=attrs2,
            extras1=extras1,
        )

        return data


MEASURES_CONFIG = get_measures_config()
FUZZY_CONFIG = get_fuzzy_config()
