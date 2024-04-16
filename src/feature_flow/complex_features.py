import sys
import regex as re
from abc import ABC
from decimal import Decimal
from pathlib import Path

SRC_DIR = Path(__file__).parent.parent
PROJECT_DIR = SRC_DIR.parent

sys.path.append(str(PROJECT_DIR))
from src.feature_flow.feature_functool import AbstractFeature, FeatureUnit


class FeatureValidationMode(object):
    STRICT = "strict"
    MODEST = "modest"
    CLIENT = "client"
    SOURCE = "source"

    modes = {STRICT, MODEST, CLIENT, SOURCE}
    default = STRICT

    @classmethod
    def checkout(self, mode: str) -> str:
        if mode not in self.modes:
            mode = self.default
        return mode


class FeatureNotFoundMode(object):
    STRICT = "strict"
    MODEST = "modest"

    modes = {STRICT, MODEST}
    default = STRICT

    @classmethod
    def checkout(self, mode: str) -> str:
        if mode not in self.modes:
            mode = self.default
        return mode


class Measure(object):
    def __init__(
        self,
        name: str,
        weight: float,
        designation: str,
        regex: str = "",
    ) -> None:
        self.name = name
        self.weight = weight
        self.designation = designation
        self.regex = regex


class FeatureMeasures(object):
    def __init__(
        self,
        measures: list[Measure],
        numerical_rx: str = r"\d*[.,]?\d+\s*",
        prefix: str = "",
        postfix: str = "",
    ) -> None:
        self.numerical_regex = numerical_rx
        self.prefix = prefix
        self.postfix = postfix
        self.measures = self._prepare(measures)

        self.__index = 0

    def _make_regex(self, measure: Measure) -> str:
        return f"{self.prefix}({self.numerical_regex}(?:{measure.designation})){self.postfix}"

    def _prepare(
        self,
        measures: list[Measure],
    ) -> list[Measure]:
        for measure in measures:
            measure.weight = Decimal(str(measure.weight))

            if not measure.regex:
                measure.regex = self._make_regex(measure)
        return measures

    def __iter__(self):
        while self.__index < len(self.measures):
            yield self.measures[self.__index]
            self.__index += 1


class Type(object):
    def __init__(
        self,
        name: str,
        designation: str,
        regex: str = "",
    ) -> None:
        self.name = name
        self.designation = designation
        self.regex = regex


class FeatureTypes(object):
    def __init__(
        self,
        types: list[Type],
        prefix: str = "",
        postfix: str = "",
    ) -> None:
        self.prefix = prefix
        self.postfix = postfix
        self.types = self._prepare(types)

        self.__index = 0

    def _make_regex(self, type_: Type) -> Type:
        return f"{self.prefix}{type_.designation}{self.postfix}"

    def _prepare(self, types: list[Type]) -> Type:
        for type_ in types:
            if not type_.regex:
                type_.regex = self._make_regex(type_)
        return types

    def __iter__(self):
        while self.__index < len(self.types):
            yield self.types[self.__index]
            self.__index += 1


class AbstractFeature(ABC):
    TYPES: FeatureTypes
    NAME: str
    VALIDATION_MODE: FeatureValidationMode
    NOT_FOUND_MODE: FeatureNotFoundMode
    PRIORITY: int = 10

    def __init__(self, v) -> None:
        self.standard_value = v

    @classmethod
    @property
    def measures(self):
        pass


class StringFeature(AbstractFeature):
    TYPES: FeatureTypes
    NAME: str
    VALIDATION_MODE: FeatureValidationMode
    NOT_FOUND_MODE: FeatureNotFoundMode
    PRIORITY: int = 10

    def __init__(
        self,
        value: str,
        type_: Type,
    ) -> None:
        self.original_value = value
        self.standard_value = self._standartization(type_)

    def _standartization(self, type_: Type) -> str:
        return type_.name

    def __eq__(self, other: AbstractFeature) -> bool:
        if isinstance(other, self.__class__):
            if self.standard_value == other.standard_value:
                return True
        return False

    def __hash__(self) -> int:
        return hash(self.standard_value)

    def __repr__(self) -> str:
        return rf"{self.NAME} = {self.standard_value}"

    def __str__(self) -> str:
        return rf"{self.NAME} = {self.standard_value}"

    @classmethod
    @property
    def measures(self):
        return self.TYPES


class NumericalFeature(AbstractFeature):
    MEASURES: FeatureMeasures
    NAME: str
    VALIDATION_MODE: FeatureValidationMode
    NOT_FOUND_MODE: FeatureNotFoundMode
    PRIORITY: int = 10

    def __init__(
        self,
        value: str,
        measure: Measure,
    ) -> None:
        self.original_value = value
        self.standard_value = self._standartization(value, measure)

    def _standartization(self, value: str, measure: Measure):
        num_value = re.search(r"\d+[.,]?\d*", value)[0]
        num_value = num_value.replace(",", ".")
        num_value = Decimal(num_value)

        kf = measure.weight
        num_value = num_value * kf

        # TODO: решить, оставить ли Decimal
        return num_value

    def __eq__(self, other: AbstractFeature) -> bool:
        if isinstance(other, self.__class__):
            if self.standard_value == other.standard_value:
                return True
        return False

    def __hash__(self) -> int:
        return hash(self.standard_value)

    def __repr__(self) -> str:
        return rf"{self.NAME} = {self.standard_value}"

    def __str__(self) -> str:
        return rf"{self.NAME} = {self.standard_value}"

    @classmethod
    @property
    def measures(self):
        return self.MEASURES


class Designation(object):
    def __init__(self, original_value: str) -> None:
        self.original_value = original_value
        self.num_value = self._get_num_value(original_value)

        self.checked = False
        self.weight = None
        self.standard_value = self.num_value

    def _get_num_value(self, value: str) -> Decimal:
        num_value: str = re.search(r"\d*[.,]?\d+", value)[0]
        num_value = Decimal(num_value.replace(",", "."))
        return num_value

    def set_weight(self, weight: float) -> None:
        self.weight = weight
        self.checked = True

    def set_standard_value(self, potential_weight: float):
        """potential weight is using if designation don't have weight"""
        if self.have_weight():
            self.standard_value = self.num_value * Decimal(str(self.weight))
        else:
            self.standard_value = self.num_value * Decimal(str(potential_weight))
        return self

    def have_weight(self) -> bool:
        if self.weight is None:
            return False
        return True

    @property
    def value(self):
        return self.original_value

    def __repr__(self) -> str:
        return rf"{self.original_value}; {self.weight}; {self.standard_value}"


class ComplexDimension(AbstractFeature):
    NAME: str = "Complex Dimension"

    _num = r"\d*[.,]?\d+"
    _sep_ = r"[xх]|на"
    _sep = rf"(?:{_sep_})"
    _sgn = r"(?:см|cm|мм|mm|м|m)?"

    _weights = [
        (0.001, r"мм|mm"),
        (0.01, r"см|cm"),
        (1, r"m([^m]|\b)|м([^м]|\b)"),
    ]

    NDIM = FeatureUnit(
        "n-размерность",
        regex=rf"{_num}\s*{_sgn}\s*{_sep}\s*{_num}\s*{_sgn}(?:\s*{_sgn}\s*{_sep}\s*{_num}\s*{_sgn})*(?:\b|$)",
        weight=1,
    )

    def __init__(
        self,
        value: str,
        measure: Measure,
    ) -> None:
        self.original_value = value
        self.standard_weight = self._weights[1][0]

        self.standard_value = self._standartization(value)

    def _set_weight(self, designation: Designation) -> Designation:
        for weight in self._weights:
            srch = re.search(weight[1], designation.value, re.IGNORECASE)
            if srch:
                designation.set_weight(weight[0])
                break

        return designation

    def _set_value(self, designations: list[Designation]) -> list[Designation]:
        designations_with_weight = [d for d in designations if d.have_weight()]

        if not designations_with_weight:
            standard_weight = self.standard_weight
        else:
            standard_weight = designations_with_weight[-1].weight

        designations = [
            designation.set_standard_value(standard_weight)
            for designation in designations
        ]
        return designations

    def _standartization(self, value: str) -> set[Decimal]:
        designations = re.split(self._sep, value, re.IGNORECASE)
        designations = [Designation(dsgn) for dsgn in designations]
        designations = [self._set_weight(designation) for designation in designations]
        designations = self._set_value(designations)
        designations = frozenset([d.standard_value for d in designations])
        return designations

    def __eq__(self, other: AbstractFeature) -> bool:
        if isinstance(other, self.__class__):
            if self.standard_value == other.standard_value:
                return True
        return False

    def __hash__(self) -> int:
        return hash(self.standard_value)

    def __repr__(self) -> str:
        return "n-размерность = " + "x".join(
            list([str(v) for v in self.standard_value])
        )

    def __str__(self) -> str:
        return "n-размерность = " + "x".join(
            list([str(v) for v in self.standard_value])
        )

    @classmethod
    @property
    def units(self):
        return [self.NDIM]


class ComplexConcentration(AbstractFeature):
    NAME = "Complex Concentration"

    _num1 = r"\d*[.,]?\d+"
    _num2 = r"\d*[.,]?\d*"
    _sep = r"[\\\/]"
    _top = r"(?:мкг|мг|г|кг)?"
    _bot = r"(?:мл|л)"

    _tops = [
        (0.001, r"мкг"),
        (1, r"мг"),
        (1000, r"г"),
        (1000000, r"кг"),
    ]

    _bots = [
        (1, r"мл"),
        (1000, r"л"),
    ]

    Numeric_Concentration = FeatureUnit(
        "Complex Numeric Concentration",
        regex=rf"{_num1}\s*{_top}\s*{_sep}\s*{_num2}\s*{_bot}",
        weight=0.1,
    )

    Percent_Concentration = FeatureUnit(
        "Complex Percent Concentation",
        weight=1,
        regex=rf"{_num1}\s*%",
    )

    def __init__(
        self,
        value: str,
        unit: FeatureUnit,
    ) -> None:
        self.original_value = value
        self.standard_weight = 1

        if unit is self.Numeric_Concentration:
            self.standard_value = self._numerical_standartization(value)
        elif unit is self.Percent_Concentration:
            self.standard_value = self._percent_standartization(value)
        else:
            raise ValueError("Undetected unit type")

    def _num_standartization(self, value: str, weights: str) -> Decimal:
        num = re.search(self._num1, value)
        if num:
            num = num[0]
            num = num.replace(",", ".")
        else:
            num = "1"

        num = Decimal(num)

        weight = Decimal("1")
        for _weight in weights:
            if re.search(_weight[1], value):
                weight = _weight[0]
                break

        num = num * Decimal(str(weight))
        return num

    def _numerical_standartization(self, value: str) -> Decimal:
        top, bot = re.split(self._sep, value, re.IGNORECASE)
        top = self._num_standartization(top, self._tops)
        bot = self._num_standartization(bot, self._bots)
        standard = top / bot * self.Numeric_Concentration.weight
        return standard

    def _percent_standartization(self, value: str):
        standard = re.search(self._num1, value)[0]
        standard = standard.replace(",", ".")
        standard = Decimal(standard) * self.Percent_Concentration.weight
        return standard

    def __eq__(self, other: AbstractFeature) -> bool:
        if isinstance(other, self.__class__):
            if self.standard_value == other.standard_value:
                return True
        return False

    def __hash__(self) -> int:
        return hash(self.standard_value)

    def __repr__(self) -> str:
        return f"Concentration = {self.standard_value}"

    def __str__(self) -> str:
        return f"Concentration = {self.standard_value}"

    @classmethod
    @property
    def units(self) -> list[FeatureUnit]:
        return [
            self.Numeric_Concentration,
            self.Percent_Concentration,
        ]


COMPLEX_MAP = {
    ComplexDimension.NAME: ComplexDimension,
    ComplexConcentration.NAME: ComplexConcentration,
}
