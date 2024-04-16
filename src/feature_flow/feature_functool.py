from abc import ABC, abstractmethod
from decimal import Decimal


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


class FeatureUnit(object):
    def __init__(
        self,
        name: str,
        regex: str,
        weight: int,
    ) -> None:
        self.name = name
        self.regex = regex
        self.weight = Decimal(str(weight))

    def __repr__(self) -> str:
        return f"{self.name} with weight {self.weight}"


class AbstractFeature(ABC):
    NAME = ""
    VALIDATION_MODE: FeatureValidationMode
    NOT_FOUND_MODE: FeatureNotFoundMode
    PRIORITY: int
    UNITS: list[FeatureUnit] = []

    @abstractmethod
    def __init__(
        self,
        value: str,
        unit: FeatureUnit,
    ) -> None:
        pass

    @classmethod
    @property
    def units(self) -> list[FeatureUnit]:
        return self.UNITS


class NotFoundStatus(object):
    ACCEPT = "accept"
    DROP = "drop"

    def __init__(
        self,
        feature_set1: set,
        feature_set2: set,
        not_found_mode: FeatureNotFoundMode,
        feature_name: str,
    ):
        self.empty1 = self._is_empty(feature_set1)
        self.empty2 = self._is_empty(feature_set2)
        self.not_found_mode = not_found_mode
        self.feature_name = feature_name

        self.both_not_found = True if self.empty1 and self.empty2 else False
        self.one_not_found = True if self.empty1 or self.empty2 else False

    def _is_empty(self, feature_set: set) -> bool:
        if len(feature_set) == 0:
            return True
        return False

    def set_decision(self, not_found_mode) -> int:
        if not_found_mode == FeatureNotFoundMode.MODEST:
            self.desicion = 1
        else:  # not_found_mode == FeatureNotFoundMode.STRICT
            self.desicion = 0

    @property
    def desicion(self) -> int:
        if self.both_not_found:
            return 1
        elif self.one_not_found:
            if self.not_found_mode == self.ACCEPT:
                return 1
            else:  # self.not_found_mode == self.DROP:
                return 0

    @property
    def status(self) -> str:
        if self.both_not_found:
            return f"Not found both {self.feature_name}: decision {self.desicion}; "
        elif self.one_not_found:
            which_not_found = "client" if self.empty1 else "source"
            return f"Not found {which_not_found} {self.feature_name}: desicion {self.desicion}; "

    def __bool__(self) -> bool:
        return self.one_not_found


class FeatureList(object):
    def __init__(
        self,
        feature_list: list[AbstractFeature] = [],
    ) -> None:
        self.feature_list = self.sort_futures(feature_list)
        self.lenght = len(feature_list)

    def sort_futures(
        self, feature_list: list[AbstractFeature]
    ) -> list[AbstractFeature]:
        features = [(feature, feature.PRIORITY) for feature in feature_list]
        features = sorted(features, key=lambda feature: feature[1])
        features = list(map(lambda feature: feature[0], features))
        return features

    def __len__(self) -> int:
        return self.lenght

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        if self._index < len(self.feature_list):
            future = self.feature_list[self._index]
            self._index += 1
            return future
        else:
            raise StopIteration

    def __repr__(self) -> str:
        return f"{self.feature_list}"
