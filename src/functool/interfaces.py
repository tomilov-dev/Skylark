from abc import ABC, abstractmethod


class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass
