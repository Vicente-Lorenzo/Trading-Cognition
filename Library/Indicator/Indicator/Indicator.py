from typing import Union
from abc import ABC, abstractmethod

from Library.Database.Dataframe import pl

class IndicatorAPI(ABC):
    
    def __init__(self, shift: int = 1):
        self._shift: int = shift
        self._data: Union[pl.DataFrame, None] = None

    def data(self) -> pl.DataFrame:
        return self._data

    def head(self, n: Union[int, None] = None) -> pl.DataFrame:
        return self._data.head(n)

    def tail(self, n: Union[int, None] = None) -> pl.DataFrame:
        return self._data.tail(n)

    def last(self, n: int = 0) -> pl.DataFrame:
        return self.data()[-(self._shift + n)]

    @abstractmethod
    def initialize(self, *series):
        pass

    @abstractmethod
    def calculate(self):
        pass

    def __repr__(self) -> str:
        return repr(self.data())