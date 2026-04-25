from typing import Union
import numpy as np

from abc import ABC, abstractmethod

class NoiseAPI(ABC):

    def __init__(self,
                 seed: Union[int, None] = None):
        self._rng = np.random.default_rng(seed)

    @abstractmethod
    def __call__(self) -> np.ndarray:
        pass

    @abstractmethod
    def reset(self) -> None:
        pass
