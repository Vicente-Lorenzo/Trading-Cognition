from typing import Union
from pathlib import Path

from Library.Utility.Path import PathAPI
from Library.Utility.Typing import format

class FileAPI:
    def __init__(self, data: Union[str, Path, PathAPI], *, encoding: str = "utf-8"):
        if isinstance(data, PathAPI):
            self._data_: str = data.file.read_text(encoding=encoding)
        elif isinstance(data, Path):
            self._data_: str = data.read_text(encoding=encoding)
        else:
            self._data_: str = data

    def __call__(self, *args, **kwargs) -> str:
        return format(self._data_, *args, **kwargs)

    def __str__(self) -> str:
        return self._data_

    def __repr__(self) -> str:
        return repr(self._data_)