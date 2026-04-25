from typing import Union
import copy
import yaml

from pathlib import Path

class ParametersAPI:

    PATH = Path("Library") / Path("Parameters")
    
    def __init__(self, path: Union[Path, None] = None):
        self.path = ParametersAPI.PATH if not path else path
        self.path.mkdir(parents=True, exist_ok=True)

    def _resolve_path(self, *args):
        return self.path.joinpath(*args)

    def _get_file_path(self, name):
        return self._resolve_path(name).with_suffix(".yml")

    @staticmethod
    def _safe_load(file_path):
        with file_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    @staticmethod
    def _safe_dump(file_path, data):
        with file_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f)

    def _get_item(self, name):
        item_path = self._resolve_path(name)
        file_path = self._get_file_path(name)

        if item_path.is_dir():
            return ParametersAPI(item_path)
        elif file_path.is_file():
            return Parameters(self._safe_load(file_path), file_path)
        else:
            return ParametersAPI(item_path)

    def _set_item(self, name, value):
        item_path = self._resolve_path(name)
        file_path = self._get_file_path(name)

        if isinstance(value, (dict, ParametersAPI, Parameters)):
            item_path.mkdir(parents=True, exist_ok=True)
            if isinstance(value, (dict, Parameters)):
                self._safe_dump(file_path, value.data if isinstance(value, Parameters) else value)
        else:
            raise ValueError("Only dictionaries, ParametersAPI, or Parameters instances can be set directly.")

    def _delete_item(self, name):
        item_path = self._resolve_path(name)
        file_path = self._get_file_path(name)

        if item_path.is_dir():
            import shutil
            shutil.rmtree(item_path)
        elif file_path.is_file():
            file_path.unlink()
        else:
            raise KeyError(f"{name} does not exist.")

    def __getattr__(self, name):
        return self._get_item(name)

    def __getitem__(self, name):
        return self._get_item(name)

    def __setattr__(self, name, value):
        if name == "path":
            super().__setattr__(name, value)
        else:
            self._set_item(name, value)

    def __setitem__(self, name, value):
        self._set_item(name, value)

    def __delattr__(self, name):
        self._delete_item(name)

    def __delitem__(self, name):
        self._delete_item(name)

    def __repr__(self) -> str:
        return repr(f"ParametersAPI(path={self.path})")

class Parameters:
    def __init__(self, data, path, parent=None, parent_key=None):
        self.data = data
        self.path = Path(path)
        self.parent = parent
        self.parent_key = parent_key

    def __getattr__(self, key):
        if key in self.data:
            value = self.data[key]
            if isinstance(value, dict):
                return Parameters(value, self.path, parent=self, parent_key=key)
            return value
        else:
            return None

    def __getitem__(self, key):
        return self.__getattr__(key)

    def __setattr__(self, key, value):
        if key in ["data", "path", "parent", "parent_key"]:
            super().__setattr__(key, value)
        else:
            self.data[key] = value
            self.__save()

    def __setitem__(self, key, value):
        return self.__setattr__(key, value)

    def __delattr__(self, key):
        if key in self.data:
            del self.data[key]
            self.__save()
        else:
            raise KeyError(f"Key {key} not found.")

    def __delitem__(self, key):
        self.__delattr__(key)

    def __save(self):
        if self.parent:
            self.parent.data[self.parent_key] = self.data
            self.parent.__save()
        else:
            with self.path.open("w", encoding="utf-8") as f:
                yaml.safe_dump(self.data, f)

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()

    def clone(self):
        return Parameters(copy.copy(self.data), self.path, parent=self.parent, parent_key=self.parent_key)

    def __repr__(self) -> str:
        return repr(f"Parameters(path={self.path}, data={self.data})")
