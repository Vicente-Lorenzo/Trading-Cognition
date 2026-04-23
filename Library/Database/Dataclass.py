from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Type, Any, Self
from dataclasses import dataclass, field, InitVar

from Library.Utility.Typing import MISSING

def overridefield(func):
    func._overridefield_ = True
    return func

def coerce(value: Any) -> Any:
    return MISSING if isinstance(value, property) else value

class DatametaAPI:

    def __init__(self, cls: Type, name: str | None = None):
        self._cls_ = cls
        self._name_ = name

    def __getattr__(self, item: str) -> Any:
        attrs: dict[str, Any] = {
            k: v
            for base in reversed(self._cls_.mro())
            if base is not object
            for k, v in base.__dict__.items()
        }
        def get_attribute_type_(attr_name: str) -> type | None:
            ann = attrs.get("__annotations__", {})
            return ann.get(attr_name, None)
        def get_property_type_(prop_name: str) -> type | None:
            prop = attrs.get(prop_name, None)
            if isinstance(prop, property):
                return prop.fget.__annotations__.get("return", None)
            return None
        fs = attrs.get("__dataclass_fields__")
        if fs is not None and (f := fs.get(item, None)) is not None:
            if isinstance(f.type, InitVar):
                ft = get_property_type_(prop_name=item)
            elif item.startswith("_"):
                item_name = item[1:]
                if item_name.endswith("_"): item_name = item_name[:-1]
                ft = get_property_type_(prop_name=item_name)
            else:
                ft = f.type
            attr_val = self._name_ if self._name_ else item
            if isinstance(ft, type) and issubclass(ft, DataclassAPI):
                return DatametaAPI(cls=ft, name=attr_val)
            return attr_val
        if (a := attrs.get(item, None)) is not None:
            if isinstance(a, property):
                at = get_property_type_(item)
            else:
                at = get_attribute_type_(item)
            attr_val = self._name_ if self._name_ else item
            if isinstance(at, type) and issubclass(at, DataclassAPI):
                return DatametaAPI(cls=at, name=attr_val)
            return attr_val
        raise AttributeError(f"'{self._cls_.__name__}' object has no attribute '{item}'")

    def __repr__(self) -> str:
        return repr(self._name_ if self._name_ else self._cls_.__name__)

@dataclass
class DataclassAPI:

    UID: Any = field(default=MISSING, kw_only=True)

    def __init_subclass__(cls, **kwargs):
        super(DataclassAPI, cls).__init_subclass__(**kwargs)
        cls.ID = DatametaAPI(cls)

    def _parse_(self, name):
        f = getattr(self, name)
        if isinstance(f, Enum): return f.name
        if isinstance(f, DataclassAPI) and (uid := f.UID) is not MISSING: return uid
        return f

    @classmethod
    def initvars(cls) -> set[str]:
        return {name for name, f in cls.__dataclass_fields__.items()
                if f.init and getattr(f, "_field_type", None) != getattr(dataclasses, "_FIELD_CLASSVAR", None)}

    @classmethod
    def fields(cls) -> set[str]:
        return {name for name, f in cls.__dataclass_fields__.items()
                if getattr(f, "_field_type", None) != getattr(dataclasses, "_FIELD_CLASSVAR", None)}

    @classmethod
    def parse(cls, data: tuple | list | dict, **overrides) -> Self:
        if isinstance(data, dict):
            valid = cls.initvars()
            kwargs = {k: v for k, v in data.items() if k in valid}
            kwargs.update(overrides)
            return cls(**kwargs)
        return cls(*data, **overrides)

    def update(self, **kwargs) -> Self:
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        return self

    def data(self, include_fields, include_initvar_fields, include_hidden_fields, include_override_fields, include_properties):
        attrs = self.__class__.__dict__
        if include_fields:
            for f_name, f in attrs.get("__dataclass_fields__", {}).items():
                if getattr(f, "_field_type", None) == getattr(dataclasses, "_FIELD_CLASSVAR", None):
                    continue
                if include_initvar_fields and getattr(f, "_field_type", None) == getattr(dataclasses, "_FIELD_INITVAR", None):
                    yield f_name, self._parse_(f_name)
                elif getattr(f, "_field_type", None) != getattr(dataclasses, "_FIELD_INITVAR", None) and (include_hidden_fields or f.repr):
                    yield f_name, self._parse_(f_name)
        if include_override_fields or include_properties:
            for cls in reversed(type(self).mro()):
                if cls is object:
                    continue
                for attr_name, attr in cls.__dict__.items():
                    if isinstance(attr, property):
                        is_field = getattr(attr.fget, "_overridefield_", False)
                        if include_override_fields and is_field:
                            yield attr_name, self._parse_(attr_name)
                        if include_properties and not is_field:
                            yield attr_name, self._parse_(attr_name)

    def tuple(self, include_fields=True, include_initvar_fields=False, include_hidden_fields=False, include_override_fields=True, include_properties=False):
        return tuple([v for _, v in self.data(
            include_fields=include_fields,
            include_initvar_fields=include_initvar_fields,
            include_hidden_fields=include_hidden_fields,
            include_override_fields=include_override_fields,
            include_properties=include_properties
        )])

    def list(self, include_fields=True, include_initvar_fields=False, include_hidden_fields=False, include_override_fields=True, include_properties=False):
        return list([v for _, v in self.data(
            include_fields=include_fields,
            include_initvar_fields=include_initvar_fields,
            include_hidden_fields=include_hidden_fields,
            include_override_fields=include_override_fields,
            include_properties=include_properties
        )])

    def dict(self, include_fields=True, include_initvar_fields=False, include_hidden_fields=False, include_override_fields=True, include_properties=False):
        return dict({k: v for k, v in self.data(
            include_fields=include_fields,
            include_initvar_fields=include_initvar_fields,
            include_hidden_fields=include_hidden_fields,
            include_override_fields=include_override_fields,
            include_properties=include_properties
        )})