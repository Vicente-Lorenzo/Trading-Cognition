from __future__ import annotations

from datetime import datetime
from typing import ClassVar, TYPE_CHECKING, Any
from dataclasses import dataclass, field, InitVar

from Library.Database.Dataframe import pl
from Library.Database.Dataclass import DataclassAPI
from Library.Utility.Typing import MISSING

if TYPE_CHECKING: from Library.Database.Database import DatabaseAPI

@dataclass
class DatapointAPI(DataclassAPI):

    Database: ClassVar[str] = "Quant"
    Schema: ClassVar[str]
    Table: ClassVar[str]

    CreatedAt: datetime | None = field(default=None, kw_only=True)
    CreatedBy: str | None = field(default=None, kw_only=True)
    UpdatedAt: datetime | None = field(default=None, kw_only=True)
    UpdatedBy: str | None = field(default=None, kw_only=True)

    db: InitVar[DatabaseAPI | None] = field(default=None, kw_only=True)
    migrate: InitVar[bool] = field(default=False, kw_only=True)
    autosave: InitVar[bool] = field(default=False, kw_only=True)
    autoload: InitVar[bool] = field(default=False, kw_only=True)
    autooverload: InitVar[bool] = field(default=False, kw_only=True)

    _db_: DatabaseAPI | None = field(default=None, init=False, repr=False)
    _migrate_: bool = field(default=False, init=False, repr=False)
    _save_: bool = field(default=False, init=False, repr=False)
    _load_: bool = field(default=False, init=False, repr=False)
    _overload_: bool = field(default=False, init=False, repr=False)

    @property
    def Key(self) -> dict:
        return {}

    @property
    def Columns(self) -> dict:
        return {
            self.ID.CreatedAt: pl.Datetime(),
            self.ID.CreatedBy: pl.String(),
            self.ID.UpdatedAt: pl.Datetime(),
            self.ID.UpdatedBy: pl.String()
        }

    @property
    def Structure(self) -> dict:
        return {
            **self.Key,
            **self.Columns
        }

    def __post_init__(self,
                      db: DatabaseAPI | None,
                      migrate: bool,
                      autosave: bool,
                      autoload: bool,
                      autooverload: bool) -> None:
        self._db_, self._migrate_, self._save_, self._load_, self._overload_ = db, migrate, autosave, autoload, autooverload
        if self._db_ is not None:
            if self._migrate_: self._db_.migrate(schema=self.Schema, table=self.Table, structure=self.Structure)
            if self._overload_: self.overload()
            elif self._load_: self.load()

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if getattr(self, "_save_", False) and name and name[0].isupper():
            try: self.save(by="Autosave")
            except Exception: pass

    def _push_(self, by: str) -> None:
        if self._db_ is None: return
        now = datetime.now()
        save_state = getattr(self, "_save_", False)
        try:
            self._save_ = False
            if not self.CreatedBy: self.CreatedBy, self.CreatedAt = by, now
            self.UpdatedBy, self.UpdatedAt = by, now
            data = {k: v for k, v in self.dict(include_fields=True, include_initvar_fields=False, include_properties=False, include_override_fields=True).items() if v is not None and v is not MISSING and k[0].isupper()}
            keys = self.Key
            key_names = list(keys.keys())
            from Library.Database.Database import IdentityKey
            is_identity = any(isinstance(v, IdentityKey) for v in keys.values())
            has_keys = all(data.get(k) is not None for k in key_names)
            if is_identity and has_keys:
                condition = " AND ".join([f'"{k}" = :{k}:' for k in key_names])
                update_data = {k: v for k, v in data.items() if k not in key_names and k not in ["CreatedAt", "CreatedBy"]}
                if update_data:
                    from Library.Database.Query import QueryAPI
                    sql = f"UPDATE {self._db_._target_(self.Schema, self.Table)} SET "
                    sql += ", ".join([f'"{k}" = :{k}:' for k in update_data.keys()])
                    sql += f" WHERE {condition}"
                    params = {**update_data, **{k: data[k] for k in key_names}}
                    self._db_.execute(QueryAPI(sql), [params])
            else:
                self._db_.upsert(schema=self.Schema, table=self.Table, data=data, key=key_names, exclude=["CreatedAt", "CreatedBy"])
        finally:
            self._save_ = save_state

    def save(self, by: str = "Autosave") -> None:
        self._push_(by=by)

    def _fetch_(self, condition: str, parameters: dict, overload: bool) -> dict | None:
        if self._db_ is None: return None
        df = self._db_.select(schema=self.Schema, table=self.Table, condition=condition, parameters=parameters, limit=1, legacy=False)
        if df.is_empty(): return None
        row = df.row(0, named=True)
        save_state, self._save_ = self._save_, False
        try:
            for k, v in row.items():
                if hasattr(self, k) and v is not None:
                    if overload or getattr(self, k) is None or getattr(self, k) is MISSING:
                        setattr(self, k, v)
        finally: self._save_ = save_state
        return row

    def _pull_(self, overload: bool) -> dict | None:
        if self._db_ is None: return None
        keys, data_dict = self.Key, self.dict(include_fields=True, include_initvar_fields=False, include_override_fields=True, include_properties=False)
        params = {k: data_dict.get(k) for k in keys}
        if any(v is None or v is MISSING for v in params.values()): return None
        return self._fetch_(condition=" AND ".join([f'"{k}" = :{k}:' for k in keys]), parameters=params, overload=overload)

    def overload(self) -> dict | None:
        return self._pull_(overload=True)

    def load(self) -> dict | None:
        return self._pull_(overload=False)