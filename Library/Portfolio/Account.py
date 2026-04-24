from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING
from enum import Enum
from dataclasses import dataclass, field, InitVar

from Library.Database.Dataframe import pl
from Library.Database.Database import PrimaryKey, ForeignKey, DatabaseAPI
from Library.Database.Datapoint import DatapointAPI
from Library.Database.Dataclass import overridefield, coerce
from Library.Database.Enumeration import as_enum
from Library.Portfolio.Portfolio import PortfolioAPI
from Library.Universe.Universe import UniverseAPI
from Library.Universe.Provider import ProviderAPI
from Library.Utility.Typing import MISSING

if TYPE_CHECKING: from Library.Database import DatabaseAPI

class AccountType(Enum):
    Hedged = 0
    Netted = 1

class MarginMode(Enum):
    Sum = 0
    Max = 1
    Net = 2

class Environment(Enum):
    Live = 0
    Demo = 1

@dataclass
class AccountAPI(DatapointAPI):

    Database: ClassVar[str] = DatapointAPI.Database
    Schema: ClassVar[str] = PortfolioAPI.Schema
    Table: ClassVar[str] = "Account"

    UID: str | None = None
    Environment: Environment | str | None = None
    AccountType: AccountType | str | None = None
    MarginMode: MarginMode | str | None = None
    Asset: str | None = None
    Balance: float | None = None
    Equity: float | None = None
    Credit: float | None = None
    Leverage: float | None = None
    MarginUsed: float | None = None
    MarginFree: float | None = None
    MarginLevel: float | None = None
    MarginStopLevel: float | None = None

    Provider: InitVar[str | ProviderAPI | None] = field(default=MISSING)

    _provider_: ProviderAPI | None = field(default=None, init=False, repr=False)

    @property
    def Key(self) -> dict:
        return {
            self.ID.UID: PrimaryKey(pl.String)
        }

    @property
    def Columns(self) -> dict:
        return {
            self.ID.Provider: ForeignKey(pl.String, reference=f'"{UniverseAPI.Schema}"."{ProviderAPI.Table}"("{ProviderAPI.ID.UID}")'),
            self.ID.Environment: pl.Enum([e.name for e in Environment]),
            self.ID.AccountType: pl.Enum([e.name for e in AccountType]),
            self.ID.MarginMode: pl.Enum([e.name for e in MarginMode]),
            self.ID.Asset: pl.String(),
            self.ID.Balance: pl.Float64(),
            self.ID.Equity: pl.Float64(),
            self.ID.Credit: pl.Float64(),
            self.ID.Leverage: pl.Float64(),
            self.ID.MarginUsed: pl.Float64(),
            self.ID.MarginFree: pl.Float64(),
            self.ID.MarginLevel: pl.Float64(),
            self.ID.MarginStopLevel: pl.Float64(),
            **super().Columns
        }

    def __post_init__(self,
                      db: DatabaseAPI | None,
                      migrate: bool,
                      autosave: bool,
                      autoload: bool,
                      autooverload: bool,
                      provider: str | ProviderAPI | None) -> None:
        provider = coerce(provider)
        self.Environment = as_enum(Environment, self.Environment)
        self.AccountType = as_enum(AccountType, self.AccountType)
        self.MarginMode = as_enum(MarginMode, self.MarginMode)
        if isinstance(provider, ProviderAPI): self._provider_ = provider
        elif provider is not MISSING and provider is not None:
            self._provider_ = ProviderAPI(UID=ProviderAPI.normalize(provider), db=db, migrate=migrate, autosave=autosave, autoload=autoload, autooverload=autooverload)
        super().__post_init__(db=db, migrate=migrate, autosave=autosave, autoload=autoload, autooverload=autooverload)

    def _pull_(self, overload: bool) -> dict | None:
        row = super()._pull_(overload=overload)
        if row:
            self.Environment = as_enum(Environment, self.Environment)
            self.AccountType = as_enum(AccountType, self.AccountType)
            self.MarginMode = as_enum(MarginMode, self.MarginMode)
        return row

    @property
    @overridefield
    def Provider(self) -> ProviderAPI | None:
        return self._provider_
    @Provider.setter
    def Provider(self, val: str | ProviderAPI | None) -> None:
        if isinstance(val, ProviderAPI): self._provider_ = val
        elif val is not None: self._provider_ = ProviderAPI(UID=ProviderAPI.normalize(val), db=self._db_, autoload=True)

    def __str__(self) -> str:
        return self.UID or ""

    @property
    def IsLive(self) -> bool:
        return self.Environment == Environment.Live
    @property
    def IsDemo(self) -> bool:
        return self.Environment == Environment.Demo
    @property
    def IsHedged(self) -> bool:
        return self.AccountType == AccountType.Hedged
    @property
    def IsNetted(self) -> bool:
        return self.AccountType == AccountType.Netted
    @property
    def UnrealizedPnL(self) -> float | None:
        if self.Equity is None or self.Balance is None: return None
        return self.Equity - self.Balance
    @property
    def UnrealizedReturn(self) -> float | None:
        upnl = self.UnrealizedPnL
        if upnl is None or not self.Balance: return None
        return upnl / self.Balance
    @property
    def MarginRatio(self) -> float | None:
        if not self.Equity or self.MarginUsed is None: return None
        return self.MarginUsed / self.Equity
    @property
    def FreeMarginRatio(self) -> float | None:
        if not self.Equity or self.MarginFree is None: return None
        return self.MarginFree / self.Equity
    @property
    def CreditRatio(self) -> float | None:
        if not self.Balance or self.Credit is None: return None
        return self.Credit / self.Balance