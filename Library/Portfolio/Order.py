from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING
from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field, InitVar

from Library.Database.Dataframe import pl
from Library.Database.Database import IdentityKey, ForeignKey, DatabaseAPI
from Library.Database.Datapoint import DatapointAPI
from Library.Database.Dataclass import overridefield, coerce
from Library.Database.Enumeration import as_enum
from Library.Portfolio.Portfolio import PortfolioAPI
from Library.Portfolio.Position import PositionAPI
from Library.Universe.Universe import UniverseAPI
from Library.Market.Timestamp import TimestampAPI
from Library.Market.Price import PriceAPI, TradeType
from Library.Utility.Typing import MISSING

if TYPE_CHECKING:
    from Library.Universe.Security import SecurityAPI
    from Library.Universe.Contract import ContractAPI

class OrderType(Enum):
    Market = 1
    Limit = 2
    Stop = 3
    StopLossTakeProfit = 4
    MarketRange = 5
    StopLimit = 6

class OrderStatus(Enum):
    Accepted = 1
    Filled = 2
    Rejected = 3
    Expired = 4
    Cancelled = 5

class TimeInForce(Enum):
    GoodTillDate = 1
    GoodTillCancel = 2
    ImmediateOrCancel = 3
    FillOrKill = 4
    MarketOnOpen = 5

@dataclass
class OrderAPI(DatapointAPI):

    Database: ClassVar[str] = DatapointAPI.Database
    Schema: ClassVar[str] = PortfolioAPI.Schema
    Table: ClassVar[str] = "Order"

    UID: int | None = None
    TradeType: TradeType | str | None = None
    OrderType: OrderType | str | None = None
    OrderStatus: OrderStatus | str | None = None
    TimeInForce: TimeInForce | str | None = None
    Volume: float | None = None
    ExecutedVolume: float | None = None
    RelativeStopLoss: float | None = None
    RelativeTakeProfit: float | None = None
    SlippageInPoints: int | None = None
    ClosingOrder: bool | None = None
    ClientOrderID: str | None = None
    IsStopOut: bool | None = None
    TrailingStopLoss: bool | None = None
    StopTriggerMethod: int | None = None
    Label: str | None = None
    Comment: str | None = None

    Security: InitVar[int | SecurityAPI | None] = field(default=MISSING)
    Position: InitVar[int | PositionAPI | None] = field(default=MISSING)

    ExecutionPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    LimitPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    StopPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    StopLossPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    TakeProfitPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    BaseSlippagePrice: InitVar[float | PriceAPI | None] = field(default=MISSING)

    EntryTimestamp: InitVar[datetime | TimestampAPI | None] = field(default=MISSING)
    ExpirationTimestamp: InitVar[datetime | TimestampAPI | None] = field(default=MISSING)
    LastUpdateTimestamp: InitVar[datetime | TimestampAPI | None] = field(default=MISSING)

    Contract: InitVar[ContractAPI | None] = field(default=MISSING)

    _security_: SecurityAPI | None = field(default=None, init=False, repr=False)
    _position_: PositionAPI | None = field(default=None, init=False, repr=False)
    _execution_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _limit_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _stop_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _stop_loss_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _take_profit_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _base_slippage_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _entry_timestamp_: TimestampAPI | None = field(default=None, init=False, repr=False)
    _expiration_timestamp_: TimestampAPI | None = field(default=None, init=False, repr=False)
    _last_update_timestamp_: TimestampAPI | None = field(default=None, init=False, repr=False)
    _contract_: ContractAPI | None = field(default=None, init=False, repr=False)

    @property
    def Structure(self) -> dict:
        from Library.Universe.Security import SecurityAPI
        return {
            self.ID.UID: IdentityKey(pl.Int64),
            self.ID.Security: ForeignKey(pl.Int64, reference=f'"{UniverseAPI.Schema}"."{SecurityAPI.Table}"("{SecurityAPI.ID.UID}")'),
            self.ID.Position: ForeignKey(pl.Int64, reference=f'"{PortfolioAPI.Schema}"."{PositionAPI.Table}"("{PositionAPI.ID.UID}")'),
            self.ID.TradeType: pl.Enum([e.name for e in TradeType]),
            self.ID.OrderType: pl.Enum([e.name for e in OrderType]),
            self.ID.OrderStatus: pl.Enum([e.name for e in OrderStatus]),
            self.ID.TimeInForce: pl.Enum([e.name for e in TimeInForce]),
            self.ID.Volume: pl.Float64(),
            self.ID.ExecutedVolume: pl.Float64(),
            self.ID.ExecutionPrice: pl.Float64(),
            self.ID.LimitPrice: pl.Float64(),
            self.ID.StopPrice: pl.Float64(),
            self.ID.StopLossPrice: pl.Float64(),
            self.ID.TakeProfitPrice: pl.Float64(),
            self.ID.RelativeStopLoss: pl.Float64(),
            self.ID.RelativeTakeProfit: pl.Float64(),
            self.ID.BaseSlippagePrice: pl.Float64(),
            self.ID.SlippageInPoints: pl.Int32(),
            self.ID.ClosingOrder: pl.Boolean(),
            self.ID.ClientOrderID: pl.String(),
            self.ID.IsStopOut: pl.Boolean(),
            self.ID.TrailingStopLoss: pl.Boolean(),
            self.ID.StopTriggerMethod: pl.Int32(),
            self.ID.EntryTimestamp: pl.Datetime(),
            self.ID.ExpirationTimestamp: pl.Datetime(),
            self.ID.LastUpdateTimestamp: pl.Datetime(),
            self.ID.Label: pl.String(),
            self.ID.Comment: pl.String(),
            **super().Structure
        }

    def __post_init__(self,
                      db: DatabaseAPI | None,
                      migrate: bool,
                      autosave: bool,
                      autoload: bool,
                      autooverload: bool,
                      security: int | SecurityAPI | None,
                      position: int | PositionAPI | None,
                      execution_price: float | PriceAPI | None,
                      limit_price: float | PriceAPI | None,
                      stop_price: float | PriceAPI | None,
                      stop_loss_price: float | PriceAPI | None,
                      take_profit_price: float | PriceAPI | None,
                      base_slippage_price: float | PriceAPI | None,
                      entry_timestamp: datetime | TimestampAPI | None,
                      expiration_timestamp: datetime | TimestampAPI | None,
                      last_update_timestamp: datetime | TimestampAPI | None,
                      contract: ContractAPI | None) -> None:
        from Library.Universe.Security import SecurityAPI
        security = coerce(security)
        position = coerce(position)
        execution_price = coerce(execution_price)
        limit_price = coerce(limit_price)
        stop_price = coerce(stop_price)
        stop_loss_price = coerce(stop_loss_price)
        take_profit_price = coerce(take_profit_price)
        base_slippage_price = coerce(base_slippage_price)
        entry_timestamp = coerce(entry_timestamp)
        expiration_timestamp = coerce(expiration_timestamp)
        last_update_timestamp = coerce(last_update_timestamp)
        contract = coerce(contract)
        self.TradeType = as_enum(TradeType, self.TradeType)
        self.OrderType = as_enum(OrderType, self.OrderType)
        self.OrderStatus = as_enum(OrderStatus, self.OrderStatus)
        self.TimeInForce = as_enum(TimeInForce, self.TimeInForce)
        if isinstance(security, SecurityAPI): self._security_ = security
        elif security is not MISSING and security is not None:
            self._security_ = SecurityAPI(UID=security, db=db, migrate=migrate, autosave=autosave, autoload=autoload, autooverload=autooverload)
        if isinstance(position, PositionAPI): self._position_ = position
        elif position is not MISSING and position is not None:
            self._position_ = PositionAPI(UID=position, db=db, migrate=migrate, autosave=autosave, autoload=autoload, autooverload=autooverload)
        if contract is not MISSING: self._contract_ = contract
        ep = self._unwrap_price_(execution_price)
        self._execution_price_ = self._make_price_(execution_price, reference=ep)
        self._limit_price_ = self._make_price_(limit_price, reference=ep)
        self._stop_price_ = self._make_price_(stop_price, reference=ep)
        self._stop_loss_price_ = self._make_price_(stop_loss_price, reference=ep)
        self._take_profit_price_ = self._make_price_(take_profit_price, reference=ep)
        self._base_slippage_price_ = self._make_price_(base_slippage_price, reference=ep)
        self._entry_timestamp_ = self._make_timestamp_(entry_timestamp)
        self._expiration_timestamp_ = self._make_timestamp_(expiration_timestamp)
        self._last_update_timestamp_ = self._make_timestamp_(last_update_timestamp)
        super().__post_init__(db=db, migrate=migrate, autosave=autosave, autoload=autoload, autooverload=autooverload)

    def _pull_(self, overload: bool) -> dict | None:
        row = super()._pull_(overload=overload)
        if row:
            self.TradeType = as_enum(TradeType, self.TradeType)
            self.OrderType = as_enum(OrderType, self.OrderType)
            self.OrderStatus = as_enum(OrderStatus, self.OrderStatus)
            self.TimeInForce = as_enum(TimeInForce, self.TimeInForce)
        return row

    @staticmethod
    def _unwrap_price_(val: float | PriceAPI | None) -> float | None:
        if isinstance(val, PriceAPI): return val.Price
        return val if val is not MISSING else None

    def _make_price_(self, val: float | PriceAPI | None, reference: float | None) -> PriceAPI | None:
        if isinstance(val, PriceAPI):
            if val.Contract is None: val.Contract = self._contract_
            if val.Reference is None: val.Reference = reference
            return val
        if val is MISSING or val is None: return None
        return PriceAPI(Price=val, Reference=reference, Contract=self._contract_)

    @staticmethod
    def _make_timestamp_(val: datetime | TimestampAPI | None) -> TimestampAPI | None:
        if isinstance(val, TimestampAPI): return val
        if val is MISSING or val is None: return None
        return TimestampAPI(DateTime=val)

    def _assign_price_(self, backing: PriceAPI | None, val: float | PriceAPI | None) -> PriceAPI | None:
        if isinstance(val, PriceAPI): return val
        if val is None: return backing
        if backing:
            backing.Price = val
            return backing
        ref = self._execution_price_.Price if self._execution_price_ else val
        return PriceAPI(Price=val, Reference=ref, Contract=self._contract_)

    @staticmethod
    def _assign_timestamp_(backing: TimestampAPI | None, val: datetime | TimestampAPI | None) -> TimestampAPI | None:
        if isinstance(val, TimestampAPI): return val
        if val is None: return backing
        if backing:
            backing.DateTime = val
            return backing
        return TimestampAPI(DateTime=val)

    @property
    @overridefield
    def Security(self) -> SecurityAPI | None:
        return self._security_
    @Security.setter
    def Security(self, val: int | SecurityAPI | None) -> None:
        from Library.Universe.Security import SecurityAPI
        if isinstance(val, SecurityAPI): self._security_ = val
        elif val is not None: self._security_ = SecurityAPI(UID=val, db=self._db_, autoload=True)

    @property
    @overridefield
    def Position(self) -> PositionAPI | None:
        return self._position_
    @Position.setter
    def Position(self, val: int | PositionAPI | None) -> None:
        if isinstance(val, PositionAPI): self._position_ = val
        elif val is not None: self._position_ = PositionAPI(UID=val, db=self._db_, autoload=True)

    @property
    @overridefield
    def ExecutionPrice(self) -> PriceAPI | None:
        return self._execution_price_
    @ExecutionPrice.setter
    def ExecutionPrice(self, val: float | PriceAPI | None) -> None:
        price = val.Price if isinstance(val, PriceAPI) else val
        if price is None: return
        if self._execution_price_:
            self._execution_price_.Price = price
            self._execution_price_.Reference = price
        else:
            self._execution_price_ = PriceAPI(Price=price, Reference=price, Contract=self._contract_)
        for backing in (self._limit_price_, self._stop_price_, self._stop_loss_price_, self._take_profit_price_, self._base_slippage_price_):
            if backing: backing.Reference = price

    @property
    @overridefield
    def LimitPrice(self) -> PriceAPI | None:
        return self._limit_price_
    @LimitPrice.setter
    def LimitPrice(self, val: float | PriceAPI | None) -> None:
        self._limit_price_ = self._assign_price_(self._limit_price_, val)

    @property
    @overridefield
    def StopPrice(self) -> PriceAPI | None:
        return self._stop_price_
    @StopPrice.setter
    def StopPrice(self, val: float | PriceAPI | None) -> None:
        self._stop_price_ = self._assign_price_(self._stop_price_, val)

    @property
    @overridefield
    def StopLossPrice(self) -> PriceAPI | None:
        return self._stop_loss_price_
    @StopLossPrice.setter
    def StopLossPrice(self, val: float | PriceAPI | None) -> None:
        self._stop_loss_price_ = self._assign_price_(self._stop_loss_price_, val)

    @property
    @overridefield
    def TakeProfitPrice(self) -> PriceAPI | None:
        return self._take_profit_price_
    @TakeProfitPrice.setter
    def TakeProfitPrice(self, val: float | PriceAPI | None) -> None:
        self._take_profit_price_ = self._assign_price_(self._take_profit_price_, val)

    @property
    @overridefield
    def BaseSlippagePrice(self) -> PriceAPI | None:
        return self._base_slippage_price_
    @BaseSlippagePrice.setter
    def BaseSlippagePrice(self, val: float | PriceAPI | None) -> None:
        self._base_slippage_price_ = self._assign_price_(self._base_slippage_price_, val)

    @property
    @overridefield
    def EntryTimestamp(self) -> TimestampAPI | None:
        return self._entry_timestamp_
    @EntryTimestamp.setter
    def EntryTimestamp(self, val: datetime | TimestampAPI | None) -> None:
        self._entry_timestamp_ = self._assign_timestamp_(self._entry_timestamp_, val)

    @property
    @overridefield
    def ExpirationTimestamp(self) -> TimestampAPI | None:
        return self._expiration_timestamp_
    @ExpirationTimestamp.setter
    def ExpirationTimestamp(self, val: datetime | TimestampAPI | None) -> None:
        self._expiration_timestamp_ = self._assign_timestamp_(self._expiration_timestamp_, val)

    @property
    @overridefield
    def LastUpdateTimestamp(self) -> TimestampAPI | None:
        return self._last_update_timestamp_
    @LastUpdateTimestamp.setter
    def LastUpdateTimestamp(self, val: datetime | TimestampAPI | None) -> None:
        self._last_update_timestamp_ = self._assign_timestamp_(self._last_update_timestamp_, val)

    @property
    def Direction(self) -> TradeType | None:
        return self.TradeType if isinstance(self.TradeType, TradeType) else None
    @property
    def IsBuy(self) -> bool:
        return self.TradeType == TradeType.Buy
    @property
    def IsSell(self) -> bool:
        return self.TradeType == TradeType.Sell
    @property
    def IsFilled(self) -> bool:
        return self.OrderStatus == OrderStatus.Filled
    @property
    def IsAccepted(self) -> bool:
        return self.OrderStatus == OrderStatus.Accepted
    @property
    def IsRejected(self) -> bool:
        return self.OrderStatus == OrderStatus.Rejected
    @property
    def IsCancelled(self) -> bool:
        return self.OrderStatus == OrderStatus.Cancelled
    @property
    def IsExpired(self) -> bool:
        return self.OrderStatus == OrderStatus.Expired
    @property
    def ExecutionRatio(self) -> float | None:
        if not self.Volume or self.ExecutedVolume is None: return None
        return self.ExecutedVolume / self.Volume
    @property
    def RiskAmount(self) -> float | None:
        if self._execution_price_ and self._stop_loss_price_ and self.Volume:
            if self.Contract and self.Contract.LotSize:
                return abs(self._execution_price_.Price - self._stop_loss_price_.Price) * self.Volume * self.Contract.LotSize
        return None

    @property
    def RewardAmount(self) -> float | None:
        if self._execution_price_ and self._take_profit_price_ and self.Volume:
            if self.Contract and self.Contract.LotSize:
                return abs(self._take_profit_price_.Price - self._execution_price_.Price) * self.Volume * self.Contract.LotSize
        return None

    @property
    def UnfilledVolume(self) -> float | None:
        if self.Volume is None or self.ExecutedVolume is None: return None
        return self.Volume - self.ExecutedVolume

    @property
    @overridefield
    def Contract(self) -> ContractAPI | None:
        return self._contract_
    @Contract.setter
    def Contract(self, val: ContractAPI | None) -> None:
        self._contract_ = val
        for backing in (self._execution_price_, self._limit_price_, self._stop_price_, self._stop_loss_price_, self._take_profit_price_, self._base_slippage_price_):
            if backing: backing.Contract = self._contract_