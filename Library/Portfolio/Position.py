from __future__ import annotations

from enum import Enum
from datetime import datetime
from typing import ClassVar, TYPE_CHECKING
from dataclasses import dataclass, field, InitVar

from Library.Database.Dataframe import pl
from Library.Database.Database import IdentityKey, ForeignKey, DatabaseAPI
from Library.Database.Datapoint import DatapointAPI
from Library.Database.Dataclass import overridefield
from Library.Database.Enumeration import as_enum
from Library.Portfolio.Portfolio import PortfolioAPI
from Library.Portfolio.PnL import PnLAPI
from Library.Universe.Universe import UniverseAPI
from Library.Market.Timestamp import TimestampAPI
from Library.Market.Price import PriceAPI, TradeType
from Library.Utility.Typing import MISSING

if TYPE_CHECKING:
    from Library.Universe.Security import SecurityAPI
    from Library.Universe.Contract import ContractAPI

class PositionType(Enum):
    Normal = 0
    Continuation = 1

@dataclass
class PositionAPI(DatapointAPI):

    Database: ClassVar[str] = DatapointAPI.Database
    Schema: ClassVar[str] = PortfolioAPI.Schema
    Table: ClassVar[str] = "Position"

    UID: int | None = None
    PositionType: PositionType | str | None = None
    TradeType: TradeType | str | None = None
    Volume: float | None = None
    Quantity: float | None = None
    UsedMargin: float | None = None
    MidBalance: float | None = None

    Security: InitVar[int | SecurityAPI | None] = field(default=MISSING)

    EntryTimestamp: InitVar[datetime | TimestampAPI | None] = field(default=MISSING)
    EntryPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    StopLossPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    TakeProfitPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    MaxRunUpPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    MaxDrawDownPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)
    ExitPrice: InitVar[float | PriceAPI | None] = field(default=MISSING)

    StopLossPnL: InitVar[float | PnLAPI | None] = field(default=MISSING)
    TakeProfitPnL: InitVar[float | PnLAPI | None] = field(default=MISSING)
    MaxRunUpPnL: InitVar[float | PnLAPI | None] = field(default=MISSING)
    MaxDrawDownPnL: InitVar[float | PnLAPI | None] = field(default=MISSING)
    GrossPnL: InitVar[float | PnLAPI | None] = field(default=MISSING)
    CommissionPnL: InitVar[float | PnLAPI | None] = field(default=MISSING)
    SwapPnL: InitVar[float | PnLAPI | None] = field(default=MISSING)
    NetPnL: InitVar[float | PnLAPI | None] = field(default=MISSING)

    EntryBalance: InitVar[float | None] = field(default=MISSING)

    Contract: InitVar[ContractAPI | None] = field(default=MISSING)

    _security_: SecurityAPI | None = field(default=None, init=False, repr=False)
    _entry_timestamp_: TimestampAPI | None = field(default=None, init=False, repr=False)
    _entry_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _stop_loss_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _take_profit_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _max_run_up_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _max_draw_down_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _exit_price_: PriceAPI | None = field(default=None, init=False, repr=False)
    _stop_loss_pnl_: PnLAPI | None = field(default=None, init=False, repr=False)
    _take_profit_pnl_: PnLAPI | None = field(default=None, init=False, repr=False)
    _max_run_up_pnl_: PnLAPI | None = field(default=None, init=False, repr=False)
    _max_draw_down_pnl_: PnLAPI | None = field(default=None, init=False, repr=False)
    _gross_pnl_: PnLAPI | None = field(default=None, init=False, repr=False)
    _commission_pnl_: PnLAPI | None = field(default=None, init=False, repr=False)
    _swap_pnl_: PnLAPI | None = field(default=None, init=False, repr=False)
    _net_pnl_: PnLAPI | None = field(default=None, init=False, repr=False)
    _entry_balance_: float | None = field(default=None, init=False, repr=False)
    _contract_: ContractAPI | None = field(default=None, init=False, repr=False)

    @property
    def Structure(self) -> dict:
        from Library.Universe.Security import SecurityAPI
        return {
            self.ID.UID: IdentityKey(pl.Int64),
            self.ID.Security: ForeignKey(pl.Int64, reference=f'"{UniverseAPI.Schema}"."{SecurityAPI.Table}"("{SecurityAPI.ID.UID}")'),
            self.ID.PositionType: pl.Enum([e.name for e in PositionType]),
            self.ID.TradeType: pl.Enum([e.name for e in TradeType]),
            self.ID.Volume: pl.Float64(),
            self.ID.Quantity: pl.Float64(),
            self.ID.EntryTimestamp: pl.Datetime(),
            self.ID.EntryPrice: pl.Float64(),
            self.ID.StopLossPrice: pl.Float64(),
            self.ID.TakeProfitPrice: pl.Float64(),
            self.ID.MaxRunUpPrice: pl.Float64(),
            self.ID.MaxDrawDownPrice: pl.Float64(),
            self.ID.ExitPrice: pl.Float64(),
            self.ID.StopLossPnL: pl.Float64(),
            self.ID.TakeProfitPnL: pl.Float64(),
            self.ID.MaxRunUpPnL: pl.Float64(),
            self.ID.MaxDrawDownPnL: pl.Float64(),
            self.ID.GrossPnL: pl.Float64(),
            self.ID.CommissionPnL: pl.Float64(),
            self.ID.SwapPnL: pl.Float64(),
            self.ID.NetPnL: pl.Float64(),
            self.ID.UsedMargin: pl.Float64(),
            self.ID.EntryBalance: pl.Float64(),
            self.ID.MidBalance: pl.Float64(),
            **super().Structure
        }

    def __post_init__(self,
                      db: DatabaseAPI | None,
                      migrate: bool,
                      autosave: bool,
                      autoload: bool,
                      autooverload: bool,
                      security: int | SecurityAPI | None,
                      entry_timestamp: datetime | TimestampAPI | None,
                      entry_price: float | PriceAPI | None,
                      stop_loss_price: float | PriceAPI | None,
                      take_profit_price: float | PriceAPI | None,
                      max_run_up_price: float | PriceAPI | None,
                      max_draw_down_price: float | PriceAPI | None,
                      exit_price: float | PriceAPI | None,
                      stop_loss_pnl: float | PnLAPI | None,
                      take_profit_pnl: float | PnLAPI | None,
                      max_run_up_pnl: float | PnLAPI | None,
                      max_draw_down_pnl: float | PnLAPI | None,
                      gross_pnl: float | PnLAPI | None,
                      commission_pnl: float | PnLAPI | None,
                      swap_pnl: float | PnLAPI | None,
                      net_pnl: float | PnLAPI | None,
                      entry_balance: float | None,
                      contract: ContractAPI | None) -> None:
        from Library.Universe.Security import SecurityAPI
        security = MISSING if isinstance(security, property) else security
        entry_timestamp = MISSING if isinstance(entry_timestamp, property) else entry_timestamp
        entry_price = MISSING if isinstance(entry_price, property) else entry_price
        stop_loss_price = MISSING if isinstance(stop_loss_price, property) else stop_loss_price
        take_profit_price = MISSING if isinstance(take_profit_price, property) else take_profit_price
        max_run_up_price = MISSING if isinstance(max_run_up_price, property) else max_run_up_price
        max_draw_down_price = MISSING if isinstance(max_draw_down_price, property) else max_draw_down_price
        exit_price = MISSING if isinstance(exit_price, property) else exit_price
        stop_loss_pnl = MISSING if isinstance(stop_loss_pnl, property) else stop_loss_pnl
        take_profit_pnl = MISSING if isinstance(take_profit_pnl, property) else take_profit_pnl
        max_run_up_pnl = MISSING if isinstance(max_run_up_pnl, property) else max_run_up_pnl
        max_draw_down_pnl = MISSING if isinstance(max_draw_down_pnl, property) else max_draw_down_pnl
        gross_pnl = MISSING if isinstance(gross_pnl, property) else gross_pnl
        commission_pnl = MISSING if isinstance(commission_pnl, property) else commission_pnl
        swap_pnl = MISSING if isinstance(swap_pnl, property) else swap_pnl
        net_pnl = MISSING if isinstance(net_pnl, property) else net_pnl
        entry_balance = MISSING if isinstance(entry_balance, property) else entry_balance
        contract = MISSING if isinstance(contract, property) else contract
        self.PositionType = as_enum(PositionType, self.PositionType)
        self.TradeType = as_enum(TradeType, self.TradeType)
        if isinstance(security, SecurityAPI): self._security_ = security
        elif security is not MISSING and security is not None:
            self._security_ = SecurityAPI(UID=security, db=db, autoload=True)
        if contract is not MISSING: self._contract_ = contract
        self._entry_balance_ = entry_balance if entry_balance is not MISSING else None
        if isinstance(entry_timestamp, TimestampAPI): self._entry_timestamp_ = entry_timestamp
        elif entry_timestamp is not MISSING and entry_timestamp is not None:
            self._entry_timestamp_ = TimestampAPI(DateTime=entry_timestamp)
        ep = self._unwrap_price_(entry_price)
        self._entry_price_ = self._make_price_(entry_price, reference=ep)
        self._stop_loss_price_ = self._make_price_(stop_loss_price, reference=ep)
        self._take_profit_price_ = self._make_price_(take_profit_price, reference=ep)
        self._max_run_up_price_ = self._make_price_(max_run_up_price, reference=ep)
        self._max_draw_down_price_ = self._make_price_(max_draw_down_price, reference=ep)
        self._exit_price_ = self._make_price_(exit_price, reference=ep)
        eb = self._entry_balance_
        self._stop_loss_pnl_ = self._make_pnl_(stop_loss_pnl, reference=eb)
        self._take_profit_pnl_ = self._make_pnl_(take_profit_pnl, reference=eb)
        self._max_run_up_pnl_ = self._make_pnl_(max_run_up_pnl, reference=eb)
        self._max_draw_down_pnl_ = self._make_pnl_(max_draw_down_pnl, reference=eb)
        self._gross_pnl_ = self._make_pnl_(gross_pnl, reference=eb)
        self._commission_pnl_ = self._make_pnl_(commission_pnl, reference=eb)
        self._swap_pnl_ = self._make_pnl_(swap_pnl, reference=eb)
        self._net_pnl_ = self._make_pnl_(net_pnl, reference=eb)
        super().__post_init__(db=db, migrate=migrate, autosave=autosave, autoload=autoload, autooverload=autooverload)

    def _pull_(self, overload: bool) -> dict | None:
        row = super()._pull_(overload=overload)
        if row:
            self.PositionType = as_enum(PositionType, self.PositionType)
            self.TradeType = as_enum(TradeType, self.TradeType)
        return row

    @staticmethod
    def _unwrap_price_(val: float | PriceAPI | None) -> float | None:
        if isinstance(val, PriceAPI): return val.Price
        return val if val is not MISSING else None

    @staticmethod
    def _unwrap_pnl_(val: float | PnLAPI | None) -> float | None:
        if isinstance(val, PnLAPI): return val.PnL
        return val if val is not MISSING else None

    def _make_price_(self, val: float | PriceAPI | None, reference: float | None) -> PriceAPI | None:
        if isinstance(val, PriceAPI):
            if val.Contract is None: val.Contract = self._contract_
            if val.Reference is None: val.Reference = reference
            return val
        if val is MISSING or val is None: return None
        return PriceAPI(Price=val, Reference=reference, Contract=self._contract_)

    def _make_pnl_(self, val: float | PnLAPI | None, reference: float | None) -> PnLAPI | None:
        if isinstance(val, PnLAPI):
            if val.Reference is None: val.Reference = reference
            return val
        if val is MISSING or val is None: return None
        return PnLAPI(PnL=val, Reference=reference)

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
    def EntryTimestamp(self) -> TimestampAPI | None:
        return self._entry_timestamp_
    @EntryTimestamp.setter
    def EntryTimestamp(self, val: datetime | TimestampAPI | None) -> None:
        if isinstance(val, TimestampAPI): self._entry_timestamp_ = val
        elif val is not None:
            if self._entry_timestamp_: self._entry_timestamp_.DateTime = val
            else: self._entry_timestamp_ = TimestampAPI(DateTime=val)

    @property
    @overridefield
    def EntryPrice(self) -> PriceAPI | None:
        return self._entry_price_
    @EntryPrice.setter
    def EntryPrice(self, val: float | PriceAPI | None) -> None:
        price = val.Price if isinstance(val, PriceAPI) else val
        if price is None: return
        if self._entry_price_:
            self._entry_price_.Price = price
            self._entry_price_.Reference = price
        else:
            self._entry_price_ = PriceAPI(Price=price, Reference=price, Contract=self._contract_)
        for backing in (self._stop_loss_price_, self._take_profit_price_, self._max_run_up_price_, self._max_draw_down_price_, self._exit_price_):
            if backing: backing.Reference = price

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
    def MaxRunUpPrice(self) -> PriceAPI | None:
        return self._max_run_up_price_
    @MaxRunUpPrice.setter
    def MaxRunUpPrice(self, val: float | PriceAPI | None) -> None:
        self._max_run_up_price_ = self._assign_price_(self._max_run_up_price_, val)

    @property
    @overridefield
    def MaxDrawDownPrice(self) -> PriceAPI | None:
        return self._max_draw_down_price_
    @MaxDrawDownPrice.setter
    def MaxDrawDownPrice(self, val: float | PriceAPI | None) -> None:
        self._max_draw_down_price_ = self._assign_price_(self._max_draw_down_price_, val)

    @property
    @overridefield
    def ExitPrice(self) -> PriceAPI | None:
        return self._exit_price_
    @ExitPrice.setter
    def ExitPrice(self, val: float | PriceAPI | None) -> None:
        self._exit_price_ = self._assign_price_(self._exit_price_, val)

    def _assign_price_(self, backing: PriceAPI | None, val: float | PriceAPI | None) -> PriceAPI | None:
        if isinstance(val, PriceAPI): return val
        if val is None: return backing
        if backing:
            backing.Price = val
            return backing
        ref = self._entry_price_.Price if self._entry_price_ else val
        return PriceAPI(Price=val, Reference=ref, Contract=self._contract_)

    @property
    @overridefield
    def StopLossPnL(self) -> PnLAPI | None:
        return self._stop_loss_pnl_
    @StopLossPnL.setter
    def StopLossPnL(self, val: float | PnLAPI | None) -> None:
        self._stop_loss_pnl_ = self._assign_pnl_(self._stop_loss_pnl_, val)

    @property
    @overridefield
    def TakeProfitPnL(self) -> PnLAPI | None:
        return self._take_profit_pnl_
    @TakeProfitPnL.setter
    def TakeProfitPnL(self, val: float | PnLAPI | None) -> None:
        self._take_profit_pnl_ = self._assign_pnl_(self._take_profit_pnl_, val)

    @property
    @overridefield
    def MaxRunUpPnL(self) -> PnLAPI | None:
        return self._max_run_up_pnl_
    @MaxRunUpPnL.setter
    def MaxRunUpPnL(self, val: float | PnLAPI | None) -> None:
        self._max_run_up_pnl_ = self._assign_pnl_(self._max_run_up_pnl_, val)

    @property
    @overridefield
    def MaxDrawDownPnL(self) -> PnLAPI | None:
        return self._max_draw_down_pnl_
    @MaxDrawDownPnL.setter
    def MaxDrawDownPnL(self, val: float | PnLAPI | None) -> None:
        self._max_draw_down_pnl_ = self._assign_pnl_(self._max_draw_down_pnl_, val)

    @property
    @overridefield
    def GrossPnL(self) -> PnLAPI | None:
        return self._gross_pnl_
    @GrossPnL.setter
    def GrossPnL(self, val: float | PnLAPI | None) -> None:
        self._gross_pnl_ = self._assign_pnl_(self._gross_pnl_, val)

    @property
    @overridefield
    def CommissionPnL(self) -> PnLAPI | None:
        return self._commission_pnl_
    @CommissionPnL.setter
    def CommissionPnL(self, val: float | PnLAPI | None) -> None:
        self._commission_pnl_ = self._assign_pnl_(self._commission_pnl_, val)

    @property
    @overridefield
    def SwapPnL(self) -> PnLAPI | None:
        return self._swap_pnl_
    @SwapPnL.setter
    def SwapPnL(self, val: float | PnLAPI | None) -> None:
        self._swap_pnl_ = self._assign_pnl_(self._swap_pnl_, val)

    @property
    @overridefield
    def NetPnL(self) -> PnLAPI | None:
        return self._net_pnl_
    @NetPnL.setter
    def NetPnL(self, val: float | PnLAPI | None) -> None:
        self._net_pnl_ = self._assign_pnl_(self._net_pnl_, val)

    def _assign_pnl_(self, backing: PnLAPI | None, val: float | PnLAPI | None) -> PnLAPI | None:
        if isinstance(val, PnLAPI): return val
        if val is None: return backing
        if backing:
            backing.PnL = val
            return backing
        return PnLAPI(PnL=val, Reference=self._entry_balance_)

    @property
    @overridefield
    def EntryBalance(self) -> float | None:
        return self._entry_balance_
    @EntryBalance.setter
    def EntryBalance(self, val: float | None) -> None:
        self._entry_balance_ = val
        for backing in (self._stop_loss_pnl_, self._take_profit_pnl_, self._max_run_up_pnl_, self._max_draw_down_pnl_, self._gross_pnl_, self._commission_pnl_, self._swap_pnl_, self._net_pnl_):
            if backing: backing.Reference = val

    @property
    def Direction(self) -> TradeType | None:
        return self.TradeType if isinstance(self.TradeType, TradeType) else None
    @property
    def IsLong(self) -> bool:
        return self.TradeType == TradeType.Buy
    @property
    def IsShort(self) -> bool:
        return self.TradeType == TradeType.Sell
    @property
    def RiskReward(self) -> float | None:
        if self._entry_price_ is None or self._stop_loss_price_ is None or self._take_profit_price_ is None: return None
        ep, sl, tp = self._entry_price_.Price, self._stop_loss_price_.Price, self._take_profit_price_.Price
        if ep is None or sl is None or tp is None: return None
        risk, reward = abs(ep - sl), abs(tp - ep)
        if not risk: return None
        return reward / risk
    @property
    def RiskAmount(self) -> float | None:
        if self._stop_loss_pnl_ and self._stop_loss_pnl_.PnL is not None:
            return abs(self._stop_loss_pnl_.PnL)
        return None

    @property
    def RewardAmount(self) -> float | None:
        if self._take_profit_pnl_ and self._take_profit_pnl_.PnL is not None:
            return abs(self._take_profit_pnl_.PnL)
        return None

    @property
    def MarginUtilization(self) -> float | None:
        if not self.UsedMargin or not self._entry_balance_: return None
        return self.UsedMargin / self._entry_balance_

    @property
    def IsProfitable(self) -> bool | None:
        if self._net_pnl_ and self._net_pnl_.PnL is not None:
            return self._net_pnl_.PnL > 0
        return None

    @property
    def NetReturn(self) -> float | None:
        if self._net_pnl_:
            return self._net_pnl_.Return
        return None

    @property
    def Drawdown(self) -> float | None:
        if self._max_draw_down_pnl_:
            return self._max_draw_down_pnl_.Return
        return None

    @property
    def Runup(self) -> float | None:
        if self._max_run_up_pnl_:
            return self._max_run_up_pnl_.Return
        return None

    @property
    def Leverage(self) -> float | None:
        if not self.UsedMargin or self.Volume is None: return None
        return self.Volume / self.UsedMargin

    @property
    @overridefield
    def Contract(self) -> ContractAPI | None:
        return self._contract_
    @Contract.setter
    def Contract(self, val: ContractAPI | None) -> None:
        self._contract_ = val
        for backing in (self._entry_price_, self._stop_loss_price_, self._take_profit_price_, self._max_run_up_price_, self._max_draw_down_price_, self._exit_price_):
            if backing: backing.Contract = self._contract_