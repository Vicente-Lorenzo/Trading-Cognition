from __future__ import annotations

from datetime import datetime
from typing import ClassVar, TYPE_CHECKING
from dataclasses import dataclass, field, InitVar

from Library.Database.Dataframe import pl
from Library.Database.Database import IdentityKey, ForeignKey, DatabaseAPI
from Library.Database.Datapoint import DatapointAPI
from Library.Database.Dataclass import overridefield
from Library.Portfolio.Portfolio import PortfolioAPI
from Library.Portfolio.Position import PositionAPI
from Library.Portfolio.PnL import PnLAPI
from Library.Market.Timestamp import TimestampAPI
from Library.Market.Price import PriceAPI
from Library.Utility.Typing import MISSING

if TYPE_CHECKING:
    from Library.Universe.Security import SecurityAPI
    from Library.Universe.Contract import ContractAPI

@dataclass
class TradeAPI(PositionAPI):

    Database: ClassVar[str] = DatapointAPI.Database
    Schema: ClassVar[str] = PortfolioAPI.Schema
    Table: ClassVar[str] = "Trade"

    UID: int | None = None

    Position: InitVar[int | PositionAPI | None] = field(default=MISSING)
    ExitTimestamp: InitVar[datetime | TimestampAPI | None] = field(default=MISSING)
    ExitBalance: InitVar[float | None] = field(default=MISSING)

    _position_: PositionAPI | None = field(default=None, init=False, repr=False)
    _exit_timestamp_: TimestampAPI | None = field(default=None, init=False, repr=False)
    _exit_balance_: float | None = field(default=None, init=False, repr=False)

    @property
    def Structure(self) -> dict:
        return {
            self.ID.UID: IdentityKey(pl.Int64),
            self.ID.Position: ForeignKey(pl.Int64, reference=f'"{PortfolioAPI.Schema}"."{PositionAPI.Table}"("{PositionAPI.ID.UID}")'),
            self.ID.ExitTimestamp: pl.Datetime(),
            self.ID.ExitBalance: pl.Float64(),
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
                      contract: ContractAPI | None,
                      position: int | PositionAPI | None,
                      exit_timestamp: datetime | TimestampAPI | None,
                      exit_balance: float | None) -> None:
        position = MISSING if isinstance(position, property) else position
        exit_timestamp = MISSING if isinstance(exit_timestamp, property) else exit_timestamp
        exit_balance = MISSING if isinstance(exit_balance, property) else exit_balance
        if isinstance(position, PositionAPI): self._position_ = position
        elif position is not MISSING and position is not None:
            self._position_ = PositionAPI(UID=position, db=db, autoload=True)
        self._exit_balance_ = exit_balance if exit_balance is not MISSING else None
        if isinstance(exit_timestamp, TimestampAPI): self._exit_timestamp_ = exit_timestamp
        elif exit_timestamp is not MISSING and exit_timestamp is not None:
            self._exit_timestamp_ = TimestampAPI(DateTime=exit_timestamp)
        super().__post_init__(db=db, migrate=migrate, autosave=autosave, autoload=autoload, autooverload=autooverload,
                              security=security,
                              entry_timestamp=entry_timestamp, entry_price=entry_price,
                              stop_loss_price=stop_loss_price, take_profit_price=take_profit_price,
                              max_run_up_price=max_run_up_price, max_draw_down_price=max_draw_down_price,
                              exit_price=exit_price,
                              stop_loss_pnl=stop_loss_pnl, take_profit_pnl=take_profit_pnl,
                              max_run_up_pnl=max_run_up_pnl, max_draw_down_pnl=max_draw_down_pnl,
                              gross_pnl=gross_pnl, commission_pnl=commission_pnl,
                              swap_pnl=swap_pnl, net_pnl=net_pnl,
                              entry_balance=entry_balance, contract=contract)

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
    def ExitTimestamp(self) -> TimestampAPI | None:
        return self._exit_timestamp_
    @ExitTimestamp.setter
    def ExitTimestamp(self, val: datetime | TimestampAPI | None) -> None:
        if isinstance(val, TimestampAPI): self._exit_timestamp_ = val
        elif val is not None:
            if self._exit_timestamp_: self._exit_timestamp_.DateTime = val
            else: self._exit_timestamp_ = TimestampAPI(DateTime=val)

    @property
    @overridefield
    def ExitBalance(self) -> float | None:
        return self._exit_balance_
    @ExitBalance.setter
    def ExitBalance(self, val: float | None) -> None:
        self._exit_balance_ = val

    @property
    def ExitReturn(self) -> PnLAPI | None:
        if self._exit_balance_ is None or self._entry_balance_ is None: return None
        return PnLAPI(PnL=self._exit_balance_ - self._entry_balance_, Reference=self._entry_balance_)

    @property
    def Duration(self) -> float | None:
        if self._entry_timestamp_ is None or self._exit_timestamp_ is None: return None
        return (self._exit_timestamp_.DateTime - self._entry_timestamp_.DateTime).total_seconds()

    @property
    def DurationDays(self) -> float | None:
        seconds = self.Duration
        return seconds / 86400.0 if seconds is not None else None

    @property
    def HoldingPeriod(self) -> float | None:
        return self.Duration

    @property
    def IsClosed(self) -> bool:
        return self._exit_timestamp_ is not None