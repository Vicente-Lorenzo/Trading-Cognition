import queue
from typing import Union, Type, Callable
from datetime import datetime
from dataclasses import dataclass

from Library.Database import DatabaseAPI
from Library.Parameters import Parameters
from Library.Utils import timer

from Library.Utility import *
from Library.Engine import MachineAPI
from Library.Analyst import AnalystAPI
from Library.Manager import ManagerAPI
from Library.Strategy import StrategyAPI
from Library.System import SystemAPI

from Library.Portfolio.Account import AccountAPI, AccountType, AssetType, MarginMode
from Library.Universe.Contract import ContractAPI, CommissionMode, SwapMode, DayOfWeek
from Library.Portfolio.Position import PositionAPI, PositionType, Direction
from Library.Portfolio.Trade import TradeAPI
from Library.Market.Bar import BarAPI
from Library.Market.Tick import TickAPI

@dataclass(slots=True)
class _TickSnapshot:
    Timestamp: datetime
    Ask: float
    Bid: float
    AskBaseConversion: float
    BidBaseConversion: float
    AskQuoteConversion: float
    BidQuoteConversion: float

@dataclass(slots=True)
class _LastPositionData:
    Volume: float
    StopLoss: Union[float, None]
    TakeProfit: Union[float, None]

class TradingSystemAPI(SystemAPI):

    SENTINEL = -1.0

    def __init__(self,
                 api,
                 broker: str,
                 group: str,
                 symbol: str,
                 timeframe: str,
                 strategy: Type[StrategyAPI],
                 parameters: Parameters) -> None:

        super().__init__(
            broker=broker,
            group=group,
            symbol=symbol,
            timeframe=timeframe,
            strategy=strategy,
            parameters=parameters
        )

        self.api = api
        self.queue: queue.Queue = queue.Queue()

        self._sync_buffer: list[BarAPI] = []
        self._initial_account: Union[AccountAPI, None] = None
        self._start_timestamp: Union[datetime, None] = None
        self._stop_timestamp: Union[datetime, None] = None

        self._ask_base_conversion: Callable[[], float] = lambda: 1.0
        self._bid_base_conversion: Callable[[], float] = lambda: 1.0
        self._ask_quote_conversion: Callable[[], float] = lambda: 1.0
        self._bid_quote_conversion: Callable[[], float] = lambda: 1.0

        self._bar_timestamp: Union[datetime, None] = None
        self._gap_tick: Union[_TickSnapshot, None] = None
        self._open_tick: Union[_TickSnapshot, None] = None
        self._high_tick: Union[_TickSnapshot, None] = None
        self._low_tick: Union[_TickSnapshot, None] = None
        self._close_tick: Union[_TickSnapshot, None] = None

        self._positions: dict[int, _LastPositionData] = {}

        self._ask_above_target: Union[float, None] = None
        self._ask_below_target: Union[float, None] = None
        self._bid_above_target: Union[float, None] = None
        self._bid_below_target: Union[float, None] = None

        self._bar_db: Union[DatabaseAPI, None] = None

    def __enter__(self):
        self.strategy = self._strategy(
            money_management=self.parameters.MoneyManagement,
            risk_management=self.parameters.RiskManagement,
            signal_management=self.parameters.SignalManagement
        )
        self.analyst = AnalystAPI(analyst_management=self.parameters.AnalystManagement)
        self.manager = ManagerAPI(manager_management=self.parameters.ManagerManagement)

        self._bar_db = DatabaseAPI(
            broker=self._broker,
            group=self._group,
            symbol=self._symbol,
            timeframe=self._timeframe
        )
        self._bar_db.__enter__()

        self._setup_conversions()
        self._init_running_bar()
        self._attach_handlers()

        return super().__enter__()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._detach_handlers()
        if self._bar_db:
            self._bar_db.__exit__(None, None, None)
        return super().__exit__(exc_type, exc_value, exc_traceback)

    def _setup_conversions(self) -> None:
        base = self.api.Symbol.BaseAsset
        quote = self.api.Symbol.QuoteAsset
        account = self.api.Account.Asset
        self._ask_base_conversion, self._bid_base_conversion = self._find_conversions(base, account)
        self._ask_quote_conversion, self._bid_quote_conversion = self._find_conversions(quote, account)

    def _find_conversions(self, from_asset, to_asset) -> tuple[Callable[[], float], Callable[[], float]]:
        if from_asset == to_asset:
            return (lambda: 1.0, lambda: 1.0)
        for symbol_name in self.api.Symbols:
            try:
                if not self.api.Symbols.Exists(symbol_name):
                    continue
                s = self.api.Symbols.GetSymbol(symbol_name)
                if s.BaseAsset is None or s.QuoteAsset is None:
                    continue
                if s.BaseAsset == from_asset and s.QuoteAsset == to_asset:
                    return (lambda sym=s: sym.Ask, lambda sym=s: sym.Bid)
                if s.QuoteAsset == from_asset and s.BaseAsset == to_asset:
                    return (lambda sym=s: 1.0 / sym.Bid, lambda sym=s: 1.0 / sym.Ask)
            except Exception as e:
                self._log.warning(lambda m=str(e): m)
        raise RuntimeError(f"No conversion symbol found for {from_asset} -> {to_asset}")

    def _tick_snapshot(self) -> _TickSnapshot:
        return _TickSnapshot(
            Timestamp=self.api.Server.Time,
            Ask=self.api.Symbol.Ask,
            Bid=self.api.Symbol.Bid,
            AskBaseConversion=self._ask_base_conversion(),
            BidBaseConversion=self._bid_base_conversion(),
            AskQuoteConversion=self._ask_quote_conversion(),
            BidQuoteConversion=self._bid_quote_conversion()
        )

    def _init_running_bar(self) -> None:
        tick = self._tick_snapshot()
        self._bar_timestamp = self.api.Bars.LastBar.OpenTime
        self._gap_tick = tick
        self._open_tick = tick
        self._high_tick = tick
        self._low_tick = tick
        self._close_tick = tick

    def _reset_running_bar(self, timestamp: datetime) -> None:
        tick = self._tick_snapshot()
        self._bar_timestamp = timestamp
        self._gap_tick = self._close_tick
        self._open_tick = tick
        self._high_tick = tick
        self._low_tick = tick
        self._close_tick = tick

    def _update_running_bar(self, tick: _TickSnapshot) -> None:
        if tick.Ask > self._high_tick.Ask or tick.Bid > self._high_tick.Bid:
            self._high_tick = tick
        if tick.Ask < self._low_tick.Ask or tick.Bid < self._low_tick.Bid:
            self._low_tick = tick
        self._close_tick = tick

    def _snapshot_bar(self, tick_volume: float) -> BarAPI:
        return BarAPI(
            Timestamp=self._bar_timestamp,
            gap_timestamp=self._gap_tick.Timestamp,
            gap_ask=self._gap_tick.Ask,
            gap_bid=self._gap_tick.Bid,
            gap_ask_base_conversion=self._gap_tick.AskBaseConversion,
            gap_bid_base_conversion=self._gap_tick.BidBaseConversion,
            gap_ask_quote_conversion=self._gap_tick.AskQuoteConversion,
            gap_bid_quote_conversion=self._gap_tick.BidQuoteConversion,
            open_timestamp=self._open_tick.Timestamp,
            open_ask=self._open_tick.Ask,
            open_bid=self._open_tick.Bid,
            open_ask_base_conversion=self._open_tick.AskBaseConversion,
            open_bid_base_conversion=self._open_tick.BidBaseConversion,
            open_ask_quote_conversion=self._open_tick.AskQuoteConversion,
            open_bid_quote_conversion=self._open_tick.BidQuoteConversion,
            high_timestamp=self._high_tick.Timestamp,
            high_ask=self._high_tick.Ask,
            high_bid=self._high_tick.Bid,
            high_ask_base_conversion=self._high_tick.AskBaseConversion,
            high_bid_base_conversion=self._high_tick.BidBaseConversion,
            high_ask_quote_conversion=self._high_tick.AskQuoteConversion,
            high_bid_quote_conversion=self._high_tick.BidQuoteConversion,
            low_timestamp=self._low_tick.Timestamp,
            low_ask=self._low_tick.Ask,
            low_bid=self._low_tick.Bid,
            low_ask_base_conversion=self._low_tick.AskBaseConversion,
            low_bid_base_conversion=self._low_tick.BidBaseConversion,
            low_ask_quote_conversion=self._low_tick.AskQuoteConversion,
            low_bid_quote_conversion=self._low_tick.BidQuoteConversion,
            close_timestamp=self._close_tick.Timestamp,
            close_ask=self._close_tick.Ask,
            close_bid=self._close_tick.Bid,
            close_ask_base_conversion=self._close_tick.AskBaseConversion,
            close_bid_base_conversion=self._close_tick.BidBaseConversion,
            close_ask_quote_conversion=self._close_tick.AskQuoteConversion,
            close_bid_quote_conversion=self._close_tick.BidQuoteConversion,
            TickVolume=tick_volume,
            symbol=self.manager.Symbol
        )

    def _convert_account(self, acc) -> AccountAPI:
        return AccountAPI(
            AccountType=AccountType(acc.AccountType),
            AssetType=AssetType[acc.Asset.Name],
            Balance=acc.Balance,
            Equity=acc.Equity,
            Credit=acc.Credit,
            Leverage=acc.PreciseLeverage,
            MarginUsed=acc.Margin,
            MarginFree=acc.FreeMargin,
            MarginLevel=acc.MarginLevel if acc.MarginLevel is not None else None,
            MarginStopLevel=acc.StopOutLevel,
            MarginMode=MarginMode(acc.TotalMarginCalculationType)
        )

    def _convert_symbol(self, sym) -> ContractAPI:
        return ContractAPI(
            BaseAssetType=AssetType[sym.BaseAsset.Name],
            QuoteAssetType=AssetType[sym.QuoteAsset.Name],
            Digits=sym.Digits,
            PointSize=sym.TickSize,
            PipSize=sym.PipSize,
            LotSize=sym.LotSize,
            VolumeMin=sym.VolumeInUnitsMin,
            VolumeMax=sym.VolumeInUnitsMax,
            VolumeStep=sym.VolumeInUnitsStep,
            Commission=sym.Commission,
            CommissionMode=CommissionMode(sym.CommissionType),
            SwapLong=sym.SwapLong,
            SwapShort=sym.SwapShort,
            SwapMode=SwapMode(sym.SwapCalculationType),
            SwapExtraDay=Day((sym.Swap3DaysRollover - 1) % 7 if sym.Swap3DaysRollover is not None else 2)
        )

    def _convert_position(self, pos) -> PositionAPI:
        try:
            pos_type = PositionType[pos.Comment]
        except (KeyError, TypeError):
            pos_type = PositionType.Normal
        return PositionAPI(
            PositionID=pos.Id,
            Type=pos_type,
            Direction=Direction(int(pos.Direction)),
            Volume=pos.VolumeInUnits,
            Quantity=pos.Quantity,
            EntryTimestamp=pos.EntryTime,
            EntryPrice=pos.EntryPrice,
            StopLossPrice=pos.StopLoss if pos.StopLoss is not None else None,
            TakeProfitPrice=pos.TakeProfit if pos.TakeProfit is not None else None,
            ExitPrice=pos.CurrentPrice,
            GrossPnL=pos.GrossProfit,
            CommissionPnL=pos.Commissions,
            SwapPnL=pos.Swap,
            NetPnL=pos.NetProfit,
            UsedMargin=pos.Margin,
            contract=self.manager.Symbol,
            entry_balance=self.manager.Account.Balance if self.manager.Account else 0.0
        )

    def _convert_trade(self, trd) -> TradeAPI:
        try:
            pos_type = PositionType[trd.Comment]
        except (KeyError, TypeError):
            pos_type = PositionType.Normal
        return TradeAPI(
            PositionID=trd.PositionId,
            TradeID=trd.ClosingDealId,
            Type=pos_type,
            Direction=Direction(int(trd.Direction)),
            Volume=trd.VolumeInUnits,
            Quantity=trd.Quantity,
            EntryTimestamp=trd.EntryTime,
            ExitTimestamp=trd.ClosingTime,
            EntryPrice=trd.EntryPrice,
            ExitPrice=trd.ClosingPrice,
            GrossPnL=trd.GrossProfit,
            CommissionPnL=trd.Commissions,
            SwapPnL=trd.Swap,
            NetPnL=trd.NetProfit,
            contract=self.manager.Symbol,
            entry_balance=self.manager.Account.Balance if self.manager.Account else 0.0
        )

    def _is_own_position(self, pos) -> bool:
        return pos.Label == self.api.InstanceId

    def _find_trade(self, position_id: int):
        last = None
        for trd in self.api.History:
            if trd.PositionId == position_id:
                last = trd
        return last

    def _attach_handlers(self) -> None:
        self.api.Positions.Opened += self._on_position_opened
        self.api.Positions.Modified += self._on_position_modified
        self.api.Positions.Closed += self._on_position_closed

    def _detach_handlers(self) -> None:
        try:
            self.api.Positions.Opened -= self._on_position_opened
            self.api.Positions.Modified -= self._on_position_modified
            self.api.Positions.Closed -= self._on_position_closed
        except Exception:
            pass

    def receive_update_id(self) -> UpdateID:
        return self.queue.get()

    def receive_update_account(self) -> AccountAPI:
        return self.queue.get()

    def receive_update_symbol(self) -> ContractAPI:
        return self.queue.get()

    def receive_update_position(self) -> PositionAPI:
        return self.queue.get()

    def receive_update_trade(self) -> TradeAPI:
        return self.queue.get()

    def receive_update_bar(self) -> BarAPI:
        return self.queue.get()

    def receive_update_target(self) -> TickAPI:
        return self.queue.get()

    def send_action_complete(self, action: CompleteAction) -> None:
        pass

    def send_action_open(self, action: Union[OpenBuyAction, OpenSellAction]) -> None:
        trade_type = 0 if isinstance(action, OpenBuyAction) else 1
        sl = action.StopLoss if action.StopLoss is not None else None
        tp = action.TakeProfit if action.TakeProfit is not None else None
        result = self.api.ExecuteMarketOrder(
            trade_type,
            self.api.Symbol.Name,
            action.Volume,
            self.api.InstanceId,
            sl,
            tp,
            action.PositionType.name
        )
        if result is not None and not result.IsSuccessful:
            self._log.error(lambda: f"Open order failed: {result.Error}")
            self.api.Stop()

    def send_action_modify_volume(self, action: Union[ModifyBuyVolumeAction, ModifySellVolumeAction]) -> None:
        pos = next((p for p in self.api.Positions if p.Id == action.PositionID), None)
        if pos is None:
            self._log.warning(lambda: f"ModifyVolume: position {action.PositionID} not found")
            return
        result = pos.ModifyVolume(action.Volume)
        if result is not None and not result.IsSuccessful:
            self._log.error(lambda: f"ModifyVolume failed: {result.Error}")
            self.api.Stop()

    def send_action_modify_stop_loss(self, action: Union[ModifyBuyStopLossAction, ModifySellStopLossAction]) -> None:
        pos = next((p for p in self.api.Positions if p.Id == action.PositionID), None)
        if pos is None:
            self._log.warning(lambda: f"ModifyStopLoss: position {action.PositionID} not found")
            return
        result = pos.ModifyStopLossPrice(action.StopLoss)
        if result is not None and not result.IsSuccessful:
            self._log.error(lambda: f"ModifyStopLoss failed: {result.Error}")
            self.api.Stop()

    def send_action_modify_take_profit(self, action: Union[ModifyBuyTakeProfitAction, ModifySellTakeProfitAction]) -> None:
        pos = next((p for p in self.api.Positions if p.Id == action.PositionID), None)
        if pos is None:
            self._log.warning(lambda: f"ModifyTakeProfit: position {action.PositionID} not found")
            return
        result = pos.ModifyTakeProfitPrice(action.TakeProfit)
        if result is not None and not result.IsSuccessful:
            self._log.error(lambda: f"ModifyTakeProfit failed: {result.Error}")
            self.api.Stop()

    def send_action_close(self, action: Union[CloseBuyAction, CloseSellAction]) -> None:
        pos = next((p for p in self.api.Positions if p.Id == action.PositionID), None)
        if pos is None:
            self._log.warning(lambda: f"Close: position {action.PositionID} not found")
            return
        result = self.api.ClosePosition(pos)
        if result is not None and not result.IsSuccessful:
            self._log.error(lambda: f"Close failed: {result.Error}")
            self.api.Stop()

    def send_action_ask_above_target(self, action: AskAboveTargetAction) -> None:
        self._ask_above_target = action.Ask

    def send_action_ask_below_target(self, action: AskBelowTargetAction) -> None:
        self._ask_below_target = action.Ask

    def send_action_bid_above_target(self, action: BidAboveTargetAction) -> None:
        self._bid_above_target = action.Bid

    def send_action_bid_below_target(self, action: BidBelowTargetAction) -> None:
        self._bid_below_target = action.Bid

    def system_management(self) -> MachineAPI:
        system_engine = MachineAPI("System Management")
        initialization = system_engine.create_state(name="Initialization", end=False)
        execution = system_engine.create_state(name="Execution", end=False)
        termination = system_engine.create_state(name="Termination", end=True)

        def sync_market(update: BarUpdate):
            self._sync_buffer.append(update.Bar)

        def init_market(update: CompleteUpdate):
            self._initial_account = update.Manager.Account
            self._start_timestamp = self._sync_buffer[-1].Timestamp.DateTime if self._sync_buffer else None
            update.Analyst.init_market_data(self._sync_buffer)

        def update_market(update: BarUpdate):
            self._stop_timestamp = update.Bar.Timestamp.DateTime
            update.Analyst.update_market_data(update.Bar)

        def update_database(update: CompleteUpdate):
            self._bar_db.push_symbol_data(update.Manager.Symbol)
            self._bar_db.push_market_data(update.Analyst.Market.data())
            self.individual_trades, self.aggregated_trades, self.statistics = update.Manager.Statistics.data(
                self._initial_account, self._start_timestamp, self._stop_timestamp
            )

        initialization.on_bar_closed(to=initialization, action=sync_market, reason=None)
        initialization.on_complete(to=execution, action=init_market, reason="Market Initialized")
        initialization.on_shutdown(to=termination, action=None, reason="Abruptly Terminated")

        execution.on_bar_closed(to=execution, action=update_market, reason=None)
        execution.on_shutdown(to=termination, action=update_database, reason="Safely Terminated")

        return system_engine

    def on_tick(self) -> None:
        try:
            tick = self._tick_snapshot()
            self._update_running_bar(tick)

            fired = False
            if self._ask_above_target is not None and tick.Ask >= self._ask_above_target:
                self._enqueue_target(UpdateID.AskAboveTarget, tick)
                fired = True
            if self._ask_below_target is not None and tick.Ask <= self._ask_below_target:
                self._enqueue_target(UpdateID.AskBelowTarget, tick)
                fired = True
            if self._bid_above_target is not None and tick.Bid >= self._bid_above_target:
                self._enqueue_target(UpdateID.BidAboveTarget, tick)
                fired = True
            if self._bid_below_target is not None and tick.Bid <= self._bid_below_target:
                self._enqueue_target(UpdateID.BidBelowTarget, tick)
                fired = True

            if not fired:
                return
        except Exception as e:
            self._log.exception(lambda m=str(e): f"on_tick: {m}")
            self.api.Stop()

    def _enqueue_target(self, update_id: UpdateID, tick: _TickSnapshot) -> None:
        self.queue.put(update_id)
        self.queue.put(TickAPI(
            Timestamp=tick.Timestamp,
            Ask=tick.Ask,
            Bid=tick.Bid,
            AskBaseConversion=tick.AskBaseConversion,
            BidBaseConversion=tick.BidBaseConversion,
            AskQuoteConversion=tick.AskQuoteConversion,
            BidQuoteConversion=tick.BidQuoteConversion,
            symbol=self.manager.Symbol
        ))
        self.queue.put(UpdateID.Complete)

    def on_bar_closed(self) -> None:
        try:
            last = self.api.Bars.LastBar
            bar = self._snapshot_bar(tick_volume=last.TickVolume)
            self.queue.put(UpdateID.BarClosed)
            self.queue.put(bar)
            self.queue.put(UpdateID.Complete)
            self._reset_running_bar(timestamp=last.OpenTime)
        except Exception as e:
            self._log.exception(lambda m=str(e): f"on_bar_closed: {m}")
            self.api.Stop()

    def _on_position_opened(self, args) -> None:
        try:
            pos = args.Position
            if not self._is_own_position(pos):
                return
            self._positions[pos.Id] = _LastPositionData(
                Volume=pos.VolumeInUnits,
                StopLoss=pos.StopLoss,
                TakeProfit=pos.TakeProfit
            )
            update_id = UpdateID.OpenedBuy if int(pos.Direction) == 0 else UpdateID.OpenedSell
            self.queue.put(update_id)
            self.queue.put(self._snapshot_bar(tick_volume=0.0))
            self.queue.put(self._convert_account(self.api.Account))
            self.queue.put(self._convert_position(pos))
            self.queue.put(UpdateID.Complete)
        except Exception as e:
            self._log.exception(lambda m=str(e): f"on_position_opened: {m}")
            self.api.Stop()

    def _on_position_modified(self, args) -> None:
        try:
            pos = args.Position
            if not self._is_own_position(pos):
                return
            last = self._positions.get(pos.Id)
            if last is None:
                return

            buy = int(pos.Direction) == 0
            if abs(pos.VolumeInUnits - last.Volume) > 1e-12:
                trade = self._find_trade(pos.Id)
                update_id = UpdateID.ModifiedBuyVolume if buy else UpdateID.ModifiedSellVolume
                self.queue.put(update_id)
                self.queue.put(self._snapshot_bar(tick_volume=0.0))
                self.queue.put(self._convert_account(self.api.Account))
                self.queue.put(self._convert_position(pos))
                self.queue.put(self._convert_trade(trade) if trade is not None else None)
                self.queue.put(UpdateID.Complete)
                last.Volume = pos.VolumeInUnits
                return

            if self._changed(last.StopLoss, pos.StopLoss):
                update_id = UpdateID.ModifiedBuyStopLoss if buy else UpdateID.ModifiedSellStopLoss
                self.queue.put(update_id)
                self.queue.put(self._snapshot_bar(tick_volume=0.0))
                self.queue.put(self._convert_account(self.api.Account))
                self.queue.put(self._convert_position(pos))
                self.queue.put(UpdateID.Complete)
                last.StopLoss = pos.StopLoss
                return

            if self._changed(last.TakeProfit, pos.TakeProfit):
                update_id = UpdateID.ModifiedBuyTakeProfit if buy else UpdateID.ModifiedSellTakeProfit
                self.queue.put(update_id)
                self.queue.put(self._snapshot_bar(tick_volume=0.0))
                self.queue.put(self._convert_account(self.api.Account))
                self.queue.put(self._convert_position(pos))
                self.queue.put(UpdateID.Complete)
                last.TakeProfit = pos.TakeProfit
        except Exception as e:
            self._log.exception(lambda m=str(e): f"on_position_modified: {m}")
            self.api.Stop()

    def _on_position_closed(self, args) -> None:
        try:
            pos = args.Position
            if not self._is_own_position(pos):
                return
            trade = self._find_trade(pos.Id)
            buy = int(pos.Direction) == 0
            update_id = UpdateID.ClosedBuy if buy else UpdateID.ClosedSell
            self.queue.put(update_id)
            self.queue.put(self._snapshot_bar(tick_volume=0.0))
            self.queue.put(self._convert_account(self.api.Account))
            self.queue.put(self._convert_trade(trade) if trade is not None else None)
            self.queue.put(UpdateID.Complete)
            self._positions.pop(pos.Id, None)
        except Exception as e:
            self._log.exception(lambda m=str(e): f"on_position_closed: {m}")
            self.api.Stop()

    @staticmethod
    def _changed(old: Union[float, None], new: Union[float, None]) -> bool:
        if old is None and new is None:
            return False
        if old is None or new is None:
            return True
        return abs(float(old) - float(new)) > 1e-12

    def on_shutdown(self) -> None:
        self.queue.put(UpdateID.Shutdown)

    @timer
    def run(self) -> None:
        self.queue.put(UpdateID.Account)
        self.queue.put(self._convert_account(self.api.Account))
        self.queue.put(UpdateID.Symbol)
        self.queue.put(self._convert_symbol(self.api.Symbol))
        self.queue.put(UpdateID.Complete)
        self.deploy(strategy=self.strategy, analyst=self.analyst, manager=self.manager)
self.analyst, manager=self.manager)
