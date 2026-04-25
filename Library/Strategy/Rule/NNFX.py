from typing import Union
from Library.Classes import PositionType, Direction, TechnicalMode
from Library.Parameters import Parameters

from Library.Utility import *
from Library.Engine import MachineAPI
from Library.Strategy import StrategyAPI

class NNFXStrategyAPI(StrategyAPI):

    def __init__(self,
                 money_management: Parameters,
                 risk_management: Parameters,
                 signal_management: Parameters):

        super().__init__(money_management, risk_management, signal_management)

        self._risk_percentage, = self.MoneyManagement.RiskPercentage

        self._stop_loss_scale, = self.RiskManagement.StopLossScale
        self._scaling_out_scale, = self.RiskManagement.ScalingOutScale
        self._scaling_out_percentage, = self.RiskManagement.ScalingOutPercentage
        self._trailing_stop_loss_scale, = self.RiskManagement.TrailingStopLossScale

        modes = {
            "Baseline": self.SignalManagement.BaselineMode,
            "Filter1": self.SignalManagement.Filter1Mode,
            "Filter2": self.SignalManagement.Filter2Mode,
            "Volume": self.SignalManagement.VolumeMode
        }

        normal_entries_buy = []
        normal_entries_sell = []
        continuation_entries_buy = []
        continuation_entries_sell = []
        normal_exits_buy = []
        normal_exits_sell = []

        for technical_name, (normal_entry_mode, continuation_entry_mode, normal_exit_mode) in modes.items():
            match normal_entry_mode:
                case TechnicalMode.Filter.name:
                    normal_entries_buy.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).filter_buy(update.Analyst.Market))
                    normal_entries_sell.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).filter_sell(update.Analyst.Market))
                case TechnicalMode.Signal.name:
                    normal_entries_buy.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).signal_buy(update.Analyst.Market))
                    normal_entries_sell.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).signal_sell(update.Analyst.Market))
            match continuation_entry_mode:
                case TechnicalMode.Filter.name:
                    continuation_entries_buy.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).filter_buy(update.Analyst.Market))
                    continuation_entries_sell.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).filter_sell(update.Analyst.Market))
                case TechnicalMode.Signal.name:
                    continuation_entries_buy.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).signal_buy(update.Analyst.Market))
                    continuation_entries_sell.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).signal_sell(update.Analyst.Market))
            match normal_exit_mode:
                case TechnicalMode.Filter.name:
                    normal_exits_buy.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).filter_sell(update.Analyst.Market))
                    normal_exits_sell.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).filter_buy(update.Analyst.Market))
                case TechnicalMode.Signal.name:
                    normal_exits_buy.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).signal_sell(update.Analyst.Market))
                    normal_exits_sell.append(lambda update, tname=technical_name: getattr(update.Analyst, tname).signal_buy(update.Analyst.Market))

        self._normal_entry_buy = lambda update: all(f(update) for f in normal_entries_buy) if normal_entries_buy else False
        self._normal_entry_sell = lambda update: all(f(update) for f in normal_entries_sell) if normal_entries_sell else False
        self._continuation_entry_buy = lambda update: all(f(update) for f in continuation_entries_buy) if continuation_entries_buy else False
        self._continuation_entry_sell = lambda update: all(f(update) for f in continuation_entries_sell) if continuation_entries_sell else False
        self._normal_exit_buy = lambda update: any(f(update) for f in normal_exits_buy) if normal_exits_buy else False
        self._normal_exit_sell = lambda update: any(f(update) for f in normal_exits_sell) if normal_exits_sell else False

        self._last_position_id: Union[int, None] = None
        self._last_position_atr: Union[float, None] = None
        self._last_position_trade_type: Union[Direction, None] = None

    def define_so_buy_action(self, update: PositionUpdate):
        self._last_position_id = update.Position.PositionID
        return [BidAboveTargetAction(update.Position.EntryPrice.Price + self._scaling_out_scale * self._last_position_atr)]

    def define_so_sell_action(self, update: PositionUpdate):
        self._last_position_id = update.Position.PositionID
        return [AskBelowTargetAction(update.Position.EntryPrice.Price - self._scaling_out_scale * self._last_position_atr)]

    def close_buy_partially_action(self, update: TickUpdate):
        position = update.Manager.Positions.Buys[self._last_position_id]
        volume = position.Volume * (1.0 - self._scaling_out_percentage / 100)
        volume = update.Manager.normalize_volume(volume) if volume else volume
        return [ModifyBuyVolumeAction(self._last_position_id, volume)]

    def close_sell_partially_action(self, update: TickUpdate):
        position = update.Manager.Positions.Sells[self._last_position_id]
        volume = position.Volume * (1.0 - self._scaling_out_percentage / 100)
        volume = update.Manager.normalize_volume(volume) if volume else volume
        return [ModifySellVolumeAction(self._last_position_id, volume)]

    def breakeven_buy_action(self, update: PositionTradeUpdate):
        return [ModifyBuyStopLossAction(self._last_position_id, update.Position.EntryPrice.Price)]

    def breakeven_sell_action(self, update: PositionTradeUpdate):
        return [ModifySellStopLossAction(self._last_position_id, update.Position.EntryPrice.Price)]

    def define_tsl_buy_action(self, update: PositionUpdate):
        return [BidAboveTargetAction(update.Position.StopLoss.Price + self._trailing_stop_loss_scale * self._last_position_atr + update.Manager.Symbol.PointSize)]

    def define_tsl_sell_action(self, update: PositionUpdate):
        return [AskBelowTargetAction(update.Position.StopLoss.Price - self._trailing_stop_loss_scale * self._last_position_atr - update.Manager.Symbol.PointSize)]

    def detected_tsl_buy_action(self, update: TickUpdate):
        return [ModifyBuyStopLossAction(self._last_position_id, update.Tick.Bid.Price - self._trailing_stop_loss_scale * self._last_position_atr)]

    def detected_tsl_sell_action(self, update: TickUpdate):
        return [ModifySellStopLossAction(self._last_position_id, update.Tick.Ask.Price + self._trailing_stop_loss_scale * self._last_position_atr)]

    @staticmethod
    def undefine_tsl_buy_action(_: TradeUpdate):
        return [BidAboveTargetAction(None)]

    @staticmethod
    def undefine_tsl_sell_action(_: TradeUpdate):
        return [AskBelowTargetAction(None)]

    def risk_management(self):

        risk_engine = MachineAPI("Risk Management")

        initialisation = risk_engine.create_state(name="Initialisation", end=False)
        waiting_open = risk_engine.create_state(name="No Position", end=False)
        waiting_so = risk_engine.create_state(name="Waiting SO", end=False)
        waiting_tsl = risk_engine.create_state(name="Waiting TSL", end=False)
        waiting_close = risk_engine.create_state(name="Waiting Close", end=False)
        termination = risk_engine.create_state(name="Termination", end=True)

        initialisation.on_complete(to=waiting_open, action=None, reason="Initialized")
        initialisation.on_shutdown(to=termination, action=None, reason="Abruptly Terminated")

        waiting_open.on_opened_buy(to=waiting_so, action=self.define_so_buy_action, reason="Opened Buy Position")
        waiting_open.on_opened_sell(to=waiting_so, action=self.define_so_sell_action, reason="Opened Sell Position")
        waiting_open.on_shutdown(to=termination, action=None, reason="Safely Terminated")

        waiting_so.on_closed_buy(to=waiting_open, action=self.undefine_tsl_buy_action, reason="Closed Buy Position")
        waiting_so.on_closed_sell(to=waiting_open, action=self.undefine_tsl_sell_action, reason="Closed Sell Position")
        waiting_so.on_bid_above_target(to=waiting_so, action=self.close_buy_partially_action, reason="Hit TP Activation for Buy Position")
        waiting_so.on_ask_below_target(to=waiting_so, action=self.close_sell_partially_action, reason="Hit TP Activation for Sell Position")
        waiting_so.on_modified_volume_buy(to=waiting_so, action=self.breakeven_buy_action, reason="Closed Partially Buy Position")
        waiting_so.on_modified_volume_sell(to=waiting_so, action=self.breakeven_sell_action, reason="Closed Partially Sell Position")
        waiting_so.on_modified_stop_loss_buy(to=waiting_tsl, action=self.define_tsl_buy_action, reason="Moved Buy Position to Break-Even")
        waiting_so.on_modified_stop_loss_sell(to=waiting_tsl, action=self.define_tsl_sell_action, reason="Moved Sell Position to Break-Even")
        waiting_so.on_shutdown(to=termination, action=None, reason="Safely Terminated")

        waiting_tsl.on_closed_buy(to=waiting_open, action=self.undefine_tsl_buy_action, reason="Closed Buy Position")
        waiting_tsl.on_closed_sell(to=waiting_open, action=self.undefine_tsl_sell_action, reason="Closed Sell Position")
        waiting_tsl.on_bid_above_target(to=waiting_tsl, action=self.detected_tsl_buy_action, reason="Hit TSL Activation for Buy Position")
        waiting_tsl.on_ask_below_target(to=waiting_tsl, action=self.detected_tsl_sell_action, reason="Hit TSL Activation for Sell Position")
        waiting_tsl.on_modified_stop_loss_buy(to=waiting_close, action=self.define_tsl_buy_action, reason="Activated TSL for Buy Position")
        waiting_tsl.on_modified_stop_loss_sell(to=waiting_close, action=self.define_tsl_sell_action, reason="Activated TSL for Sell Position")
        waiting_tsl.on_shutdown(to=termination, action=None, reason="Safely Terminated")

        waiting_close.on_closed_buy(to=waiting_open, action=self.undefine_tsl_buy_action, reason="Closed Buy Position")
        waiting_close.on_closed_sell(to=waiting_open, action=self.undefine_tsl_sell_action, reason="Closed Sell Position")
        waiting_close.on_bid_above_target(to=waiting_close, action=self.detected_tsl_buy_action, reason="Hit TSL Update for Buy Position")
        waiting_close.on_ask_below_target(to=waiting_close, action=self.detected_tsl_sell_action, reason="Hit TSL Update for Sell Position")
        waiting_close.on_modified_stop_loss_buy(to=waiting_close, action=self.define_tsl_buy_action, reason="Updated TSL for Buy Position")
        waiting_close.on_modified_stop_loss_sell(to=waiting_close, action=self.define_tsl_sell_action, reason="Updated TSL for Sell Position")
        waiting_close.on_shutdown(to=termination, action=None, reason="Safely Terminated")

        return risk_engine

    def calculate_position(self, update: BarUpdate):
        self._last_position_atr = update.Analyst.Volatility.Result.last()
        sl_pips = self._stop_loss_scale * self._last_position_atr / update.Manager.Symbol.PipSize
        volume = update.Manager.volume_by_risk(self._risk_percentage, sl_pips)
        return volume, sl_pips

    def open_buy_position(self, update: BarUpdate, position_type: PositionType):
        self._last_position_trade_type = Direction.Buy
        volume, sl_pips = self.calculate_position(update)
        return self.close_sell_position(update) + [OpenBuyAction(position_type, volume, sl_pips, None)]

    def open_sell_position(self, update: BarUpdate, position_type: PositionType):
        self._last_position_trade_type = Direction.Sell
        volume, sl_pips = self.calculate_position(update)
        return self.close_buy_position(update) + [OpenSellAction(position_type, volume, sl_pips, None)]

    def close_buy_position(self, update: BarUpdate):
        return [CloseBuyAction(self._last_position_id)] if update.Manager.Positions.Buys else []

    def close_sell_position(self, update: BarUpdate):
        return [CloseSellAction(self._last_position_id)] if update.Manager.Positions.Sells else []

    def update_position(self, update: BarUpdate):
        if not update.Manager.Positions.Buys:
            if self._normal_entry_buy(update):
                return self.open_buy_position(update, PositionType.Normal)
            if self._last_position_trade_type and self._last_position_trade_type == Direction.Buy and self._continuation_entry_buy(update):
                return self.open_buy_position(update, PositionType.Continuation)
        elif self._normal_exit_buy(update):
            return self.close_buy_position(update)

        if not update.Manager.Positions.Sells:
            if self._normal_entry_sell(update):
                return self.open_sell_position(update, PositionType.Normal)
            if self._last_position_trade_type and self._last_position_trade_type == Direction.Sell and self._continuation_entry_sell(update):
                return self.open_sell_position(update, PositionType.Continuation)
        elif self._normal_exit_sell(update):
            return self.close_sell_position(update)
        return None

    def signal_management(self):

        signal_engine = MachineAPI("Signal Management")

        initialisation = signal_engine.create_state(name="Initialisation", end=False)
        waiting_signal = signal_engine.create_state(name="Waiting Signal", end=False)
        termination = signal_engine.create_state(name="Termination", end=True)

        initialisation.on_complete(to=waiting_signal, action=None, reason="Initialized")
        initialisation.on_shutdown(to=termination, action=None, reason="Abruptly Terminated")

        waiting_signal.on_bar_closed(to=waiting_signal, action=self.update_position, reason=None)
        waiting_signal.on_shutdown(to=termination, action=None, reason="Safely Terminated")

        return signal_engine
