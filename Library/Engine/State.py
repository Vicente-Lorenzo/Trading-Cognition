from typing import Union, Self, Callable

from Library.Utility import *

class StateAPI:

    def __init__(self, name: str, end: bool):
        self.name: str = name
        self.end: bool = end

        self.complete_transition: Union[TransitionAPI, None] = None
        self.account_transition: Union[TransitionAPI, None] = None
        self.security_transition: Union[TransitionAPI, None] = None
        self.opened_buy_transition: Union[TransitionAPI, None] = None
        self.opened_sell_transition: Union[TransitionAPI, None] = None
        self.modified_volume_buy_transition: Union[TransitionAPI, None] = None
        self.modified_stop_loss_buy_transition: Union[TransitionAPI, None] = None
        self.modified_take_profit_buy_transition: Union[TransitionAPI, None] = None
        self.modified_volume_sell_transition: Union[TransitionAPI, None] = None
        self.modified_stop_loss_sell_transition: Union[TransitionAPI, None] = None
        self.modified_take_profit_sell_transition: Union[TransitionAPI, None] = None
        self.closed_buy_transition: Union[TransitionAPI, None] = None
        self.closed_sell_transition: Union[TransitionAPI, None] = None
        self.bar_transition: Union[TransitionAPI, None] = None
        self.ask_above_target_transition: Union[TransitionAPI, None] = None
        self.ask_below_target_transition: Union[TransitionAPI, None] = None
        self.bid_above_target_transition: Union[TransitionAPI, None] = None
        self.bid_below_target_transition: Union[TransitionAPI, None] = None
        self.shutdown_transition: Union[TransitionAPI, None] = None

    def on_complete(self, to: Self, action: Callable[[CompleteUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.complete_transition = TransitionAPI(to, action, reason)

    def on_account(self, to: Self, action: Callable[[AccountUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.account_transition = TransitionAPI(to, action, reason)

    def on_security(self, to: Self, action: Callable[[SecurityUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.security_transition = TransitionAPI(to, action, reason)

    def on_opened_buy(self, to: Self, action: Callable[[PositionUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.opened_buy_transition = TransitionAPI(to, action, reason)

    def on_opened_sell(self, to: Self, action: Callable[[PositionUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.opened_sell_transition = TransitionAPI(to, action, reason)

    def on_modified_volume_buy(self, to: Self, action: Callable[[PositionTradeUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.modified_volume_buy_transition = TransitionAPI(to, action, reason)

    def on_modified_stop_loss_buy(self, to: Self, action: Callable[[PositionUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.modified_stop_loss_buy_transition = TransitionAPI(to, action, reason)

    def on_modified_take_profit_buy(self, to: Self, action: Callable[[PositionUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.modified_take_profit_buy_transition = TransitionAPI(to, action, reason)

    def on_modified_volume_sell(self, to: Self, action: Callable[[PositionTradeUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.modified_volume_sell_transition = TransitionAPI(to, action, reason)

    def on_modified_stop_loss_sell(self, to: Self, action: Callable[[PositionUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.modified_stop_loss_sell_transition = TransitionAPI(to, action, reason)

    def on_modified_take_profit_sell(self, to: Self, action: Callable[[PositionUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.modified_take_profit_sell_transition = TransitionAPI(to, action, reason)

    def on_closed_buy(self, to: Self, action: Callable[[TradeUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.closed_buy_transition = TransitionAPI(to, action, reason)

    def on_closed_sell(self, to: Self, action: Callable[[TradeUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.closed_sell_transition = TransitionAPI(to, action, reason)

    def on_bar_closed(self, to: Self, action: Callable[[BarUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.bar_transition = TransitionAPI(to, action, reason)

    def on_ask_above_target(self, to: Self, action: Callable[[TickUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.ask_above_target_transition = TransitionAPI(to, action, reason)

    def on_ask_below_target(self, to: Self, action: Callable[[TickUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.ask_below_target_transition = TransitionAPI(to, action, reason)

    def on_bid_above_target(self, to: Self, action: Callable[[TickUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.bid_above_target_transition = TransitionAPI(to, action, reason)

    def on_bid_below_target(self, to: Self, action: Callable[[TickUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.bid_below_target_transition = TransitionAPI(to, action, reason)

    def on_shutdown(self, to: Self, action: Callable[[CompleteUpdate], list[Action] | None] | None, reason: Union[str, None]) -> None:
        self.shutdown_transition = TransitionAPI(to, action, reason)

class TransitionAPI:

    def __init__(self, to: StateAPI, action: Callable[[Update], list[Action] | None] | None, reason: Union[str, None]):
        self.to: StateAPI = to
        self.action: Callable[[Update], list[Action] | None] | None = action
        self.reason: Union[str, None] = reason

    def perform_action(self, args: Update) -> Union[list[Action], None]:
        return self.action(args) if self.action is not None else None
