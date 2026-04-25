from typing import Union
from Library.Logging import HandlerAPI

from Library.Utility import *
from Library.Engine import StateAPI, TransitionAPI

class MachineAPI:

    def __init__(self, name: Union[str, None]):
        self.at = None
        self._states: list[StateAPI] = []
        self._log: HandlerAPI = HandlerAPI(Class=self.__class__.__name__, Subclass=name)

    def create_state(self, name: Union[str, None], end: bool) -> StateAPI:
        state = StateAPI(name, end)
        self._states.append(state)
        self.at = state if not self.at else self.at
        return state

    def _process_update(self, transition: TransitionAPI, args) -> list[Action]:
        if transition is not None:
            ret = transition.perform_action(args)
            if transition.reason is not None:
                self._log.debug(lambda: f"[{self.at.name}] > {transition.reason} > [{transition.to.name}]")
            self.at = transition.to
            return ret if ret is not None else []
        return []

    def perform_update_complete(self, args: CompleteUpdate) -> list[Action]:
        return self._process_update(self.at.complete_transition, args)

    def perform_update_account(self, args: AccountUpdate) -> list[Action]:
        return self._process_update(self.at.account_transition, args)

    def perform_update_security(self, args: SecurityUpdate) -> list[Action]:
        return self._process_update(self.at.security_transition, args)

    def perform_update_opened_buy(self, args: PositionUpdate) -> list[Action]:
        return self._process_update(self.at.opened_buy_transition, args)

    def perform_update_opened_sell(self, args: PositionUpdate) -> list[Action]:
        return self._process_update(self.at.opened_sell_transition, args)

    def perform_update_modified_volume_buy(self, args: PositionTradeUpdate) -> list[Action]:
        return self._process_update(self.at.modified_volume_buy_transition, args)

    def perform_update_modified_stop_loss_buy(self, args: PositionUpdate) -> list[Action]:
        return self._process_update(self.at.modified_stop_loss_buy_transition, args)

    def perform_update_modified_take_profit_buy(self, args: PositionUpdate) -> list[Action]:
        return self._process_update(self.at.modified_take_profit_buy_transition, args)

    def perform_update_modified_volume_sell(self, args: PositionTradeUpdate) -> list[Action]:
        return self._process_update(self.at.modified_volume_sell_transition, args)

    def perform_update_modified_stop_loss_sell(self, args: PositionUpdate) -> list[Action]:
        return self._process_update(self.at.modified_stop_loss_sell_transition, args)

    def perform_update_modified_take_profit_sell(self, args: PositionUpdate) -> list[Action]:
        return self._process_update(self.at.modified_take_profit_sell_transition, args)

    def perform_update_closed_buy(self, args: TradeUpdate) -> list[Action]:
        return self._process_update(self.at.closed_buy_transition, args)

    def perform_update_closed_sell(self, args: TradeUpdate) -> list[Action]:
        return self._process_update(self.at.closed_sell_transition, args)

    def perform_update_bar_closed(self, args: BarUpdate) -> list[Action]:
        return self._process_update(self.at.bar_transition, args)

    def perform_update_ask_above_target(self, args: TickUpdate) -> list[Action]:
        return self._process_update(self.at.ask_above_target_transition, args)

    def perform_update_ask_below_target(self, args: TickUpdate) -> list[Action]:
        return self._process_update(self.at.ask_below_target_transition, args)

    def perform_update_bid_above_target(self, args: TickUpdate) -> list[Action]:
        return self._process_update(self.at.bid_above_target_transition, args)

    def perform_update_bid_below_target(self, args: TickUpdate) -> list[Action]:
        return self._process_update(self.at.bid_below_target_transition, args)

    def perform_update_shutdown(self, args: CompleteUpdate) -> list[Action]:
        return self._process_update(self.at.shutdown_transition, args)
