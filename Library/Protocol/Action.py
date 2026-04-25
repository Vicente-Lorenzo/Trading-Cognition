from typing import Union
from enum import Enum
from dataclasses import dataclass, field

from Library.Database.Dataclass import DataclassAPI
from Library.Portfolio.Position import PositionType
from Library.Utility.Typing import cast

class ActionID(Enum):
    Complete = 0
    OpenBuy = 1
    OpenSell = 2
    ModifyBuyVolume = 3
    ModifyBuyStopLoss = 4
    ModifyBuyTakeProfit = 5
    ModifySellVolume = 6
    ModifySellStopLoss = 7
    ModifySellTakeProfit = 8
    CloseBuy = 9
    CloseSell = 10
    AskAboveTarget = 11
    AskBelowTarget = 12
    BidAboveTarget = 13
    BidBelowTarget = 14

@dataclass(slots=True)
class CompleteAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.Complete, init=False)

@dataclass(slots=True)
class OpenBuyAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.OpenBuy, init=False)
    PositionType: PositionType
    Volume: float
    StopLoss: Union[float, None]
    TakeProfit: Union[float, None]

    def __post_init__(self):
        self.PositionType = PositionType(self.PositionType)
        self.StopLoss = cast(self.StopLoss, float, None)
        self.TakeProfit = cast(self.TakeProfit, float, None)

@dataclass(slots=True)
class OpenSellAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.OpenSell, init=False)
    PositionType: PositionType
    Volume: float
    StopLoss: Union[float, None]
    TakeProfit: Union[float, None]

    def __post_init__(self):
        self.PositionType = PositionType(self.PositionType)
        self.StopLoss = cast(self.StopLoss, float, None)
        self.TakeProfit = cast(self.TakeProfit, float, None)

@dataclass(slots=True)
class ModifyBuyVolumeAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.ModifyBuyVolume, init=False)
    PositionID: int
    Volume: float

@dataclass(slots=True)
class ModifySellVolumeAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.ModifySellVolume, init=False)
    PositionID: int
    Volume: float

@dataclass(slots=True)
class ModifyBuyStopLossAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.ModifyBuyStopLoss, init=False)
    PositionID: int
    StopLoss: Union[float, None]

@dataclass(slots=True)
class ModifySellStopLossAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.ModifySellStopLoss, init=False)
    PositionID: int
    StopLoss: Union[float, None]

@dataclass(slots=True)
class ModifyBuyTakeProfitAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.ModifyBuyTakeProfit, init=False)
    PositionID: int
    TakeProfit: Union[float, None]

@dataclass(slots=True)
class ModifySellTakeProfitAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.ModifySellTakeProfit, init=False)
    PositionID: int
    TakeProfit: Union[float, None]

@dataclass(slots=True)
class CloseBuyAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.CloseBuy, init=False)
    PositionID: int

@dataclass(slots=True)
class CloseSellAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.CloseSell, init=False)
    PositionID: int

@dataclass(slots=True)
class AskAboveTargetAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.AskAboveTarget, init=False)
    Ask: Union[float, None]

    def __post_init__(self):
        self.Ask = cast(self.Ask, float, None)

@dataclass(slots=True)
class AskBelowTargetAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.AskBelowTarget, init=False)
    Ask: Union[float, None]

    def __post_init__(self):
        self.Ask = cast(self.Ask, float, None)

@dataclass(slots=True)
class BidAboveTargetAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.BidAboveTarget, init=False)
    Bid: Union[float, None]

    def __post_init__(self):
        self.Bid = cast(self.Bid, float, None)

@dataclass(slots=True)
class BidBelowTargetAction(DataclassAPI):
    ActionID: ActionID = field(default=ActionID.BidBelowTarget, init=False)
    Bid: Union[float, None]

    def __post_init__(self):
        self.Bid = cast(self.Bid, float, None)

Action = Union[CompleteAction, OpenBuyAction, OpenSellAction, ModifyBuyVolumeAction, ModifyBuyStopLossAction, ModifyBuyTakeProfitAction, ModifySellVolumeAction, ModifySellStopLossAction, ModifySellTakeProfitAction, CloseBuyAction, CloseSellAction, AskAboveTargetAction, AskBelowTargetAction, BidAboveTargetAction, BidBelowTargetAction]
