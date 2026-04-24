from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import datetime
from typing import ClassVar, TYPE_CHECKING
from dataclasses import dataclass

from Library.Database.Dataframe import pl
from Library.Database.Datapoint import DatapointAPI
from Library.Database.Query import QueryAPI

if TYPE_CHECKING:
    from Library.Database.Database import DatabaseAPI
    from Library.Portfolio.Account import AccountAPI
    from Library.Portfolio.Order import OrderAPI
    from Library.Portfolio.Position import PositionAPI
    from Library.Portfolio.Trade import TradeAPI

@dataclass(kw_only=True)
class PortfolioAPI(DatapointAPI):
    Database: ClassVar[str] = DatapointAPI.Database
    Schema: ClassVar[str] = "Portfolio"
    Table: ClassVar[str] = "Portfolio"

    Trades: Sequence[TradeAPI] = ()
    Positions: Sequence[PositionAPI] = ()

    @property
    def ROI(self) -> float | None:
        if not self.Trades: return None
        entry_b = self.Trades[0].EntryBalance
        if not entry_b: return None
        net_pnl = sum((t.NetPnL.PnL for t in self.Trades if t.NetPnL and t.NetPnL.PnL is not None), 0.0)
        return net_pnl / entry_b

    @property
    def AnnualizedReturn(self) -> float | None:
        roi = self.ROI
        if roi is None or not self.Trades: return None
        first, last = self.Trades[0].EntryTimestamp, self.Trades[-1].ExitTimestamp
        if not first or not last: return None
        days = (last.DateTime - first.DateTime).total_seconds() / 86400
        if days <= 0: return None
        return ((1.0 + roi) ** (365.0 / days)) - 1.0

    @property
    def LogReturn(self) -> float | None:
        roi = self.ROI
        if roi is None or roi <= -1.0: return None
        return math.log1p(roi)

    @property
    def StandardDeviation(self) -> float | None:
        if not self.Trades or len(self.Trades) < 2: return None
        returns = [t.NetPnL.Return for t in self.Trades if t.NetPnL and t.NetPnL.Return is not None]
        if len(returns) < 2: return None
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
        return math.sqrt(var)

    @property
    def Volatility(self) -> float | None:
        std = self.StandardDeviation
        return std * math.sqrt(252.0) if std is not None else None

    @property
    def ValueAtRisk(self) -> float | None:
        if not self.Trades: return None
        returns = sorted([t.NetPnL.Return for t in self.Trades if t.NetPnL and t.NetPnL.Return is not None])
        if not returns: return None
        index = int(0.05 * len(returns))
        return returns[index]

    @property
    def RiskRewardRatio(self) -> float | None:
        winners = [t.NetPnL.Return for t in self.Trades if t.NetPnL and t.NetPnL.Return is not None and t.NetPnL.Return > 0]
        losers = [abs(t.NetPnL.Return) for t in self.Trades if t.NetPnL and t.NetPnL.Return is not None and t.NetPnL.Return < 0]
        if not winners or not losers: return None
        avg_w = sum(winners) / len(winners)
        avg_l = sum(losers) / len(losers)
        if avg_l == 0: return None
        return avg_w / avg_l

    @property
    def SharpeRatio(self) -> float | None:
        ret = self.AnnualizedReturn
        vol = self.Volatility
        if ret is None or not vol: return None
        return ret / vol

    @property
    def SortinoRatio(self) -> float | None:
        ret = self.AnnualizedReturn
        if ret is None or not self.Trades: return None
        returns = [t.NetPnL.Return for t in self.Trades if t.NetPnL and t.NetPnL.Return is not None and t.NetPnL.Return < 0]
        if not returns: return None
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        downside_vol = math.sqrt(var) * math.sqrt(252.0)
        if not downside_vol: return None
        return ret / downside_vol

    @property
    def CalmarRatio(self) -> float | None:
        ret = self.AnnualizedReturn
        if ret is None or not self.Trades: return None
        drawdowns = [t.MaxDrawDownPnL.Return for t in self.Trades if t.MaxDrawDownPnL and t.MaxDrawDownPnL.Return is not None]
        if not drawdowns: return None
        max_dd = min(drawdowns)
        if max_dd >= 0: return None
        return ret / abs(max_dd)

    @staticmethod
    def save_accounts(data: AccountAPI | Sequence[AccountAPI], by: str = "Autosave") -> None:
        if isinstance(data, (list, tuple)):
            for acc in data: acc.save(by=by)
        else: data.save(by=by)

    @staticmethod
    def load_accounts(data: AccountAPI | Sequence[AccountAPI]) -> None:
        if isinstance(data, (list, tuple)):
            for acc in data: acc.load()
        else: data.load()

    @staticmethod
    def save_orders(data: OrderAPI | Sequence[OrderAPI], by: str = "Autosave") -> None:
        if isinstance(data, (list, tuple)):
            for ord in data: ord.save(by=by)
        else: data.save(by=by)

    @staticmethod
    def load_orders(data: OrderAPI | Sequence[OrderAPI]) -> None:
        if isinstance(data, (list, tuple)):
            for ord in data: ord.load()
        else: data.load()

    @staticmethod
    def save_positions(data: PositionAPI | Sequence[PositionAPI], by: str = "Autosave") -> None:
        if isinstance(data, (list, tuple)):
            for pos in data: pos.save(by=by)
        else: data.save(by=by)

    @staticmethod
    def load_positions(data: PositionAPI | Sequence[PositionAPI]) -> None:
        if isinstance(data, (list, tuple)):
            for pos in data: pos.load()
        else: data.load()

    @staticmethod
    def save_trades(data: TradeAPI | Sequence[TradeAPI], by: str = "Autosave") -> None:
        if isinstance(data, (list, tuple)):
            for tr in data: tr.save(by=by)
        else: data.save(by=by)

    @staticmethod
    def load_trades(data: TradeAPI | Sequence[TradeAPI]) -> None:
        if isinstance(data, (list, tuple)):
            for tr in data: tr.load()
        else: data.load()

    @staticmethod
    def _from_join_row_(row: dict, cls: type) -> object:
        from Library.Portfolio.Account import AccountAPI
        from Library.Portfolio.Order import OrderAPI
        from Library.Portfolio.Position import PositionAPI
        from Library.Portfolio.Trade import TradeAPI
        from Library.Universe.Provider import ProviderAPI
        from Library.Universe.Security import SecurityAPI
        from Library.Universe.Contract import ContractAPI

        def _extract(prefix: str, api_cls: type):
            if row.get(f"{prefix}_UID") is None: return None
            sub_dict = {k.replace(f"{prefix}_", ""): v for k, v in row.items() if k.startswith(f"{prefix}_")}
            return api_cls.parse(sub_dict)

        main_dict = {k: v for k, v in row.items() if not any(k.startswith(p) for p in ("Provider_", "Security_", "Contract_", "Position_"))}

        if cls is AccountAPI:
            main_dict["Provider"] = _extract("Provider", ProviderAPI)
        elif cls is OrderAPI:
            main_dict["Security"] = _extract("Security", SecurityAPI)
            main_dict["Contract"] = _extract("Contract", ContractAPI)
            main_dict["Position"] = _extract("Position", PositionAPI)
        elif cls is PositionAPI:
            main_dict["Security"] = _extract("Security", SecurityAPI)
            main_dict["Contract"] = _extract("Contract", ContractAPI)
        elif cls is TradeAPI:
            main_dict["Position"] = _extract("Position", PositionAPI)
            main_dict["Security"] = _extract("Security", SecurityAPI)
            main_dict["Contract"] = _extract("Contract", ContractAPI)

        return cls.parse(main_dict)

    @staticmethod
    def pull_accounts(db: DatabaseAPI, as_dataframe: bool = True) -> pl.DataFrame | list[AccountAPI]:
        from Library.Portfolio.Account import AccountAPI
        sql = f'''
        SELECT a.*,
               p."UID" AS "Provider_UID", p."Platform" AS "Provider_Platform", p."Name" AS "Provider_Name", p."Abbreviation" AS "Provider_Abbreviation", p."CreatedAt" AS "Provider_CreatedAt", p."CreatedBy" AS "Provider_CreatedBy", p."UpdatedAt" AS "Provider_UpdatedAt", p."UpdatedBy" AS "Provider_UpdatedBy"
        FROM "{AccountAPI.Schema}"."{AccountAPI.Table}" a
        LEFT JOIN "Universe"."Provider" p ON a."Provider" = p."UID"
        '''
        df = db.executeone(QueryAPI(sql), schema=AccountAPI.Schema, table=AccountAPI.Table).fetchall(legacy=False)
        if as_dataframe: return df
        return [PortfolioAPI._from_join_row_(row, AccountAPI) for row in df.iter_rows(named=True)]

    @staticmethod
    def push_accounts(db: DatabaseAPI, data: pl.DataFrame | list[dict] | tuple | dict) -> None:
        from Library.Portfolio.Account import AccountAPI
        db.upsert(schema=AccountAPI.Schema, table=AccountAPI.Table, data=data, key=["UID"], exclude=["CreatedAt", "CreatedBy"])

    @staticmethod
    def pull_orders(db: DatabaseAPI, start: datetime | None = None, stop: datetime | None = None, as_dataframe: bool = True) -> pl.DataFrame | list[OrderAPI]:
        from Library.Portfolio.Order import OrderAPI
        sql = f'''
        SELECT o.*,
               s."UID" AS "Security_UID", s."Provider" AS "Security_Provider", s."Category" AS "Security_Category", s."Ticker" AS "Security_Ticker", s."Contract" AS "Security_Contract", s."CreatedAt" AS "Security_CreatedAt", s."CreatedBy" AS "Security_CreatedBy", s."UpdatedAt" AS "Security_UpdatedAt", s."UpdatedBy" AS "Security_UpdatedBy",
               c."UID" AS "Contract_UID", c."Ticker" AS "Contract_Ticker", c."Provider" AS "Contract_Provider", c."Type" AS "Contract_Type", c."Digits" AS "Contract_Digits", c."PointSize" AS "Contract_PointSize", c."PipSize" AS "Contract_PipSize", c."LotSize" AS "Contract_LotSize", c."VolumeMin" AS "Contract_VolumeMin", c."VolumeMax" AS "Contract_VolumeMax", c."VolumeStep" AS "Contract_VolumeStep", c."Commission" AS "Contract_Commission", c."CommissionMode" AS "Contract_CommissionMode", c."SwapLong" AS "Contract_SwapLong", c."SwapShort" AS "Contract_SwapShort", c."SwapMode" AS "Contract_SwapMode", c."SwapExtraDay" AS "Contract_SwapExtraDay", c."SwapSummerTime" AS "Contract_SwapSummerTime", c."SwapWinterTime" AS "Contract_SwapWinterTime", c."SwapPeriod" AS "Contract_SwapPeriod", c."Expiry" AS "Contract_Expiry", c."CreatedAt" AS "Contract_CreatedAt", c."CreatedBy" AS "Contract_CreatedBy", c."UpdatedAt" AS "Contract_UpdatedAt", c."UpdatedBy" AS "Contract_UpdatedBy",
               p."UID" AS "Position_UID", p."Security" AS "Position_Security", p."PositionType" AS "Position_PositionType", p."TradeType" AS "Position_TradeType", p."Volume" AS "Position_Volume", p."Quantity" AS "Position_Quantity", p."EntryTimestamp" AS "Position_EntryTimestamp", p."EntryPrice" AS "Position_EntryPrice", p."StopLossPrice" AS "Position_StopLossPrice", p."TakeProfitPrice" AS "Position_TakeProfitPrice", p."MaxRunUpPrice" AS "Position_MaxRunUpPrice", p."MaxDrawDownPrice" AS "Position_MaxDrawDownPrice", p."ExitPrice" AS "Position_ExitPrice", p."StopLossPnL" AS "Position_StopLossPnL", p."TakeProfitPnL" AS "Position_TakeProfitPnL", p."MaxRunUpPnL" AS "Position_MaxRunUpPnL", p."MaxDrawDownPnL" AS "Position_MaxDrawDownPnL", p."GrossPnL" AS "Position_GrossPnL", p."CommissionPnL" AS "Position_CommissionPnL", p."SwapPnL" AS "Position_SwapPnL", p."NetPnL" AS "Position_NetPnL", p."UsedMargin" AS "Position_UsedMargin", p."EntryBalance" AS "Position_EntryBalance", p."MidBalance" AS "Position_MidBalance", p."CreatedAt" AS "Position_CreatedAt", p."CreatedBy" AS "Position_CreatedBy", p."UpdatedAt" AS "Position_UpdatedAt", p."UpdatedBy" AS "Position_UpdatedBy"
        FROM "{OrderAPI.Schema}"."{OrderAPI.Table}" o
        LEFT JOIN "Universe"."Security" s ON o."Security" = s."UID"
        LEFT JOIN "Universe"."Contract" c ON s."Contract" = c."UID"
        LEFT JOIN "Portfolio"."Position" p ON o."Position" = p."UID"
        '''
        params = {}
        if start and stop:
            sql += ' WHERE o."EntryTimestamp" BETWEEN :start: AND :stop:'
            params = {"start": start, "stop": stop}
        df = db.executeone(QueryAPI(sql), **params, schema=OrderAPI.Schema, table=OrderAPI.Table).fetchall(legacy=False)
        if as_dataframe: return df
        return [PortfolioAPI._from_join_row_(row, OrderAPI) for row in df.iter_rows(named=True)]

    @staticmethod
    def push_orders(db: DatabaseAPI, data: pl.DataFrame | list[dict] | tuple | dict) -> None:
        from Library.Portfolio.Order import OrderAPI
        db.upsert(schema=OrderAPI.Schema, table=OrderAPI.Table, data=data, key=["UID"], exclude=["CreatedAt", "CreatedBy"])

    @staticmethod
    def pull_positions(db: DatabaseAPI, start: datetime | None = None, stop: datetime | None = None, as_dataframe: bool = True) -> pl.DataFrame | list[PositionAPI]:
        from Library.Portfolio.Position import PositionAPI
        sql = f'''
        SELECT pos.*,
               s."UID" AS "Security_UID", s."Provider" AS "Security_Provider", s."Category" AS "Security_Category", s."Ticker" AS "Security_Ticker", s."Contract" AS "Security_Contract", s."CreatedAt" AS "Security_CreatedAt", s."CreatedBy" AS "Security_CreatedBy", s."UpdatedAt" AS "Security_UpdatedAt", s."UpdatedBy" AS "Security_UpdatedBy",
               c."UID" AS "Contract_UID", c."Ticker" AS "Contract_Ticker", c."Provider" AS "Contract_Provider", c."Type" AS "Contract_Type", c."Digits" AS "Contract_Digits", c."PointSize" AS "Contract_PointSize", c."PipSize" AS "Contract_PipSize", c."LotSize" AS "Contract_LotSize", c."VolumeMin" AS "Contract_VolumeMin", c."VolumeMax" AS "Contract_VolumeMax", c."VolumeStep" AS "Contract_VolumeStep", c."Commission" AS "Contract_Commission", c."CommissionMode" AS "Contract_CommissionMode", c."SwapLong" AS "Contract_SwapLong", c."SwapShort" AS "Contract_SwapShort", c."SwapMode" AS "Contract_SwapMode", c."SwapExtraDay" AS "Contract_SwapExtraDay", c."SwapSummerTime" AS "Contract_SwapSummerTime", c."SwapWinterTime" AS "Contract_SwapWinterTime", c."SwapPeriod" AS "Contract_SwapPeriod", c."Expiry" AS "Contract_Expiry", c."CreatedAt" AS "Contract_CreatedAt", c."CreatedBy" AS "Contract_CreatedBy", c."UpdatedAt" AS "Contract_UpdatedAt", c."UpdatedBy" AS "Contract_UpdatedBy"
        FROM "{PositionAPI.Schema}"."{PositionAPI.Table}" pos
        LEFT JOIN "Universe"."Security" s ON pos."Security" = s."UID"
        LEFT JOIN "Universe"."Contract" c ON s."Contract" = c."UID"
        '''
        params = {}
        if start and stop:
            sql += ' WHERE pos."EntryTimestamp" BETWEEN :start: AND :stop:'
            params = {"start": start, "stop": stop}
        df = db.executeone(QueryAPI(sql), **params, schema=PositionAPI.Schema, table=PositionAPI.Table).fetchall(legacy=False)
        if as_dataframe: return df
        return [PortfolioAPI._from_join_row_(row, PositionAPI) for row in df.iter_rows(named=True)]

    @staticmethod
    def push_positions(db: DatabaseAPI, data: pl.DataFrame | list[dict] | tuple | dict) -> None:
        from Library.Portfolio.Position import PositionAPI
        db.upsert(schema=PositionAPI.Schema, table=PositionAPI.Table, data=data, key=["UID"], exclude=["CreatedAt", "CreatedBy"])

    @staticmethod
    def pull_trades(db: DatabaseAPI, start: datetime | None = None, stop: datetime | None = None, as_dataframe: bool = True) -> pl.DataFrame | list[TradeAPI]:
        from Library.Portfolio.Trade import TradeAPI
        sql = f'''
        SELECT t.*,
               p."UID" AS "Position_UID", p."Security" AS "Position_Security", p."PositionType" AS "Position_PositionType", p."TradeType" AS "Position_TradeType", p."Volume" AS "Position_Volume", p."Quantity" AS "Position_Quantity", p."EntryTimestamp" AS "Position_EntryTimestamp", p."EntryPrice" AS "Position_EntryPrice", p."StopLossPrice" AS "Position_StopLossPrice", p."TakeProfitPrice" AS "Position_TakeProfitPrice", p."MaxRunUpPrice" AS "Position_MaxRunUpPrice", p."MaxDrawDownPrice" AS "Position_MaxDrawDownPrice", p."ExitPrice" AS "Position_ExitPrice", p."StopLossPnL" AS "Position_StopLossPnL", p."TakeProfitPnL" AS "Position_TakeProfitPnL", p."MaxRunUpPnL" AS "Position_MaxRunUpPnL", p."MaxDrawDownPnL" AS "Position_MaxDrawDownPnL", p."GrossPnL" AS "Position_GrossPnL", p."CommissionPnL" AS "Position_CommissionPnL", p."SwapPnL" AS "Position_SwapPnL", p."NetPnL" AS "Position_NetPnL", p."UsedMargin" AS "Position_UsedMargin", p."EntryBalance" AS "Position_EntryBalance", p."MidBalance" AS "Position_MidBalance", p."CreatedAt" AS "Position_CreatedAt", p."CreatedBy" AS "Position_CreatedBy", p."UpdatedAt" AS "Position_UpdatedAt", p."UpdatedBy" AS "Position_UpdatedBy",
               s."UID" AS "Security_UID", s."Provider" AS "Security_Provider", s."Category" AS "Security_Category", s."Ticker" AS "Security_Ticker", s."Contract" AS "Security_Contract", s."CreatedAt" AS "Security_CreatedAt", s."CreatedBy" AS "Security_CreatedBy", s."UpdatedAt" AS "Security_UpdatedAt", s."UpdatedBy" AS "Security_UpdatedBy",
               c."UID" AS "Contract_UID", c."Ticker" AS "Contract_Ticker", c."Provider" AS "Contract_Provider", c."Type" AS "Contract_Type", c."Digits" AS "Contract_Digits", c."PointSize" AS "Contract_PointSize", c."PipSize" AS "Contract_PipSize", c."LotSize" AS "Contract_LotSize", c."VolumeMin" AS "Contract_VolumeMin", c."VolumeMax" AS "Contract_VolumeMax", c."VolumeStep" AS "Contract_VolumeStep", c."Commission" AS "Contract_Commission", c."CommissionMode" AS "Contract_CommissionMode", c."SwapLong" AS "Contract_SwapLong", c."SwapShort" AS "Contract_SwapShort", c."SwapMode" AS "Contract_SwapMode", c."SwapExtraDay" AS "Contract_SwapExtraDay", c."SwapSummerTime" AS "Contract_SwapSummerTime", c."SwapWinterTime" AS "Contract_SwapWinterTime", c."SwapPeriod" AS "Contract_SwapPeriod", c."Expiry" AS "Contract_Expiry", c."CreatedAt" AS "Contract_CreatedAt", c."CreatedBy" AS "Contract_CreatedBy", c."UpdatedAt" AS "Contract_UpdatedAt", c."UpdatedBy" AS "Contract_UpdatedBy"
        FROM "{TradeAPI.Schema}"."{TradeAPI.Table}" t
        LEFT JOIN "Portfolio"."Position" p ON t."Position" = p."UID"
        LEFT JOIN "Universe"."Security" s ON p."Security" = s."UID"
        LEFT JOIN "Universe"."Contract" c ON s."Contract" = c."UID"
        '''
        params = {}
        if start and stop:
            sql += ' WHERE t."ExitTimestamp" BETWEEN :start: AND :stop:'
            params = {"start": start, "stop": stop}
        df = db.executeone(QueryAPI(sql), **params, schema=TradeAPI.Schema, table=TradeAPI.Table).fetchall(legacy=False)
        if as_dataframe: return df
        return [PortfolioAPI._from_join_row_(row, TradeAPI) for row in df.iter_rows(named=True)]

    @staticmethod
    def push_trades(db: DatabaseAPI, data: pl.DataFrame | list[dict] | tuple | dict) -> None:
        from Library.Portfolio.Trade import TradeAPI
        db.upsert(schema=TradeAPI.Schema, table=TradeAPI.Table, data=data, key=["UID"], exclude=["CreatedAt", "CreatedBy"])