from datetime import datetime, timezone

from Library.Database.Dataframe import pd, pl
from Library.Utility.Service import ServiceAPI
from Library.Utility.Typing import MISSING, Missing
from Library.Spotware.Market import _millis_, _PRICE_SCALE_

class PortfolioAPI(ServiceAPI):
    """Spotware Portfolio interface: accounts, trader, positions, orders, deals, cash flow."""

    def accounts(self, legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches the list of ctid trader accounts available for the current access token.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        def _fetch_():
            request = Protobuf.get("ProtoOAGetAccountListByAccessTokenReq",
                                   accessToken=self._api_._access_token_)
            response = self._api_._send_(request)
            data = [{
                "AccountId": a.ctidTraderAccountId,
                "IsLive": a.isLive,
                "TraderLogin": a.traderLogin,
                "LastClosingDealTimestamp": a.lastClosingDealTimestamp,
                "LastBalanceUpdateTimestamp": a.lastBalanceUpdateTimestamp
            } for a in response.ctidTraderAccount]
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Accounts Operation: Fetched {len(df)} accounts ({timer.result()})")
        return df

    def trader(self, legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches trader (account) information for the active account.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        def _fetch_():
            request = Protobuf.get("ProtoOATraderReq",
                                   ctidTraderAccountId=self._api_._account_id_)
            response = self._api_._send_(request)
            t = response.trader
            money = 10 ** int(getattr(t, "moneyDigits", 2) or 2)
            data = [{
                "AccountId": t.ctidTraderAccountId,
                "TraderLogin": t.traderLogin,
                "BrokerName": t.brokerName,
                "DepositAssetId": t.depositAssetId,
                "Balance": t.balance / money,
                "BalanceVersion": t.balanceVersion,
                "ManagerBonus": t.managerBonus / money,
                "IbBonus": t.ibBonus / money,
                "NonWithdrawableBonus": t.nonWithdrawableBonus / money,
                "AccessRights": t.accessRights,
                "SwapFree": t.swapFree,
                "LeverageInCents": t.leverageInCents,
                "TotalMarginCalculationType": t.totalMarginCalculationType,
                "MaxLeverage": t.maxLeverage,
                "FrenchRisk": t.frenchRisk,
                "AccountType": t.accountType,
                "RegistrationTimestamp": t.registrationTimestamp,
                "IsLimitedRisk": t.isLimitedRisk,
                "LimitedRiskMarginCalculationStrategy": t.limitedRiskMarginCalculationStrategy,
                "MoneyDigits": t.moneyDigits
            }]
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Trader Operation: Fetched trader info ({timer.result()})")
        return df

    def positions(self, legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches all currently open positions for the active account.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        def _fetch_():
            request = Protobuf.get("ProtoOAReconcileReq",
                                   ctidTraderAccountId=self._api_._account_id_)
            response = self._api_._send_(request)
            data = []
            for p in response.position:
                t = p.tradeData
                money = 10 ** int(getattr(p, "moneyDigits", 2) or 2)
                data.append({
                    "PositionId": p.positionId,
                    "SymbolId": t.symbolId,
                    "TradeSide": t.tradeSide,
                    "Volume": t.volume,
                    "OpenTimestamp": datetime.fromtimestamp(t.openTimestamp / 1000, tz=timezone.utc) if t.openTimestamp else None,
                    "Label": t.label,
                    "Comment": t.comment,
                    "PositionStatus": p.positionStatus,
                    "EntryPrice": p.price,
                    "StopLoss": p.stopLoss if p.HasField("stopLoss") else None,
                    "TakeProfit": p.takeProfit if p.HasField("takeProfit") else None,
                    "Swap": p.swap / money,
                    "Commission": p.commission / money,
                    "MarginRate": p.marginRate,
                    "MirroringCommission": p.mirroringCommission / money,
                    "GuaranteedStopLoss": p.guaranteedStopLoss,
                    "UsedMargin": p.usedMargin / money,
                    "LastUpdateTimestamp": datetime.fromtimestamp(p.utcLastUpdateTimestamp / 1000, tz=timezone.utc) if p.utcLastUpdateTimestamp else None,
                    "MoneyDigits": p.moneyDigits
                })
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Positions Operation: Fetched {len(df)} positions ({timer.result()})")
        return df

    def orders(self,
               start: datetime | None = None,
               stop: datetime | None = None,
               legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches orders. If start/stop are provided, fetches historical orders in that range.
        Otherwise returns pending orders from reconcile.
        :param start: Start datetime (UTC) for historical orders.
        :param stop: End datetime (UTC) for historical orders.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        def _fetch_():
            if start is not None:
                stop_ = stop or datetime.now(timezone.utc)
                request = Protobuf.get("ProtoOAOrderListReq",
                                       ctidTraderAccountId=self._api_._account_id_,
                                       fromTimestamp=_millis_(start),
                                       toTimestamp=_millis_(stop_))
                response = self._api_._send_(request)
                orders = response.order
            else:
                request = Protobuf.get("ProtoOAReconcileReq",
                                       ctidTraderAccountId=self._api_._account_id_)
                response = self._api_._send_(request)
                orders = response.order
            data = []
            for o in orders:
                t = o.tradeData
                data.append({
                    "OrderId": o.orderId,
                    "PositionId": o.positionId if o.HasField("positionId") else None,
                    "SymbolId": t.symbolId,
                    "TradeSide": t.tradeSide,
                    "Volume": t.volume,
                    "OpenTimestamp": datetime.fromtimestamp(t.openTimestamp / 1000, tz=timezone.utc) if t.openTimestamp else None,
                    "Label": t.label,
                    "Comment": t.comment,
                    "OrderType": o.orderType,
                    "OrderStatus": o.orderStatus,
                    "ExecutionPrice": o.executionPrice if o.HasField("executionPrice") else None,
                    "ExecutedVolume": o.executedVolume,
                    "LimitPrice": o.limitPrice if o.HasField("limitPrice") else None,
                    "StopPrice": o.stopPrice if o.HasField("stopPrice") else None,
                    "StopLoss": o.stopLoss if o.HasField("stopLoss") else None,
                    "TakeProfit": o.takeProfit if o.HasField("takeProfit") else None,
                    "TimeInForce": o.timeInForce,
                    "ExpirationTimestamp": datetime.fromtimestamp(o.expirationTimestamp / 1000, tz=timezone.utc) if o.HasField("expirationTimestamp") and o.expirationTimestamp else None,
                    "LastUpdateTimestamp": datetime.fromtimestamp(o.utcLastUpdateTimestamp / 1000, tz=timezone.utc) if o.utcLastUpdateTimestamp else None
                })
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Orders Operation: Fetched {len(df)} orders ({timer.result()})")
        return df

    def deals(self,
              start: datetime,
              stop: datetime | None = None,
              rows: int = 1000,
              legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches historical deals (executed trades) for the active account.
        :param start: Start datetime (UTC).
        :param stop: End datetime (UTC, defaults to now).
        :param rows: Maximum number of rows to fetch.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        stop = stop or datetime.now(timezone.utc)
        def _fetch_():
            request = Protobuf.get("ProtoOADealListReq",
                                   ctidTraderAccountId=self._api_._account_id_,
                                   fromTimestamp=_millis_(start),
                                   toTimestamp=_millis_(stop),
                                   maxRows=int(rows))
            response = self._api_._send_(request)
            data = []
            for d in response.deal:
                money = 10 ** int(getattr(d, "moneyDigits", 2) or 2)
                close = d.closePositionDetail if d.HasField("closePositionDetail") else None
                data.append({
                    "DealId": d.dealId,
                    "OrderId": d.orderId,
                    "PositionId": d.positionId,
                    "SymbolId": d.symbolId,
                    "TradeSide": d.tradeSide,
                    "Volume": d.volume,
                    "FilledVolume": d.filledVolume,
                    "CreateTimestamp": datetime.fromtimestamp(d.createTimestamp / 1000, tz=timezone.utc) if d.createTimestamp else None,
                    "ExecutionTimestamp": datetime.fromtimestamp(d.executionTimestamp / 1000, tz=timezone.utc) if d.executionTimestamp else None,
                    "LastUpdateTimestamp": datetime.fromtimestamp(d.utcLastUpdateTimestamp / 1000, tz=timezone.utc) if d.utcLastUpdateTimestamp else None,
                    "ExecutionPrice": d.executionPrice if d.HasField("executionPrice") else None,
                    "DealStatus": d.dealStatus,
                    "MarginRate": d.marginRate,
                    "Commission": d.commission / money,
                    "BaseToUsdConversionRate": d.baseToUsdConversionRate,
                    "GrossPnL": (close.grossProfit / money) if close else None,
                    "Swap": (close.swap / money) if close else None,
                    "ClosePrice": close.closedPrice if close else None,
                    "ClosedVolume": close.closedVolume if close else None,
                    "Balance": (close.balance / money) if close else None,
                    "MoneyDigits": d.moneyDigits
                })
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Deals Operation: Fetched {len(df)} deals ({timer.result()})")
        return df

    def pnl(self, legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches unrealized PnL for all open positions on the active account.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        def _fetch_():
            request = Protobuf.get("ProtoOAGetPositionUnrealizedPnLReq",
                                   ctidTraderAccountId=self._api_._account_id_)
            response = self._api_._send_(request)
            money = 10 ** int(getattr(response, "moneyDigits", 2) or 2)
            data = [{
                "PositionId": p.positionId,
                "GrossUnrealizedPnL": p.grossUnrealizedPnL / money,
                "NetUnrealizedPnL": p.netUnrealizedPnL / money
            } for p in response.positionUnrealizedPnL]
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"PnL Operation: Fetched {len(df)} unrealized PnL rows ({timer.result()})")
        return df

    def cashflow(self,
                 start: datetime,
                 stop: datetime | None = None,
                 legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches deposit / withdraw cash flow history for the active account.
        :param start: Start datetime (UTC).
        :param stop: End datetime (UTC, defaults to now).
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        stop = stop or datetime.now(timezone.utc)
        def _fetch_():
            request = Protobuf.get("ProtoOACashFlowHistoryListReq",
                                   ctidTraderAccountId=self._api_._account_id_,
                                   fromTimestamp=_millis_(start),
                                   toTimestamp=_millis_(stop))
            response = self._api_._send_(request)
            data = []
            for h in response.depositWithdraw:
                money = 10 ** int(getattr(h, "moneyDigits", 2) or 2)
                data.append({
                    "BalanceHistoryId": h.balanceHistoryId,
                    "OperationType": h.operationType,
                    "Balance": h.balance / money,
                    "Delta": h.delta / money,
                    "Equity": (h.equity / money) if h.HasField("equity") else None,
                    "BalanceVersion": h.balanceVersion if h.HasField("balanceVersion") else None,
                    "ChangeBalanceTimestamp": datetime.fromtimestamp(h.changeBalanceTimestamp / 1000, tz=timezone.utc) if h.changeBalanceTimestamp else None,
                    "ExternalNote": h.externalNote if h.HasField("externalNote") else None,
                    "MoneyDigits": h.moneyDigits
                })
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"CashFlow Operation: Fetched {len(df)} cash flow entries ({timer.result()})")
        return df
