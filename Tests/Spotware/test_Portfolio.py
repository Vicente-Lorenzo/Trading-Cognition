import pytest
from datetime import datetime, timezone

import Library.Market
import Library.Portfolio

from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAGetAccountListByAccessTokenRes,
    ProtoOATraderRes,
    ProtoOAReconcileRes,
    ProtoOAOrderListRes,
    ProtoOADealListRes,
    ProtoOAGetPositionUnrealizedPnLRes,
    ProtoOACashFlowHistoryListRes
)

def test_accounts_parses_response(spotware):
    res = ProtoOAGetAccountListByAccessTokenRes()
    res.accessToken = "tok"
    a = res.ctidTraderAccount.add(); a.ctidTraderAccountId = 1001; a.isLive = False; a.traderLogin = 9001
    b = res.ctidTraderAccount.add(); b.ctidTraderAccountId = 1002; b.isLive = True; b.traderLogin = 9002
    spotware._responses_.append(res)
    df = spotware.portfolio.accounts()
    assert len(df) == 2
    assert df["AccountId"].to_list() == [1001, 1002]
    assert df["IsLive"].to_list() == [False, True]
    assert type(spotware._sent_[0]).__name__ == "ProtoOAGetAccountListByAccessTokenReq"
    assert spotware._sent_[0].accessToken == "test_token"

def test_trader_scales_balance_by_money_digits(spotware):
    res = ProtoOATraderRes()
    res.ctidTraderAccountId = 123
    t = res.trader
    t.ctidTraderAccountId = 123
    t.traderLogin = 9001
    t.brokerName = "Test Broker"
    t.depositAssetId = 1
    t.balance = 1000000
    t.balanceVersion = 1
    t.managerBonus = 0
    t.ibBonus = 0
    t.nonWithdrawableBonus = 0
    t.swapFree = False
    t.leverageInCents = 10000
    t.totalMarginCalculationType = 0
    t.maxLeverage = 30000
    t.frenchRisk = False
    t.registrationTimestamp = 0
    t.isLimitedRisk = False
    t.moneyDigits = 2
    spotware._responses_.append(res)
    df = spotware.portfolio.trader()
    assert len(df) == 1
    assert df["Balance"][0] == pytest.approx(10000.0)
    assert df["TraderLogin"][0] == 9001
    assert df["BrokerName"][0] == "Test Broker"
    assert df["MoneyDigits"][0] == 2

def test_positions_parses_reconcile(spotware):
    res = ProtoOAReconcileRes()
    res.ctidTraderAccountId = 123
    p = res.position.add()
    p.positionId = 555
    p.tradeData.symbolId = 1
    p.tradeData.volume = 10000
    p.tradeData.tradeSide = 1
    p.tradeData.openTimestamp = 1577836800000
    p.tradeData.label = "L1"
    p.tradeData.comment = "c"
    p.tradeData.guaranteedStopLoss = False
    p.positionStatus = 1
    p.swap = 0
    p.price = 1.05
    p.commission = -200
    p.marginRate = 1.0
    p.mirroringCommission = 0
    p.guaranteedStopLoss = False
    p.usedMargin = 50000
    p.utcLastUpdateTimestamp = 1577836800000
    p.moneyDigits = 2
    spotware._responses_.append(res)
    df = spotware.portfolio.positions()
    assert len(df) == 1
    assert df["PositionId"][0] == 555
    assert df["SymbolId"][0] == 1
    assert df["TradeSide"][0] == 1
    assert df["Volume"][0] == 10000
    assert df["Commission"][0] == pytest.approx(-2.0)
    assert df["UsedMargin"][0] == pytest.approx(500.0)

def test_orders_pending_via_reconcile(spotware):
    res = ProtoOAReconcileRes()
    res.ctidTraderAccountId = 123
    o = res.order.add()
    o.orderId = 1
    o.tradeData.symbolId = 5
    o.tradeData.volume = 1000
    o.tradeData.tradeSide = 2
    o.tradeData.openTimestamp = 1577836800000
    o.tradeData.guaranteedStopLoss = False
    o.orderType = 1
    o.orderStatus = 1
    o.executedVolume = 0
    o.baseSlippagePrice = 0
    o.slippageInPoints = 0
    o.closingOrder = False
    o.timeInForce = 1
    spotware._responses_.append(res)
    df = spotware.portfolio.orders()
    assert len(df) == 1
    assert df["OrderId"][0] == 1
    assert df["SymbolId"][0] == 5
    assert df["TradeSide"][0] == 2
    assert type(spotware._sent_[0]).__name__ == "ProtoOAReconcileReq"

def test_orders_historical_with_range(spotware):
    res = ProtoOAOrderListRes()
    res.ctidTraderAccountId = 123
    o = res.order.add()
    o.orderId = 2
    o.tradeData.symbolId = 7
    o.tradeData.volume = 5000
    o.tradeData.tradeSide = 1
    o.tradeData.openTimestamp = 1577836800000
    o.tradeData.guaranteedStopLoss = False
    o.orderType = 2
    o.orderStatus = 3
    o.executedVolume = 5000
    o.baseSlippagePrice = 0
    o.slippageInPoints = 0
    o.closingOrder = False
    o.timeInForce = 1
    res.hasMore = False
    spotware._responses_.append(res)
    df = spotware.portfolio.orders(
        start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        stop=datetime(2020, 1, 2, tzinfo=timezone.utc)
    )
    assert len(df) == 1
    assert df["OrderId"][0] == 2
    assert type(spotware._sent_[0]).__name__ == "ProtoOAOrderListReq"
    assert spotware._sent_[0].fromTimestamp == 1577836800000

def test_deals_parses_and_scales(spotware):
    res = ProtoOADealListRes()
    res.ctidTraderAccountId = 123
    d = res.deal.add()
    d.dealId = 700
    d.orderId = 1
    d.positionId = 555
    d.volume = 1000
    d.filledVolume = 1000
    d.symbolId = 1
    d.createTimestamp = 1577836800000
    d.executionTimestamp = 1577836800500
    d.utcLastUpdateTimestamp = 1577836800500
    d.executionPrice = 1.05
    d.tradeSide = 1
    d.dealStatus = 2
    d.marginRate = 1.0
    d.commission = -300
    d.baseToUsdConversionRate = 1.0
    d.moneyDigits = 2
    res.hasMore = False
    spotware._responses_.append(res)
    df = spotware.portfolio.deals(
        start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        stop=datetime(2020, 1, 2, tzinfo=timezone.utc)
    )
    assert len(df) == 1
    assert df["DealId"][0] == 700
    assert df["Commission"][0] == pytest.approx(-3.0)
    sent = spotware._sent_[0]
    assert type(sent).__name__ == "ProtoOADealListReq"
    assert sent.maxRows == 1000
    assert sent.fromTimestamp == 1577836800000

def test_pnl_scales_by_money_digits(spotware):
    res = ProtoOAGetPositionUnrealizedPnLRes()
    res.ctidTraderAccountId = 123
    res.moneyDigits = 2
    p = res.positionUnrealizedPnL.add()
    p.positionId = 555
    p.grossUnrealizedPnL = 12500
    p.netUnrealizedPnL = 12300
    spotware._responses_.append(res)
    df = spotware.portfolio.pnl()
    assert len(df) == 1
    assert df["PositionId"][0] == 555
    assert df["GrossUnrealizedPnL"][0] == pytest.approx(125.0)
    assert df["NetUnrealizedPnL"][0] == pytest.approx(123.0)

def test_cashflow_parses_entries(spotware):
    res = ProtoOACashFlowHistoryListRes()
    res.ctidTraderAccountId = 123
    h = res.depositWithdraw.add()
    h.operationType = 1
    h.balanceHistoryId = 1
    h.balance = 100000
    h.delta = 50000
    h.changeBalanceTimestamp = 1577836800000
    h.moneyDigits = 2
    spotware._responses_.append(res)
    df = spotware.portfolio.cashflow(
        start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        stop=datetime(2020, 1, 2, tzinfo=timezone.utc)
    )
    assert len(df) == 1
    assert df["Balance"][0] == pytest.approx(1000.0)
    assert df["Delta"][0] == pytest.approx(500.0)
    assert df["OperationType"][0] == 1
    assert type(spotware._sent_[0]).__name__ == "ProtoOACashFlowHistoryListReq"
