from datetime import datetime, timezone

from Library.Database.Dataframe import pd, pl
from Library.Utility.Service import ServiceAPI
from Library.Utility.Typing import MISSING, Missing

_PERIOD_MAP_ = {
    "M1": 1, "M2": 2, "M3": 3, "M4": 4, "M5": 5, "M10": 6, "M15": 7, "M30": 8,
    "H1": 9, "H4": 10, "H12": 11, "D1": 12, "W1": 13, "MN1": 14
}
_QUOTE_MAP_ = {"BID": 1, "ASK": 2}
_PRICE_SCALE_ = 100000.0

def _period_(value: str | int) -> int:
    if isinstance(value, int): return value
    return _PERIOD_MAP_[str(value).upper()]

def _quote_(value: str | int) -> int:
    if isinstance(value, int): return value
    return _QUOTE_MAP_[str(value).upper()]

def _millis_(value: datetime) -> int:
    if value.tzinfo is None: value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)

class MarketAPI(ServiceAPI):
    """Spotware Market Data interface: historical bars (trendbars) and intraday ticks."""

    def bars(self,
             symbol: int,
             start: datetime,
             stop: datetime = None,
             period: str | int = "M1",
             count: int | None = None,
             legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches historical trendbars (OHLCV) for a symbol.
        :param symbol: Symbol id.
        :param start: Start datetime (UTC).
        :param stop: End datetime (UTC, defaults to now).
        :param period: Trendbar period (e.g. "M1", "H1", "D1") or raw enum int.
        :param count: Optional maximum number of bars.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        stop = stop or datetime.now(timezone.utc)
        period_id = _period_(period)
        def _fetch_():
            kwargs = {
                "ctidTraderAccountId": self._api_._account_id_,
                "symbolId": int(symbol),
                "period": period_id,
                "fromTimestamp": _millis_(start),
                "toTimestamp": _millis_(stop)
            }
            if count is not None: kwargs["count"] = int(count)
            request = Protobuf.get("ProtoOAGetTrendbarsReq", **kwargs)
            response = self._api_._send_(request)
            data = []
            for bar in response.trendbar:
                low = bar.low
                ts = datetime.fromtimestamp(bar.utcTimestampInMinutes * 60, tz=timezone.utc)
                data.append({
                    "SymbolId": int(symbol),
                    "DateTime": ts,
                    "Open": (low + bar.deltaOpen) / _PRICE_SCALE_,
                    "High": (low + bar.deltaHigh) / _PRICE_SCALE_,
                    "Low": low / _PRICE_SCALE_,
                    "Close": (low + bar.deltaClose) / _PRICE_SCALE_,
                    "Volume": bar.volume,
                    "Period": response.period
                })
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Bars Operation: Fetched {len(df)} trendbars ({timer.result()})")
        return df

    def ticks(self,
              symbol: int,
              start: datetime,
              stop: datetime = None,
              quote: str | int = "BID",
              legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches historical tick data for a symbol. Iterates until all ticks are received.
        :param symbol: Symbol id.
        :param start: Start datetime (UTC).
        :param stop: End datetime (UTC, defaults to now).
        :param quote: Quote type: "BID" or "ASK" (or raw enum int).
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        stop = stop or datetime.now(timezone.utc)
        quote_id = _quote_(quote)
        def _fetch_():
            from_ts = _millis_(start)
            to_ts = _millis_(stop)
            data = []
            while True:
                request = Protobuf.get("ProtoOAGetTickDataReq",
                                       ctidTraderAccountId=self._api_._account_id_,
                                       symbolId=int(symbol),
                                       type=quote_id,
                                       fromTimestamp=from_ts,
                                       toTimestamp=to_ts)
                response = self._api_._send_(request)
                ticks = list(response.tickData)
                if not ticks: break
                cumulative_ts = 0
                cumulative_price = 0
                for i, t in enumerate(ticks):
                    cumulative_ts = t.timestamp if i == 0 else cumulative_ts + t.timestamp
                    cumulative_price = t.tick if i == 0 else cumulative_price + t.tick
                    data.append({
                        "SymbolId": int(symbol),
                        "DateTime": datetime.fromtimestamp(cumulative_ts / 1000, tz=timezone.utc),
                        "Price": cumulative_price / _PRICE_SCALE_,
                        "QuoteType": quote_id
                    })
                if not bool(getattr(response, "hasMore", False)): break
                earliest = min(t_ts for t_ts in (d["DateTime"] for d in data[-len(ticks):]))
                new_to = int(earliest.timestamp() * 1000) - 1
                if new_to <= from_ts: break
                to_ts = new_to
            data.sort(key=lambda r: r["DateTime"])
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Ticks Operation: Fetched {len(df)} ticks ({timer.result()})")
        return df
