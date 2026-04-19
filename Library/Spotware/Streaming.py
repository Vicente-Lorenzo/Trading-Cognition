import threading
from typing import Callable

from Library.Utility.Service import ServiceAPI
from Library.Spotware.Market import _PRICE_SCALE_, _period_

_SPOT_EVENT_ = "ProtoOASpotEvent"
_DEPTH_EVENT_ = "ProtoOADepthEvent"

class StreamingAPI(ServiceAPI):
    """Spotware Streaming Data interface: live spots, depth quotes, and trendbars."""

    def spots(self,
              symbols: int | list[int],
              callback: Callable,
              frame: bool = True,
              limit: int | None = None,
              timeout: int | None = None) -> None:
        """
        Subscribes to live spot (bid/ask) events for the given symbols.
        :param symbols: Symbol id or list of symbol ids.
        :param callback: Function to handle incoming data.
        :param frame: If True, callback receives a DataFrame; otherwise a dict.
        :param limit: Number of updates to receive before returning.
        :param timeout: Seconds to wait before returning (None for indefinite).
        """
        from ctrader_open_api import Protobuf
        ids = [int(i) for i in self._api_.flatten(symbols)]
        try:
            self.connect()
            counter = {"n": 0}
            done = threading.Event()
            def handler(message):
                if done.is_set(): return
                payload = Protobuf.extract(message)
                if type(payload).__name__ != _SPOT_EVENT_: return
                if int(payload.symbolId) not in ids: return
                data = {"SymbolId": int(payload.symbolId)}
                if payload.HasField("bid"): data["Bid"] = payload.bid / _PRICE_SCALE_
                if payload.HasField("ask"): data["Ask"] = payload.ask / _PRICE_SCALE_
                if payload.HasField("timestamp"): data["Timestamp"] = int(payload.timestamp)
                if len(data) > 1:
                    callback(self._api_.frame([data]) if frame else data)
                    counter["n"] += 1
                    if limit is not None and counter["n"] >= limit: done.set()
            self._api_._subscribe_(handler)
            try:
                request = Protobuf.get("ProtoOASubscribeSpotsReq",
                                       ctidTraderAccountId=self._api_._account_id_,
                                       symbolId=ids)
                self._api_._send_(request)
                done.wait(timeout=timeout)
            finally:
                self._api_._unsubscribe_(handler)
                try:
                    unsub = Protobuf.get("ProtoOAUnsubscribeSpotsReq",
                                         ctidTraderAccountId=self._api_._account_id_,
                                         symbolId=ids)
                    self._api_._send_(unsub)
                except Exception: pass
        except KeyboardInterrupt:
            self._log_.info(lambda: "Spots Operation: Interrupted by User")
        except Exception as e:
            self._log_.error(lambda: "Spots Operation: Failed")
            self._log_.exception(lambda: str(e))
            raise

    def depth(self,
              symbols: int | list[int],
              callback: Callable,
              frame: bool = True,
              limit: int | None = None,
              timeout: int | None = None) -> None:
        """
        Subscribes to live depth-of-book quote updates for the given symbols.
        :param symbols: Symbol id or list of symbol ids.
        :param callback: Function to handle incoming data.
        :param frame: If True, callback receives a DataFrame; otherwise a dict.
        :param limit: Number of updates to receive before returning.
        :param timeout: Seconds to wait before returning (None for indefinite).
        """
        from ctrader_open_api import Protobuf
        ids = [int(i) for i in self._api_.flatten(symbols)]
        try:
            self.connect()
            counter = {"n": 0}
            done = threading.Event()
            def handler(message):
                if done.is_set(): return
                payload = Protobuf.extract(message)
                if type(payload).__name__ != _DEPTH_EVENT_: return
                if int(payload.symbolId) not in ids: return
                rows = []
                for q in payload.newQuotes:
                    rows.append({
                        "SymbolId": int(payload.symbolId),
                        "QuoteId": int(q.id),
                        "Size": int(q.size),
                        "Bid": (q.bid / _PRICE_SCALE_) if q.HasField("bid") else None,
                        "Ask": (q.ask / _PRICE_SCALE_) if q.HasField("ask") else None,
                        "Action": "New"
                    })
                for qid in payload.deletedQuotes:
                    rows.append({
                        "SymbolId": int(payload.symbolId),
                        "QuoteId": int(qid),
                        "Size": None,
                        "Bid": None,
                        "Ask": None,
                        "Action": "Deleted"
                    })
                if not rows: return
                callback(self._api_.frame(rows) if frame else rows)
                counter["n"] += 1
                if limit is not None and counter["n"] >= limit: done.set()
            self._api_._subscribe_(handler)
            try:
                request = Protobuf.get("ProtoOASubscribeDepthQuotesReq",
                                       ctidTraderAccountId=self._api_._account_id_,
                                       symbolId=ids)
                self._api_._send_(request)
                done.wait(timeout=timeout)
            finally:
                self._api_._unsubscribe_(handler)
                try:
                    unsub = Protobuf.get("ProtoOAUnsubscribeDepthQuotesReq",
                                         ctidTraderAccountId=self._api_._account_id_,
                                         symbolId=ids)
                    self._api_._send_(unsub)
                except Exception: pass
        except KeyboardInterrupt:
            self._log_.info(lambda: "Depth Operation: Interrupted by User")
        except Exception as e:
            self._log_.error(lambda: "Depth Operation: Failed")
            self._log_.exception(lambda: str(e))
            raise

    def bars(self,
             symbol: int,
             period: str | int,
             callback: Callable,
             frame: bool = True,
             limit: int | None = None,
             timeout: int | None = None) -> None:
        """
        Subscribes to live trendbar updates for a symbol. Trendbars arrive inside spot events.
        :param symbol: Symbol id.
        :param period: Trendbar period (e.g. "M1", "H1", "D1") or raw enum int.
        :param callback: Function to handle incoming data.
        :param frame: If True, callback receives a DataFrame; otherwise a dict.
        :param limit: Number of updates to receive before returning.
        :param timeout: Seconds to wait before returning (None for indefinite).
        """
        from ctrader_open_api import Protobuf
        sid = int(symbol)
        period_id = _period_(period)
        try:
            self.connect()
            counter = {"n": 0}
            done = threading.Event()
            def handler(message):
                if done.is_set(): return
                payload = Protobuf.extract(message)
                if type(payload).__name__ != _SPOT_EVENT_: return
                if int(payload.symbolId) != sid: return
                for bar in payload.trendbar:
                    if int(bar.period) != period_id: continue
                    low = bar.low
                    data = {
                        "SymbolId": sid,
                        "Period": int(bar.period),
                        "UtcTimestampInMinutes": int(bar.utcTimestampInMinutes),
                        "Open": (low + bar.deltaOpen) / _PRICE_SCALE_,
                        "High": (low + bar.deltaHigh) / _PRICE_SCALE_,
                        "Low": low / _PRICE_SCALE_,
                        "Close": (low + bar.deltaClose) / _PRICE_SCALE_,
                        "Volume": int(bar.volume)
                    }
                    callback(self._api_.frame([data]) if frame else data)
                    counter["n"] += 1
                    if limit is not None and counter["n"] >= limit:
                        done.set()
                        return
            self._api_._subscribe_(handler)
            try:
                request = Protobuf.get("ProtoOASubscribeLiveTrendbarReq",
                                       ctidTraderAccountId=self._api_._account_id_,
                                       period=period_id,
                                       symbolId=sid)
                self._api_._send_(request)
                done.wait(timeout=timeout)
            finally:
                self._api_._unsubscribe_(handler)
                try:
                    unsub = Protobuf.get("ProtoOAUnsubscribeLiveTrendbarReq",
                                         ctidTraderAccountId=self._api_._account_id_,
                                         period=period_id,
                                         symbolId=sid)
                    self._api_._send_(unsub)
                except Exception: pass
        except KeyboardInterrupt:
            self._log_.info(lambda: "Live Bars Operation: Interrupted by User")
        except Exception as e:
            self._log_.error(lambda: "Live Bars Operation: Failed")
            self._log_.exception(lambda: str(e))
            raise
