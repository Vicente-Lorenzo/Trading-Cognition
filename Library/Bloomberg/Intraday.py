import blpapi
from typing import Union
from datetime import datetime

from Library.Database.Dataframe import pd, pl
from Library.Utility.Service import ServiceAPI
from Library.Utility.Typing import MISSING, Missing

class IntradayAPI(ServiceAPI):
    """Bloomberg Intraday Data interface."""

    def bars(self,
             security: str,
             start: datetime,
             stop: datetime = None,
             interval: int = 1,
             event_type: str = "TRADE",
             timeout: int = 0,
             legacy: Union[bool, Missing] = MISSING) -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Fetches intraday bar data for a security.
        :param security: Security ticker.
        :param start: Start datetime.
        :param stop: End datetime (defaults to now).
        :param interval: Bar interval in minutes.
        :param event_type: Event type (TRADE, BID, ASK, etc.).
        :param timeout: Wait time in milliseconds (0 for indefinite).
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        stop = stop or datetime.now()
        def _fetch_():
            service = self._api_._service_("//blp/refdata")
            request = service.createRequest("IntradayBarRequest")
            request.set("security", security)
            request.set("eventType", event_type)
            request.set("interval", interval)
            request.set("startDateTime", start)
            request.set("endDateTime", stop)
            correlation = blpapi.CorrelationId(security)
            self._api_._session_.sendRequest(request, correlationId=correlation)
            data = []
            while True:
                event = self._api_._session_.nextEvent(timeout)
                if event.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                    for message in event:
                        if message.hasElement("responseError"):
                            error = message.getElement("responseError")
                            self._log_.error(lambda: f"Bars Operation: Response Error - {error.getElementAsString('message')}")
                            continue
                        if not message.hasElement("barData"): continue
                        bar_data = message.getElement("barData")
                        if not bar_data.hasElement("barTickData"): continue
                        bars_array = bar_data.getElement("barTickData")
                        for i in range(bars_array.numValues()):
                            bar = bars_array.getValueAsElement(i)
                            row = {"Security": security}
                            for j in range(bar.numElements()):
                                element = bar.getElement(j)
                                name = str(element.name())
                                row[name.capitalize() if name == "time" else name] = element.getValue()
                            data.append(row)
                    if event.eventType() == blpapi.Event.RESPONSE: break
                elif event.eventType() == blpapi.Event.TIMEOUT:
                    self._log_.warning(lambda: "Bars Operation: Timeout reached while waiting for response")
                    break
            _df_ = self._api_.frame(data, legacy=False)
            if not _df_.is_empty() and "Time" in _df_.columns: _df_ = _df_.drop_nulls(subset=["Time"])
            return self._api_.frame(_df_, legacy=legacy)
        timer, result_df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Bars Operation: Fetched {len(result_df)} bars ({timer.result()})")
        return result_df

    def ticks(self,
              security: str,
              start: datetime,
              stop: datetime = None,
              event_types: Union[str, list[str]] = "TRADE",
              timeout: int = 0,
              legacy: Union[bool, Missing] = MISSING) -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Fetches intraday tick data for a security.
        :param security: Security ticker.
        :param start: Start datetime.
        :param stop: End datetime (defaults to now).
        :param event_types: List of event types (TRADE, BID, ASK, etc.).
        :param timeout: Wait time in milliseconds (0 for indefinite).
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        stop = stop or datetime.now()
        def _fetch_():
            service = self._api_._service_("//blp/refdata")
            request = service.createRequest("IntradayTickRequest")
            request.set("security", security)
            types = [event_types] if isinstance(event_types, str) else event_types
            et_element = request.getElement("eventTypes")
            for et in types:
                et_element.appendValue(et)
            request.set("startDateTime", start)
            if stop: request.set("endDateTime", stop)
            request.set("includeConditionCodes", True)
            correlation = blpapi.CorrelationId(security)
            self._api_._session_.sendRequest(request, correlationId=correlation)
            data = []
            while True:
                event = self._api_._session_.nextEvent(timeout)
                if event.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                    for message in event:
                        if message.hasElement("responseError"):
                            error = message.getElement("responseError")
                            self._log_.error(lambda: f"Ticks Operation: Response Error - {error.getElementAsString('message')}")
                            continue
                        if not message.hasElement("tickData"): continue
                        tick_data_outer = message.getElement("tickData")
                        if not tick_data_outer.hasElement("tickData"): continue
                        ticks_array = tick_data_outer.getElement("tickData")
                        for i in range(ticks_array.numValues()):
                            tick = ticks_array.getValueAsElement(i)
                            row = {"Security": security}
                            for j in range(tick.numElements()):
                                element = tick.getElement(j)
                                name = str(element.name())
                                row[name.capitalize() if name == "time" else name] = element.getValue()
                            data.append(row)
                    if event.eventType() == blpapi.Event.RESPONSE: break
                elif event.eventType() == blpapi.Event.TIMEOUT:
                    self._log_.warning(lambda: "Ticks Operation: Timeout reached while waiting for response")
                    break
            _df_ = self._api_.frame(data, legacy=False)
            if not _df_.is_empty() and "Time" in _df_.columns: _df_ = _df_.drop_nulls(subset=["Time"])
            return self._api_.frame(_df_, legacy=legacy)
        timer, result_df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Ticks Operation: Fetched {len(result_df)} ticks ({timer.result()})")
        return result_df