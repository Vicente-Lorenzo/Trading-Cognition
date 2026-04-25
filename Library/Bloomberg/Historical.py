import blpapi
from typing import Union
from datetime import date, datetime

from Library.Database.Dataframe import pd, pl
from Library.Utility.Service import ServiceAPI
from Library.Utility.Typing import MISSING, Missing

class HistoricalAPI(ServiceAPI):
    """Bloomberg Historical Data interface."""

    _SERVICE_URI_  = "//blp/refdata"
    _REQUEST_TYPE_ = "HistoricalDataRequest"

    def fetch(self,
              securities: Union[str, list[str]],
              fields: Union[str, list[str]],
              start: Union[str, date, datetime],
              stop: Union[str, date, datetime] = None,
              timeframe: str = "DAILY",
              timeout: int = 0,
              legacy: Union[bool, Missing] = MISSING) -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Fetches historical data for multiple securities and fields.
        :param securities: Security ticker or list of tickers.
        :param fields: Field mnemonic or list of fields.
        :param start: Start date/datetime.
        :param stop: End date/datetime.
        :param timeframe: Periodicity (e.g., DAILY, WEEKLY, MONTHLY).
        :param timeout: Wait time in milliseconds (0 for indefinite).
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        securities = self._api_.flatten(securities)
        fields = self._api_.flatten(fields)
        if isinstance(start, (date, datetime)): start = start.strftime("%Y%m%d")
        if isinstance(stop, (date, datetime)): stop = stop.strftime("%Y%m%d")
        def _fetch_():
            service = self._api_._service_(self._SERVICE_URI_)
            request = service.createRequest(self._REQUEST_TYPE_)
            for sec in securities:
                request.getElement("securities").appendValue(sec)
            for fld in fields:
                request.getElement("fields").appendValue(fld)
            request.set("startDate", start)
            if stop: request.set("endDate", stop)
            request.set("periodicitySelection", str(timeframe).upper())
            self._api_._session_.sendRequest(request)
            data = []
            while True:
                event = self._api_._session_.nextEvent(timeout)
                if event.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE):
                    for msg in event:
                        if not msg.hasElement("securityData"): continue
                        sec_data = msg.getElement("securityData")
                        sec_name = sec_data.getElementAsString("security")
                        fld_array = sec_data.getElement("fieldData")
                        for i in range(fld_array.numValues()):
                            fld_data = fld_array.getValueAsElement(i)
                            row = {"Security": sec_name, "Date": fld_data.getElementAsDatetime("date")}
                            for j in range(fld_data.numElements()):
                                f = fld_data.getElement(j)
                                if str(f.name()) != "date" and not f.isNull(): row[str(f.name())] = f.getValue()
                            data.append(row)
                if event.eventType() == blpapi.Event.RESPONSE: break
                if event.eventType() == blpapi.Event.TIMEOUT:
                    self._log_.warning(lambda: "Fetch Operation: Timeout reached while waiting for response")
                    break
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Fetch Operation: Fetched {len(df)} historical data points ({timer.result()})")
        return df