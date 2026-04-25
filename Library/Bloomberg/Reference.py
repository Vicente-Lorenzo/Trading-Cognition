import blpapi
from typing import Union

from Library.Database.Dataframe import pd, pl
from Library.Utility.Service import ServiceAPI
from Library.Utility.Typing import MISSING, Missing

class ReferenceAPI(ServiceAPI):
    """Bloomberg Reference Data interface."""

    _SERVICE_URI_  = "//blp/refdata"
    _REQUEST_TYPE_ = "ReferenceDataRequest"

    def fetch(self,
              securities: Union[str, list[str]],
              fields: Union[str, list[str]],
              overrides: dict[str, str] = None,
              timeout: int = 0,
              legacy: Union[bool, Missing] = MISSING) -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Fetches reference data for multiple securities and fields.
        :param securities: Security ticker or list of tickers.
        :param fields: Field mnemonic or list of fields.
        :param overrides: Dictionary of field overrides (e.g., {'VWAP_START_TIME': '09:30:00'}).
        :param timeout: Wait time in milliseconds (0 for indefinite).
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        securities = self._api_.flatten(securities)
        fields = self._api_.flatten(fields)
        def _fetch_():
            service = self._api_._service_(self._SERVICE_URI_)
            request = service.createRequest(self._REQUEST_TYPE_)
            for sec in securities:
                request.getElement("securities").appendValue(sec)
            for fld in fields:
                request.getElement("fields").appendValue(fld)
            if overrides:
                ovr = request.getElement("overrides")
                for k, v in overrides.items():
                    o = ovr.appendElement()
                    o.setElement("fieldId", k)
                    o.setElement("value", v)
            self._api_._session_.sendRequest(request)
            data = []
            while True:
                event = self._api_._session_.nextEvent(timeout)
                if event.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE):
                    for msg in event:
                        if not msg.hasElement("securityData"): continue
                        sec_array = msg.getElement("securityData")
                        for i in range(sec_array.numValues()):
                            sec_data = sec_array.getValueAsElement(i)
                            row = {"Security": sec_data.getElementAsString("security")}
                            if sec_data.hasElement("securityError"):
                                row["Error"] = sec_data.getElement("securityError").getElementAsString("message")
                            else:
                                fld_data = sec_data.getElement("fieldData")
                                for j in range(fld_data.numElements()):
                                    f = fld_data.getElement(j)
                                    if not f.isNull(): row[str(f.name())] = f.getValue()
                            data.append(row)
                if event.eventType() == blpapi.Event.RESPONSE: break
                if event.eventType() == blpapi.Event.TIMEOUT:
                    self._log_.warning(lambda: "Fetch Operation: Timeout reached while waiting for response")
                    break
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Fetch Operation: Fetched {len(df)} reference data points ({timer.result()})")
        return df