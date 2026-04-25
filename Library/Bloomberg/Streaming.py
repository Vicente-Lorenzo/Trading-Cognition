import blpapi
from typing import Union, Callable

from Library.Utility.Service import ServiceAPI

class StreamingAPI(ServiceAPI):
    """Bloomberg Streaming Data interface."""

    def subscribe(self,
                  securities: Union[str, list[str]],
                  fields: Union[str, list[str]],
                  callback: Callable,
                  frame: bool = True,
                  limit: int = None,
                  timeout: int = 0) -> None:
        """
        Subscribes to real-time market data updates.
        :param securities: Security ticker or list of tickers.
        :param fields: Field mnemonic or list of fields.
        :param callback: Function to handle incoming data.
        :param frame: If True, callback receives a DataFrame; otherwise a dict.
        :param limit: Number of updates to receive before returning.
        :param timeout: Wait time in milliseconds (0 for indefinite).
        """
        securities = [securities] if isinstance(securities, str) else securities
        fields = [fields] if isinstance(fields, str) else fields
        try:
            self.connect()
            subscriptions = blpapi.SubscriptionList()
            for security in securities:
                subscriptions.add(security, fields, "", blpapi.CorrelationId(security))
            self._api_._session_.subscribe(subscriptions)
            count = 0
            while True:
                try:
                    event = self._api_._session_.nextEvent(timeout)
                    if event.eventType() in [blpapi.Event.SUBSCRIPTION_DATA, blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                        for message in event:
                            security = message.correlationId().value()
                            data = {"Security": security}
                            for field in fields:
                                if message.hasElement(field):
                                    element = message.getElement(field)
                                    if not element.isNull():
                                        data[field] = element.getValue()
                            if len(data) > 1:
                                callback(self.frame([data]) if frame else data)
                                count += 1
                                if limit is not None and count >= limit: return
                    elif event.eventType() == blpapi.Event.TIMEOUT:
                        self._log_.warning(lambda: "Streaming Operation: Timeout reached while waiting for response")
                        return
                except KeyboardInterrupt:
                    self._log_.info(lambda: "Streaming Operation: Interrupted by User")
                    return
        except Exception as e:
            self._log_.error(lambda: "Streaming Operation: Failed")
            self._log_.exception(lambda: str(e))
            raise