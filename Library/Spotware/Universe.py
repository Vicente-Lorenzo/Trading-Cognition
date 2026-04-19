from Library.Database.Dataframe import pd, pl
from Library.Utility.Service import ServiceAPI
from Library.Utility.Typing import MISSING, Missing

class UniverseAPI(ServiceAPI):
    """Spotware Universe (static reference) interface: symbols, assets, categories, asset classes."""

    def symbols(self,
                archived: bool = False,
                legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches the light symbol list for the active account.
        :param archived: If True, includes archived symbols.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        def _fetch_():
            request = Protobuf.get("ProtoOASymbolsListReq",
                                   ctidTraderAccountId=self._api_._account_id_,
                                   includeArchivedSymbols=bool(archived))
            response = self._api_._send_(request)
            data = []
            for s in response.symbol:
                data.append({
                    "SymbolId": s.symbolId,
                    "Name": s.symbolName,
                    "Enabled": s.enabled,
                    "BaseAssetId": s.baseAssetId,
                    "QuoteAssetId": s.quoteAssetId,
                    "CategoryId": s.symbolCategoryId,
                    "Description": s.description,
                    "Archived": False
                })
            for s in response.archivedSymbol:
                data.append({
                    "SymbolId": s.symbolId,
                    "Name": s.name,
                    "Enabled": False,
                    "BaseAssetId": None,
                    "QuoteAssetId": None,
                    "CategoryId": None,
                    "Description": s.description,
                    "Archived": True
                })
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Symbols Operation: Fetched {len(df)} symbols ({timer.result()})")
        return df

    def symbol(self,
               ids: int | list[int],
               legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches detailed symbol metadata for one or more symbol ids.
        :param ids: Symbol id or list of ids.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        ids_list = self._api_.flatten(ids)
        def _fetch_():
            request = Protobuf.get("ProtoOASymbolByIdReq",
                                   ctidTraderAccountId=self._api_._account_id_,
                                   symbolId=[int(i) for i in ids_list])
            response = self._api_._send_(request)
            data = []
            for s in list(response.symbol) + list(response.archivedSymbol):
                data.append({
                    "SymbolId": s.symbolId,
                    "Digits": s.digits,
                    "PipPosition": s.pipPosition,
                    "LotSize": s.lotSize,
                    "MinVolume": s.minVolume,
                    "MaxVolume": s.maxVolume,
                    "StepVolume": s.stepVolume,
                    "Commission": s.commission,
                    "CommissionType": s.commissionType,
                    "MinCommission": s.minCommission,
                    "MinCommissionType": s.minCommissionType,
                    "MinCommissionAsset": s.minCommissionAsset,
                    "SwapLong": s.swapLong,
                    "SwapShort": s.swapShort,
                    "SwapCalculationType": s.swapCalculationType,
                    "SwapRollover3Days": s.swapRollover3Days,
                    "SwapPeriod": s.swapPeriod,
                    "SwapTime": s.swapTime,
                    "TradingMode": s.tradingMode,
                    "EnableShortSelling": s.enableShortSelling,
                    "GuaranteedStopLoss": s.guaranteedStopLoss,
                    "MaxExposure": s.maxExposure,
                    "LeverageId": s.leverageId,
                    "PnLConversionFeeRate": s.pnlConversionFeeRate,
                    "ScheduleTimeZone": s.scheduleTimeZone
                })
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Symbol Operation: Fetched {len(df)} symbol details ({timer.result()})")
        return df

    def assets(self, legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches the list of assets for the active account.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        def _fetch_():
            request = Protobuf.get("ProtoOAAssetListReq",
                                   ctidTraderAccountId=self._api_._account_id_)
            response = self._api_._send_(request)
            data = [{
                "AssetId": a.assetId,
                "Name": a.name,
                "DisplayName": a.displayName,
                "Digits": a.digits
            } for a in response.asset]
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Assets Operation: Fetched {len(df)} assets ({timer.result()})")
        return df

    def classes(self, legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches the list of asset classes.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        def _fetch_():
            request = Protobuf.get("ProtoOAAssetClassListReq",
                                   ctidTraderAccountId=self._api_._account_id_)
            response = self._api_._send_(request)
            data = [{
                "AssetClassId": c.id,
                "Name": c.name
            } for c in response.assetClass]
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Classes Operation: Fetched {len(df)} asset classes ({timer.result()})")
        return df

    def categories(self, legacy: bool | Missing = MISSING) -> pd.DataFrame | pl.DataFrame:
        """
        Fetches the list of symbol categories.
        :param legacy: If True, returns Pandas DataFrame; if False, Polars. Defaults to the API setting.
        """
        from ctrader_open_api import Protobuf
        def _fetch_():
            request = Protobuf.get("ProtoOASymbolCategoryListReq",
                                   ctidTraderAccountId=self._api_._account_id_)
            response = self._api_._send_(request)
            data = [{
                "CategoryId": c.id,
                "AssetClassId": c.assetClassId,
                "Name": c.name
            } for c in response.symbolCategory]
            return self._api_.frame(data, legacy=legacy)
        timer, df = super()._fetch_(callback=_fetch_)
        self._log_.info(lambda: f"Categories Operation: Fetched {len(df)} categories ({timer.result()})")
        return df
