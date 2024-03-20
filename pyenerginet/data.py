import pandas as pd
from warnings import warn
from pyenerginet.base import EnerginetBaseClass


class EnerginetData(EnerginetBaseClass):

    def get_elspot_prices(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        price_area: str = None,
        currency: str = "DKK",
    ) -> pd.DataFrame:
        """Downloads power spot prices in Denmark and neighbors from Energinet
        https://www.energidataservice.dk/tso-electricity/Elspotprices

        :param start: dt start
        :param end: dt end
        :param price_area: one in ("DE", "DK1", "DK2", "NO2", "SE3", "SE4").
            defaults to None in which case all are returned
        :param currency: one in ("EUR", "DKK") defaults to "DKK"
        """
        url = self.base_url + "/Elspotprices"
        df = self._pivot_request(
            url,
            start,
            end,
            filters={"PriceArea": price_area},
        )
        df = df.filter(like=currency).squeeze()
        return df

    def get_production_per_municipality(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        municipality_no: int = None,
        columns: str = "all",
    ) -> pd.DataFrame:
        """Downloads production per municipality from Energinet API:
        https://www.energidataservice.dk/tso-electricity/ProductionMunicipalityHour

        :param start: dt start
        :param end: dt end
        :param municipality_no: For munucipality numbers see:
            https://en.wikipedia.org/wiki/List_of_municipalities_of_Denmark
        :param columns: defaults to "all". otherwise list of columns to return.
            You can see the list of columns on the webpage
        """
        url = self.base_url + "/ProductionMunicipalityHour"
        df = self._pivot_request(
            url,
            start,
            end,
            filters={"MunicipalityNo": municipality_no},
            columns=columns,
        )
        return df

    def get_prod_cons(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        price_area: str = None,
        columns: str = "all",
        validated: bool = True,
    ) -> pd.DataFrame:
        """Gets production and consumption data from
        https://www.energidataservice.dk/tso-electricity/ProductionConsumptionSettlement or
        https://www.energidataservice.dk/tso-electricity/ElectricityBalanceNonv
        depending on whether 'validated' is True. Note that the columns returned in the two
        cases are not the same!

        :param start: dt start
        :param end: dt end
        :param price_area: one in ('DK1', 'DK2'), defaults to None, i.e. both
        :param columns: defaults to "all". otherwise list of columns to return.
            You can see the list of columns on the webpage
        :param validated: whether to use validated settlement data or temporary unofficial data
        """
        url = self.base_url + (
            "/ProductionConsumptionSettlement"
            if validated
            else "/ElectricityBalanceNonv"
        )
        df = self._pivot_request(
            url,
            start,
            end,
            filters={"PriceArea": price_area},
            columns=columns,
        )
        return df

    def get_fcr_dk1(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        columns: str = "all",
    ):
        """Downloads DK1 FCR data from Energinet.
        https://www.energidataservice.dk/tso-electricity/FcrDK1
        Full overview of ancillary services:
        https://en.energinet.dk/media/gmci1x5h/ancillary-services-to-be-delivered-in-denmark-tender-conditions-1-2-2024.pdf

        :param start: dt start
        :param end: dt end
        :param columns: defaults to "all". otherwise list of columns to return.
            You can see the list of columns on the webpage
        """
        url = self.base_url + "/FcrDK1"
        df = self._select_columns_request(url, start, end, columns)
        return df

    def get_fcr_dk1_old(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        columns: str = "all",
    ) -> pd.DataFrame:
        """Downloads DK1 FCR data from Energinet using an outdated url (for data pre-2021)
        https://www.energidataservice.dk/tso-electricity/FcrReservesDK1

        :param start: dt start
        :param end: dt end
        :param columns: defaults to "all". otherwise list of columns to return.
            You can see the list of columns on the webpage
        """
        url = self.base_url + "/RegulatingBalancePowerdata"
        df = self._select_columns_request(url, start, end, columns)
        return df

    def get_balancing(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        price_area: str = None,
        columns: str = "all",
    ) -> pd.DataFrame:
        """Gets balancing data from
        https://www.energidataservice.dk/tso-electricity/RegulatingBalancePowerdata

        :param start: dt start
        :param end: dt end
        :param price_area: one in ('DK1', 'DK2'), defaults to None, i.e. both
        :param columns: defaults to "all". otherwise list of columns to return.
            You can see the list of columns on the webpage
        """
        url = self.base_url + "/RegulatingBalancePowerdata"
        df = self._pivot_request(
            url,
            start,
            end,
            filters={"PriceArea": price_area},
            columns=columns,
        )
        return df

    def get_co2_emission(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        price_area: str = None,
        forecast: bool = False,
    ) -> pd.DataFrame:
        """Gets CO2 emission data from
        https://www.energidataservice.dk/tso-electricity/CO2Emis or
        https://www.energidataservice.dk/tso-electricity/CO2EmisProg

        :param start: dt start
        :param end: dt end
        :param price_area: one in ('DK1', 'DK2'), defaults to None, i.e. both
        :param forecast: whether to get forecasted co2 emissions, defaults to False, i.e. realised
        """
        url = self.base_url + "/CO2Emis" + "Prog" * forecast
        df = self._pivot_request(
            url,
            start,
            end,
            filters={"PriceArea": price_area},
            columns="all",
        )
        return df

    def get_consumption_per_industry_code(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        dk_36_code: str = None,
        dk_19_code: str = None,
    ) -> pd.DataFrame:
        """Gets Electricity Consumption per DK36/DK19 Industry Code based on the CVR register.
        https://www.energidataservice.dk/tso-electricity/ConsumptionDK3619codehour
        For info on codes: https://www.dst.dk/da/Statistik/dokumentation/nomenklaturer/db07

        :param start: dt start
        :param end: dt end
        :param dk_36_code: code 36, defaults to None in which case no filter is applied
        :param dk_19_code: code 19, defaults to None in which case no filter is applied
        """
        url = self.base_url + "/ConsumptionDK3619codehour"
        warning_text = (
            "Only one between 'dk_36_code' and 'dk_19_code' can be not None. "
            "Filtering on 'dk_36_code'"
        )

        if dk_36_code is not None and dk_19_code is not None:
            warn(warning_text)
        filters = {
            "DK36Code": dk_36_code,
            "DK19Code": dk_19_code if dk_36_code is None else None,
        }

        df = self._select_columns_request(
            url, start, end, columns="all", filters=filters
        )
        return df

    def get_countertrading_volume(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """Gets countertrading volumes from DK-DE border.
        https://www.energidataservice.dk/tso-electricity/CountertradeIntraday

        :param start: dt start
        :param end: dt end
        """
        url = self.base_url + "/CountertradeIntraday"
        df = self._select_columns_request(url, start, end, "all")
        return df

    def get_realtime_prod_ex(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        price_area: str = None,
        columns: str = "all",
    ) -> pd.DataFrame:
        """Gets real time production and exchanges with 5min granularity.
        https://www.energidataservice.dk/tso-electricity/ElectricityProdex5MinRealtime

        :param start: dt start
        :param end: dt end
        :param price_area: one in ('DK1', 'DK2'), defaults to None, i.e. both
        :param columns: defaults to "all". otherwise list of columns to return.
            You can see the list of columns on the webpage
        """
        url = self.base_url + "/ElectricityProdex5MinRealtime"
        df = self._pivot_request(
            url,
            start,
            end,
            filters={"PriceArea": price_area},
            columns=columns,
        )
        return df

    def get_exchange_flows(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        price_area: str = None,
        columns: str = "all",
    ) -> pd.DataFrame:
        """Gets scheduled exchange flows from
        https://www.energidataservice.dk/tso-electricity/ForeignExchange

        :param start: dt start
        :param end: dt end
        :param price_area: one in ('DK1', 'DK2'), defaults to None, i.e. both
        :param columns: defaults to "all". otherwise list of columns to return.
            You can see the list of columns on the webpage
        """
        url = self.base_url + "/ForeignExchange"
        df = self._pivot_request(
            url,
            start,
            end,
            filters={"PriceArea": price_area},
            columns=columns,
        )
        return df

    def get_res_forecast(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        price_area: str = None,
        tech: str = None,
        resolution: str = "1H",
        columns: str = "all",
    ) -> pd.DataFrame:
        """Gets renewables forecasts from
        https://www.energidataservice.dk/tso-electricity/Forecasts_5Min or
        https://www.energidataservice.dk/tso-electricity/Forecasts_Hour
        depending on the specified granularity.

        :param start: dt start
        :param end: dt end
        :param price_area: one in ('DK1', 'DK2'), defaults to None, i.e. both
        :param tech: which technology to get forecasts for. See webpage for details.
            Defaults to None in which case all are fetched
        :param resolution: resolution of the forecasts. One in ('1H', '5min'), defaults to "1H"
        :param columns: defaults to "all". otherwise list of columns to return.
            You can see the list of columns on the webpage
        """

        suffix = "Hour" if resolution == "1H" else "5Min"
        url = self.base_url + f"/Forecasts_{suffix}"

        df = self._pivot_request(
            url,
            start,
            end,
            filters={"PriceArea": price_area, "ForecastType": tech},
            columns=columns,
        )
        return df

    def get_power_system_now(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        columns: str = "all",
    ) -> pd.DataFrame:
        """Retrieves live power system data from
        https://www.energidataservice.dk/tso-electricity/PowerSystemRightNow

        :param start: dt start
        :param end: dt end
        :param columns: defaults to "all". otherwise list of columns to return.
            You can see the list of columns on the webpage
        """

        url = self.base_url + "/PowerSystemRightNow"
        df = self._select_columns_request(url, start, end, columns)
        return df
