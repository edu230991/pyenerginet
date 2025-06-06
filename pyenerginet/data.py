import pandas as pd
from warnings import warn
from pyenerginet.base import EnerginetBaseClass


class EnerginetData(EnerginetBaseClass):
    category_dict = {
        "production": ["forecast", "actual"],
        "price": ["day_ahead", "imbalance"],
    }

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

    def get_imbalance_price(
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
        url = self.base_url + "/ImbalancePrice"
        df = self._pivot_request(
            url,
            start,
            end,
            filters={"PriceArea": price_area},
            columns=columns,
        )
        return df

    def get_data(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        variable: str,
        category: str,
        price_area: str = None,
        tech: str = None,
        version: str = None,
    ) -> pd.DataFrame:
        """Easier function to use to get most relevant data

        :param start: dt start
        :param end: dt end
        :param variable: one in ('production', 'price')
        :param category: see self.category_dict for a list of options per variable
        :param price_area: one in ('DK1', 'DK2'), defaults to both
        :param tech: 'ofw' for offshore wind, 'onw' for onshore wind, 'spv' for solar
        :param version:
            For forecasts:
            One in ('ForecastDayAhead', 'ForecastIntraday', 'Forecast5Hour',
            'Forecast1Hour', 'ForecastCurrent') defaults to all
            For actuals: one in ('validated', 'unvalidated', 'bestof'). defaults to 'bestof'
        """
        if variable == "production":
            if tech is None:
                raise KeyError("tech must be specified")
            if category == "forecast":
                cols = "all" if version is None else version
                tech_names = {
                    "ofw": "Offshore Wind",
                    "onw": "Onshore Wind",
                    "spv": "Solar",
                }
                return self.get_res_forecast(
                    start, end, price_area, tech_names[tech], columns=cols
                )
            elif category == "actual":
                # first try to get validated data
                tech_names = {
                    "ofw": "OffshoreWindGe100MW_MWh",
                    "onw": "OnshoreWindGe50kW_MWh",
                    "spv": "SolarPowerGe40kW_MWh",
                }

                validated = version == "bestof" or version == "validated"
                if validated:
                    try:
                        df = self.get_prod_cons(
                            start,
                            end,
                            price_area,
                            validated=validated,
                            columns=tech_names[tech],
                        ).dropna()
                        if version == "bestof":
                            data_complete = df.index[-1] >= end - pd.to_timedelta("1h")
                            if not data_complete:
                                # need to load some unvalidated data to complete
                                validated = False
                    except:
                        validated = False
                        version = "unvalidated"
                        df = pd.DataFrame()
                        warn(
                            UserWarning(
                                "No validated data found for this interval, using unvalidated."
                            )
                        )

                if not validated:
                    if version == "bestof":
                        start = df.index[-1]
                    tech_names = {
                        "ofw": "OffshoreWindPower",
                        "onw": "OnshoreWindPower",
                        "spv": "SolarPower",
                    }
                    df_unval = self.get_prod_cons(
                        start,
                        end,
                        price_area,
                        validated=False,
                        columns=tech_names[tech],
                    )
                    if version == "bestof":
                        # we are asking for best-of version of the data,
                        # so append the two and remove duplicates
                        df = pd.concat([df, df_unval]).dropna()
                        df = df[~df.index.duplicated()].copy()
                    else:
                        df = df_unval.copy()
                return df

        elif variable == "price":
            if category == "day_ahead":
                return self.get_elspot_prices(start, end, price_area)
            elif category == "imbalance":
                cols = [
                    "ImbalancePriceDKK",
                ]
                return self.get_imbalance_price(start, end, price_area, columns=cols)
