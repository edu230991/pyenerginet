import pandas as pd
from importlib.metadata import version
from pyenerginet.base import EnerginetBaseClass

__version__ = version(__package__)


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
            key="PriceArea",
            filter_value=price_area,
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
        :param municipality_no: For munucipality numbers see: https://en.wikipedia.org/wiki/List_of_municipalities_of_Denmark
        :param columns: defaults to "all". otherwise list of columns to return among
            ('OffshoreWindLt100MW_MWh', 'OffshoreWindGe100MW_MWh', 'OnshoreWindMWh', 'ThermalPowerMWh')
        """
        url = self.base_url + "/ProductionMunicipalityHour"
        df = self._pivot_request(
            url,
            start,
            end,
            key="MunicipalityNo",
            filter_value=municipality_no,
            columns=columns,
        )
        return df

    def get_prodcons_settlement(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        price_area: str = None,
        columns: str = "all",
    ) -> pd.DataFrame:
        """Gets production and consumption settlement data from Energinet API
        https://www.energidataservice.dk/tso-electricity/ProductionConsumptionSettlement

        :param start: dt start
        :param end: dt end
        :param price_area: one in ("DE", "DK1", "DK2", "NO2", "SE3", "SE4").
            defaults to None in which case all are returned
        :param columns: defaults to "all". otherwise list of columns to return
        """
        url = self.base_url + "/ProductionConsumptionSettlement"
        filter_key = "PriceArea" if price_area is not None else None
        filter_value = price_area
        df = self._select_columns_request(
            url, start, end, columns, filter_key, filter_value
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
        :param columns: defaults to "all". otherwise list of columns to return
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
        :param columns: defaults to "all". otherwise list of columns to return
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
        :param price_area: one in ('DK1', 'DK2'), defaults to None
        :param columns: defaults to "all"
        """
        url = self.base_url + "/RegulatingBalancePowerdata"
        df = self._pivot_request(
            url,
            start,
            end,
            key="PriceArea",
            filter_value=price_area,
            columns=columns,
        )
        return df
