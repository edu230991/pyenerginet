import re
import requests
import pandas as pd


class EnerginetBaseClass:
    base_url = "https://api.energidataservice.dk/dataset"

    def _get_params(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        filter_key: str = None,
        filter_value: str = None,
    ) -> dict:
        """Returns standard request parameters

        :param start: dt start
        :param end: dt end
        :param filter_key: defaults to None, in which case no filter is applied
        :param filter_value: defaults to None, in which case no filter is applied
        """
        params = {
            "offset": 0,
            "start": start.tz_convert("CET").strftime("%Y-%m-%dT%H:%M"),
            "end": end.tz_convert("CET").strftime("%Y-%m-%dT%H:%M"),
            "sort": "HourUTC ASC",
        }
        if filter_key is not None:
            params["filter"] = f'{{"{filter_key}":["{filter_value}"]}}'
        return params

    def _base_request(self, url: str, params: dict) -> pd.DataFrame:
        """Makes request and parses dataframe

        :param url: url to request
        :param params: request parameters
        """
        r = requests.get(url, params=params)
        r.raise_for_status()

        df = pd.DataFrame(r.json()["records"])
        df.index = pd.to_datetime(
            df["HourUTC"], format="%Y-%m-%dT%H:%M:%S"
        ).dt.tz_localize("UTC")
        df.index.name = None
        df = df.drop(columns=["HourUTC", "HourDK"])
        if "filter" in params:
            to_drop = re.findall(r'"(.*?)"', params["filter"])[0]
            df = df.drop(columns=to_drop)
        return df

    def _select_columns_request(
        self,
        url: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        columns: str = "all",
        filter_key: str = None,
        filter_value: str = None,
    ):
        params = self._get_params(start, end, filter_key, filter_value)
        df = self._base_request(url, params)
        df = df.tz_convert(start.tz)
        if columns != "all":
            df = df[columns]
        return df
