import re
import requests
import pandas as pd


class EnerginetBaseClass:
    base_url = "https://api.energidataservice.dk/dataset"

    def _get_params(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        filters: dict = {},
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
        }
        filter_list = []
        for k, v in filters.items():
            if v is not None:
                v = [v] if not isinstance(v, list) else v
                vlist = '","'.join([str(vv) for vv in v])
                filter_list.append(f'"{k}":["{vlist}"]')
        if len(filter_list):
            params["filter"] = "{" + ",".join(filter_list) + "}"
        return params

    def _base_request(self, url: str, params: dict) -> pd.DataFrame:
        """Makes request and parses dataframe

        :param url: url to request
        :param params: request parameters
        """
        r = requests.get(url, params=params)
        r.raise_for_status()

        df = pd.DataFrame(r.json()["records"])

        time_cols = [col for col in df.columns if "UTC" in col]
        dk_time_cols = [col.replace("UTC", "DK") for col in time_cols]
        for col in time_cols:
            df[col] = pd.to_datetime(
                df[col], format="%Y-%m-%dT%H:%M:%S"
            ).dt.tz_localize("UTC")

        df = df.set_index(time_cols, drop=True).drop(columns=dk_time_cols)
        df.index.names = [col.replace("UTC", "") for col in time_cols]

        if "filter" in params:
            to_drop = re.findall(r'"(.*?)"', params["filter"])[0]

            df = df.drop(columns=to_drop)
        return df.sort_index()

    def _select_columns_request(
        self,
        url: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        columns: str = "all",
        filters: dict = {},
    ) -> pd.DataFrame:
        """Runs base request for given url formatting dataframe as timeseries
        and selecting a subset of columns.

        :param url: url from energinet to call
        :param start: dt start
        :param end: dt end
        :param columns: defaults to "all"
        :param filter_key: column name to apply filter to, defaults to None
        :param filter_value: value of filter to apply, defaults to None
        """
        params = self._get_params(start, end, filters)
        df = self._base_request(url, params)

        level = 0 if isinstance(df.index, pd.MultiIndex) else None
        df = df.tz_convert(start.tz, level=level).truncate(start, end)

        if columns != "all":
            df = df[columns]
        return df.squeeze()

    def _pivot_request(
        self,
        url: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        columns: str = "all",
        filters: dict = {},
    ) -> pd.DataFrame:
        """Runs base request for given url formatting dataframe as timeseries
        and selecting a subset of columns, after pivoting in to make the data time-indexed.

        :param url: url from energinet to call
        :param start: dt start
        :param end: dt end
        :param columns: defaults to "all"
        :param filters: dictionary with filters to apply.
            DataFrame will be pivoted around column names (keys) for which values are None.
            DataFrame will be filtered where the columns specified in the keys are equal to
            the values
        """
        df = self._select_columns_request(url, start, end, "all", filters)

        if isinstance(df, pd.DataFrame):
            pivot_keys = [key for key in list(filters.keys()) if key in df.columns]
            if pivot_keys:
                df = df.pivot(columns=pivot_keys)
            if columns != "all":
                df = df[columns]

        # make sure there is no useless multiindex
        if isinstance(df, pd.DataFrame):
            if isinstance(df.columns, pd.MultiIndex):
                for i in range(len(df.columns.levels)):
                    if len(df.columns.levels[i]) == 1:
                        df = df.droplevel(i, axis=1)
        return df.squeeze()
