import pytest
import pandas as pd
from pyenerginet import EnerginetData


@pytest.fixture
def energinetdata():
    return EnerginetData()


@pytest.fixture
def end():
    return pd.Timestamp("2023-03-27", tz="CET")


@pytest.fixture
def start(end):
    return end - pd.Timedelta("2d")


def assert_timeseries(df: pd.DataFrame, start: pd.Timestamp):
    """Checks that returned data is time-indexed and meets our requirements"""
    assert df.shape[0]
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz == start.tz


@pytest.mark.parametrize(
    "tz,price_area", [("CET", None), ("CET", "DK1"), ("UTC", "DK2")]
)
def test_get_elspot_prices(tz, price_area, start, end, energinetdata):
    """Tests that the 'get_elspot_prices' method returns a pandas dataframe
    with multiple rows and columns"""
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    pricedf = energinetdata.get_elspot_prices(start, end, price_area=price_area)
    assert_timeseries(pricedf, start)


@pytest.mark.parametrize(
    "tz,no,cols",
    [
        (tz, no, cols)
        for cols in ("all", "SolarMWh")
        for no in (None, 101)
        for tz in ("CET", "UTC")
    ],
)
def test_get_production_per_municipality(tz, no, cols, start, end, energinetdata):
    """Tests that the 'get_production_per_municipality' method returns a pandas dataframe
    with multiple rows and columns"""
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    df = energinetdata.get_production_per_municipality(
        start, end, municipality_no=no, columns=cols
    )
    assert_timeseries(df, start)


@pytest.mark.parametrize(
    "tz,area,cols",
    [
        (tz, area, cols)
        for cols in ("all", "ImbalancePriceEUR", ["ImbalanceMWh", "ImbalancePriceEUR"])
        for area in ("DK1", "DK2", None)
        for tz in ("CET", "UTC")
    ],
)
def test_get_balancing(tz, area, cols, start, end, energinetdata):
    """Tests that the 'get_balancing' method returns a pandas dataframe
    with multiple rows and columns"""
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    df = energinetdata.get_balancing(start, end, area, cols)
    assert_timeseries(df, start)


def test_get_fcr(start, end, energinetdata):
    """Tests that the 'get_fcr_dk1' method returns a pandas dataframe
    with multiple rows and columns"""
    df = energinetdata.get_fcr_dk1(start, end)
    assert_timeseries(df, start)


@pytest.mark.parametrize("area", ["DK1", "DK2", None])
def test_get_prodcons_settlement(area, start, end, energinetdata):
    """Tests that the 'get_prodcons_settlement' method returns a pandas dataframe
    with multiple rows and columns"""
    df = energinetdata.get_prodcons_settlement(start, end, area)
    assert_timeseries(df, start)
