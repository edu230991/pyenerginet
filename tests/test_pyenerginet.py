import pytest
import pandas as pd
from pyenerginet import EnerginetData


@pytest.fixture
def energinetdata():
    return EnerginetData()


@pytest.fixture
def end():
    return pd.Timestamp("today").normalize().tz_localize("UTC")


@pytest.fixture
def start(end):
    return end - pd.Timedelta("2d")


@pytest.mark.parametrize("tz", ["CET", "UTC"])
def test_get_elspot_prices(tz, start, end, energinetdata):
    """Tests that the 'get_elspot_prices' method returns a pandas dataframe
    with multiple rows and columns"""
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    pricedf = energinetdata.get_elspot_prices(start, end)
    assert pricedf.shape[0]
    assert pricedf.shape[1]
    assert isinstance(pricedf.index, pd.DatetimeIndex)
    assert pricedf.index.tz == start.tz


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
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    df = energinetdata.get_production_per_municipality(
        start, end, municipality_no=no, columns=cols
    )
    assert df.shape[0]
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz == start.tz


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
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    df = energinetdata.get_balancing(start, end, area, cols)
    assert df.shape[0]
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz == start.tz
