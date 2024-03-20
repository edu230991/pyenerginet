import pytest
import warnings
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
    if isinstance(df, pd.DataFrame):
        # check that 1-column dataframes are turned into series
        assert df.shape[1] != 1


@pytest.mark.parametrize(
    "tz,price_area", [("CET", None), ("CET", "DK1"), ("UTC", "DK2")]
)
def test_get_elspot_prices(tz, price_area, start, end, energinetdata):
    """Tests the 'get_elspot_prices' method"""
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    pricedf = energinetdata.get_elspot_prices(start, end, price_area=price_area)
    assert_timeseries(pricedf, start)


@pytest.mark.parametrize(
    "tz,no,cols",
    [
        (tz, no, c)
        for c in ("all", "SolarMWh")
        for no in (None, 101)
        for tz in ("CET", "UTC")
    ],
)
def test_get_production_per_municipality(tz, no, cols, start, end, energinetdata):
    """Tests the 'get_production_per_municipality' method"""
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    df = energinetdata.get_production_per_municipality(
        start, end, municipality_no=no, columns=cols
    )
    assert_timeseries(df, start)


@pytest.mark.parametrize(
    "tz,area,cols",
    [
        (tz, a, c)
        for c in ("all", "ImbalancePriceEUR", ["ImbalanceMWh", "ImbalancePriceEUR"])
        for a in ("DK1", "DK2", None)
        for tz in ("CET", "UTC")
    ],
)
def test_get_balancing(tz, area, cols, start, end, energinetdata):
    """Tests the 'get_balancing' method"""
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    df = energinetdata.get_balancing(start, end, area, cols)
    assert_timeseries(df, start)


def test_get_fcr(start, end, energinetdata):
    """Tests the 'get_fcr_dk1' method"""
    df = energinetdata.get_fcr_dk1(start, end)
    assert_timeseries(df, start)


@pytest.mark.parametrize(
    "area,cols,validated",
    [
        (None, "all", True),
        (None, "all", False),
        ("DK1", "all", True),
        ("DK1", "all", False),
        ("DK2", "all", True),
        ("DK2", "all", False),
        (None, "CentralPowerMWh", True),
        ("DK2", "CentralPowerMWh", True),
        (None, ["CentralPowerMWh", "ExchangeNO_MWh"], True),
        ("DK1", ["CentralPowerMWh", "ExchangeNO_MWh"], True),
        (None, "TotalLoad", False),
        ("DK1", "TotalLoad", False),
        (None, ["TotalLoad", "OffshoreWindPower"], False),
        ("DK1", ["TotalLoad", "OffshoreWindPower"], False),
    ],
)
def test_get_prod_cons(area, cols, validated, start, end, energinetdata):
    """Tests the 'get_prod_cons' method"""
    df = energinetdata.get_prod_cons(start, end, area, cols, validated)
    assert_timeseries(df, start)


@pytest.mark.parametrize(
    "tz,area,forecast",
    [
        (tz, a, fc)
        for tz in ["CET", "UTC"]
        for a in ["DK1", "DK2"]
        for fc in [True, False]
    ],
)
def test_get_co2_emission(tz, area, forecast, start, end, energinetdata):
    """Tests the 'get_co2_emission' method"""
    start = start.tz_convert(tz)
    end = end.tz_convert(tz)
    df = energinetdata.get_co2_emission(start, end, area, forecast)
    assert_timeseries(df, start)


@pytest.mark.parametrize(
    "code_36,code_19", [("CH", None), (None, "B"), (None, None), ("CH", "B")]
)
def test_get_consumption_per_industry_code(
    code_36, code_19, start, end, energinetdata, recwarn
):
    """Tests the 'get_consumption_per_industry_code' method"""
    df = energinetdata.get_consumption_per_industry_code(start, end, code_36, code_19)
    assert df.shape[0]
    assert isinstance(df, pd.DataFrame)
    if code_36 is not None and code_19 is not None:
        # check that warning is raised when both codes are not None
        assert len(recwarn) == 1


@pytest.mark.parametrize(
    "area,tech,cols,res",
    [
        (a, t, c, r)
        for a in ["DK1", "DK2", None]
        for t in [None, "Offshore Wind", ["Onshore Wind", "Offshore Wind"]]
        for c in ["all", "ForecastCurrent", ["ForecastCurrent", "ForecastDayAhead"]]
        for r in ["1H", "5min"]
    ],
)
def test_get_res_forecast(area, tech, cols, res, start, end, energinetdata):
    """Tests the 'get_res_forecast' method"""
    df = energinetdata.get_res_forecast(start, end, area, tech, res, cols)
    assert df.shape[0]
    if isinstance(df, pd.DataFrame):
        # check that 1-column dataframes are turned into series
        assert df.shape[1] != 1
        if isinstance(df.columns, pd.MultiIndex):
            # check that multi-index level of lenght 1 are dropped
            for level in df.columns.levels:
                assert len(level) > 1


@pytest.mark.parametrize(
    "cols", ["all", "SolarPower", ["SolarPower", "OffshoreWindPower"]]
)
def test_get_power_system_now(cols, start, end, energinetdata):
    """Tests the 'get_power_system_now' method"""
    df = energinetdata.get_power_system_now(start, end, cols)
    assert_timeseries(df, start)
