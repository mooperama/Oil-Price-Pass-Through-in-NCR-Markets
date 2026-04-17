"""
test_pipeline.py — Consolidated unit tests for all pipeline modules.

Tests are organized by module: Brent, DOE, DTI, SQL, Integration.
Row counts are NOT hardcoded — they check structural invariants instead,
so tests pass regardless of whether data was updated with new weeks.
"""

import sys, os, sqlite3
from pathlib import Path
import numpy as np, pandas as pd, pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from functions.brent_pipeline import load_brent, clean_brent, aggregate_weekly, run_brent_eda
from functions.doe_pipeline import load_doe, clean_doe, aggregate_doe_weekly
from functions.dti_pipeline import (
    load_weekly_panel, filter_by_coverage, clean_weekly_panel, aggregate_commodity_weekly)
from functions.sql_pipeline import create_database, validate_database
from functions.integration_pipeline import (
    create_lag_features, compute_fsfi, build_merged_dataset)

SEMI = Path("data/semi_cleaned_data")
CLEAN = Path("data/cleaned_data")

# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="module")
def brent_raw(): return load_brent(SEMI / "Brent_WTI_Prices_2020_to_Present.csv")
@pytest.fixture(scope="module")
def brent_clean(brent_raw): return clean_brent(brent_raw)
@pytest.fixture(scope="module")
def brent_weekly(brent_clean): return aggregate_weekly(brent_clean)
@pytest.fixture(scope="module")
def doe_raw(): return load_doe(SEMI / "DOE_NCR_OilPrices_Compiled.csv")
@pytest.fixture(scope="module")
def doe_clean(doe_raw): return clean_doe(doe_raw)
@pytest.fixture(scope="module")
def doe_weekly(doe_clean): return aggregate_doe_weekly(doe_clean)
@pytest.fixture(scope="module")
def dti_raw(): return load_weekly_panel(SEMI / "dti_weekly_panel.csv")
@pytest.fixture(scope="module")
def dti_filtered(dti_raw): return filter_by_coverage(dti_raw)
@pytest.fixture(scope="module")
def dti_clean(dti_filtered): return clean_weekly_panel(dti_filtered[0])
@pytest.fixture(scope="module")
def dti_weekly(dti_clean): return aggregate_commodity_weekly(dti_clean)
@pytest.fixture(scope="module")
def lag_features(brent_weekly, doe_weekly): return create_lag_features(brent_weekly, doe_weekly)
@pytest.fixture(scope="module")
def fsfi_result(dti_weekly, brent_weekly): return compute_fsfi(dti_weekly, brent_weekly)
@pytest.fixture(scope="module")
def fsfi_df(fsfi_result): return fsfi_result[0]
@pytest.fixture(scope="module")
def fsfi_weights(fsfi_result): return fsfi_result[1]
@pytest.fixture(scope="module")
def fsfi_corrs(fsfi_result): return fsfi_result[2]
@pytest.fixture(scope="module")
def merged(brent_weekly, doe_weekly, dti_weekly, fsfi_df, lag_features):
    return build_merged_dataset(brent_weekly, doe_weekly, dti_weekly, fsfi_df, lag_features)


# ============================================================================
# BRENT TESTS
# ============================================================================

class TestBrentLoad:
    def test_returns_dataframe(self, brent_raw): assert isinstance(brent_raw, pd.DataFrame)
    def test_columns(self, brent_raw):
        for c in ["Date","Brent_Price_USD","WTI_Price_USD","price_flag","Spread_Brent_WTI"]:
            assert c in brent_raw.columns
    def test_datetime(self, brent_raw): assert pd.api.types.is_datetime64_any_dtype(brent_raw.Date)
    def test_no_null_dates(self, brent_raw): assert brent_raw.Date.isna().sum() == 0
    def test_no_null_brent(self, brent_raw): assert brent_raw.Brent_Price_USD.isna().sum() == 0
    def test_numeric(self, brent_raw): assert pd.api.types.is_float_dtype(brent_raw.Brent_Price_USD)
    def test_brent_positive(self, brent_raw): assert (brent_raw.Brent_Price_USD > 0).all()
    def test_wti_negative_event(self, brent_raw):
        assert brent_raw.WTI_Price_USD.min() < 0  # April 2020
    def test_has_data(self, brent_raw): assert len(brent_raw) > 1000
    def test_no_dup_dates(self, brent_raw): assert brent_raw.Date.is_unique
    def test_sorted(self, brent_raw): assert brent_raw.Date.is_monotonic_increasing
    def test_spread(self, brent_raw):
        pd.testing.assert_series_equal(
            brent_raw.Spread_Brent_WTI,
            brent_raw.Brent_Price_USD - brent_raw.WTI_Price_USD, check_names=False)
    def test_flag(self, brent_raw):
        flagged = brent_raw[brent_raw.price_flag == 1]
        assert len(flagged) >= 1

class TestBrentClean:
    def test_no_weekends(self, brent_clean): assert (brent_clean.Date.dt.dayofweek < 5).all()
    def test_log_return_exists(self, brent_clean): assert "brent_log_return" in brent_clean.columns
    def test_first_nan(self, brent_clean): assert pd.isna(brent_clean.brent_log_return.iloc[0])
    def test_finite(self, brent_clean):
        assert np.isfinite(brent_clean.brent_log_return.dropna()).all()
    def test_winsorized(self, brent_clean):
        v = brent_clean.brent_log_return.dropna()
        assert v.min() >= v.quantile(0.01) - abs(v.quantile(0.01))*0.01
    def test_iso_cols(self, brent_clean):
        for c in ["iso_year","iso_week","week_start"]: assert c in brent_clean.columns

class TestBrentWeekly:
    def test_columns(self, brent_weekly):
        for c in ["date","brent_open","brent_close","brent_high","brent_low",
                   "brent_mean","brent_realized_vol","brent_wret"]:
            assert c in brent_weekly.columns
    def test_fewer_rows(self, brent_weekly, brent_clean): assert len(brent_weekly) < len(brent_clean)
    def test_ohlc(self, brent_weekly):
        assert (brent_weekly.brent_high >= brent_weekly.brent_low).all()
    def test_vol_positive(self, brent_weekly):
        assert (brent_weekly.brent_realized_vol.dropna() >= 0).all()
    def test_sorted(self, brent_weekly): assert brent_weekly.date.is_monotonic_increasing

class TestBrentEDA:
    def test_eda_runs(self, brent_clean):
        d = Path("tests/tmp_figs"); d.mkdir(parents=True, exist_ok=True)
        s = run_brent_eda(brent_clean, fig_dir=d)
        assert isinstance(s, dict)
        assert "brent_return_stats" in s
        assert (d/"brent_01_price_levels.png").exists()


# ============================================================================
# DOE TESTS
# ============================================================================

class TestDOELoad:
    def test_dataframe(self, doe_raw): assert isinstance(doe_raw, pd.DataFrame)
    def test_columns(self, doe_raw):
        for c in ["Effectivity Date","City","Product","Brand","Price_Mid"]:
            assert c in doe_raw.columns
    def test_no_col_no(self, doe_raw): assert "No." not in doe_raw.columns
    def test_datetime(self, doe_raw): assert pd.api.types.is_datetime64_any_dtype(doe_raw["Effectivity Date"])
    def test_standardized(self, doe_raw):
        prods = set(doe_raw.Product.unique())
        assert {"Ron91","Ron95","Ron97"} & prods == set()
        assert {"RON 91","RON 95","RON 97","Diesel"}.issubset(prods)
    def test_positive(self, doe_raw): assert (doe_raw["Price Low (P/L)"] > 0).all()
    def test_high_geq_low(self, doe_raw):
        assert (doe_raw["Price High (P/L)"] >= doe_raw["Price Low (P/L)"]).all()
    def test_mid_correct(self, doe_raw):
        exp = (doe_raw["Price Low (P/L)"] + doe_raw["Price High (P/L)"]) / 2
        pd.testing.assert_series_equal(doe_raw.Price_Mid, exp, check_names=False)
    def test_has_data(self, doe_raw): assert len(doe_raw) > 90000

class TestDOEClean:
    def test_dedup(self, doe_clean, doe_raw): assert len(doe_clean) < len(doe_raw)
    def test_no_dups(self, doe_clean):
        assert doe_clean.duplicated(subset=["Effectivity Date","City","Product","Brand"]).sum()==0
    def test_no_nulls(self, doe_clean):
        assert doe_clean.Price_Mid.isna().sum() == 0
    def test_city_norm(self, doe_clean):
        assert "Paranaque City" not in doe_clean.City.values

class TestDOEWeekly:
    def test_columns(self, doe_weekly):
        for c in ["date","Product","price_mid_median","n_stations","doe_log_return"]:
            assert c in doe_weekly.columns
    def test_core_products(self, doe_weekly):
        assert {"Diesel","RON 91","RON 95","RON 97"}.issubset(doe_weekly.Product.unique())
    def test_positive(self, doe_weekly): assert (doe_weekly.n_stations > 0).all()


# ============================================================================
# DTI TESTS
# ============================================================================

class TestDTILoad:
    def test_dataframe(self, dti_raw): assert isinstance(dti_raw, pd.DataFrame)
    def test_columns(self, dti_raw):
        for c in ["date","series_id","commodity","brand","price","log_price"]:
            assert c in dti_raw.columns
    def test_positive(self, dti_raw): assert (dti_raw.price > 0).all()
    def test_log_consistent(self, dti_raw):
        np.testing.assert_allclose(dti_raw.log_price.values, np.log(dti_raw.price).values, rtol=1e-10)
    def test_29_commodities(self, dti_raw): assert dti_raw.commodity.nunique() == 29
    def test_218_sku(self, dti_raw): assert dti_raw.series_id.nunique() == 218

class TestDTIFilter:
    def test_flour_dropped(self, dti_filtered):
        df = dti_filtered[0]
        assert "flour" not in df.commodity.values
        assert "hard flour" not in df.commodity.values
        assert "soft flour" not in df.commodity.values
    def test_26_remain(self, dti_filtered): assert dti_filtered[0].commodity.nunique() == 26
    def test_215_sku_remain(self, dti_filtered): assert dti_filtered[0].series_id.nunique() == 215
    def test_report_29(self, dti_filtered): assert len(dti_filtered[1]) == 29
    def test_key_items_retained(self, dti_filtered):
        actual = set(dti_filtered[0].commodity.unique())
        assert {"canned sardines","instant noodles","coffee","vinegar"}.issubset(actual)
    def test_no_row_loss(self, dti_raw, dti_filtered):
        for c in ["canned sardines","vinegar"]:
            assert len(dti_raw[dti_raw.commodity==c]) == len(dti_filtered[0][dti_filtered[0].commodity==c])

class TestDTIClean:
    def test_monday(self, dti_clean): assert (dti_clean.date.dt.dayofweek == 0).all()
    def test_iso(self, dti_clean):
        for c in ["iso_year","iso_week","week_start"]: assert c in dti_clean.columns
    def test_dlog(self, dti_clean):
        assert "dlog_1w" in dti_clean.columns
        sid = dti_clean.series_id.iloc[0]
        s = dti_clean[dti_clean.series_id==sid].sort_values("date")
        if len(s)>2:
            pd.testing.assert_series_equal(
                s.dlog_1w.reset_index(drop=True),
                s.log_price.diff(1).reset_index(drop=True), check_names=False)

class TestDTIWeekly:
    def test_columns(self, dti_weekly):
        for c in ["date","commodity","category","price_median","dti_dlog"]: assert c in dti_weekly.columns
    def test_26(self, dti_weekly): assert dti_weekly.commodity.nunique() == 26
    def test_category(self, dti_weekly):
        assert dti_weekly[dti_weekly.commodity=="canned sardines"].category.iloc[0] == "basic"
        assert dti_weekly[dti_weekly.commodity=="battery"].category.iloc[0] == "prime"


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestLags:
    def test_dataframe(self, lag_features): assert isinstance(lag_features, pd.DataFrame)
    def test_brent_lags(self, lag_features):
        for k in range(9):
            assert f"brent_ret_lag{k}" in lag_features.columns
    def test_diesel_lags(self, lag_features):
        for k in range(9):
            assert f"diesel_ret_lag{k}" in lag_features.columns
    def test_lag0_current(self, brent_weekly, lag_features):
        ch = pd.merge(brent_weekly[["date","brent_wret"]],
                       lag_features[["date","brent_ret_lag0"]], on="date", how="inner").dropna()
        if len(ch)>0:
            np.testing.assert_allclose(ch.brent_wret.values, ch.brent_ret_lag0.values, rtol=1e-10)
    def test_lag1_shifted(self, lag_features):
        l0 = lag_features.brent_ret_lag0; l1 = lag_features.brent_ret_lag1
        exp = l0.shift(1); mask = exp.notna() & l1.notna()
        if mask.sum()>0: np.testing.assert_allclose(l1[mask].values, exp[mask].values, rtol=1e-10)

class TestFSFI:
    def test_dataframe(self, fsfi_df): assert isinstance(fsfi_df, pd.DataFrame)
    def test_columns(self, fsfi_df):
        for c in ["date","fsfi","fsfi_cumulative"]: assert c in fsfi_df.columns
    def test_weights_sum_1(self, fsfi_weights): assert abs(sum(fsfi_weights.values()) - 1.0) < 1e-8
    def test_positive_weights(self, fsfi_weights):
        for c, w in fsfi_weights.items(): assert w > 0
    def test_positive_corrs(self, fsfi_weights, fsfi_corrs):
        for c in fsfi_weights: assert fsfi_corrs.get(c, 0) > 0
    def test_cumsum(self, fsfi_df):
        np.testing.assert_allclose(fsfi_df.fsfi_cumulative.values, fsfi_df.fsfi.cumsum().values, atol=1e-10)

class TestSQL:
    @pytest.fixture(autouse=True, scope="class")
    def db(self, brent_weekly, doe_weekly, dti_weekly, dti_clean, lag_features, fsfi_df, fsfi_weights):
        p = Path("tests/test.db"); p.parent.mkdir(exist_ok=True)
        if p.exists(): p.unlink()
        create_database(brent_weekly, doe_weekly, dti_weekly, dti_clean, lag_features, fsfi_df, fsfi_weights, db_path=p)
        yield p
        if p.exists(): p.unlink()

    def test_exists(self, db): assert db.exists()
    def test_tables(self, db):
        c = sqlite3.connect(str(db)); tabs = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}; c.close()
        assert {"oil_prices","fuel_prices","food_prices","food_prices_sku","lag_features","fsfi","fsfi_weights","metadata"}.issubset(tabs)
    def test_oil_data(self, db):
        c = sqlite3.connect(str(db)); n = pd.read_sql("SELECT COUNT(*) n FROM oil_prices",c).n[0]; c.close()
        assert n > 300
    def test_join(self, db):
        c = sqlite3.connect(str(db))
        n = pd.read_sql("""SELECT COUNT(*) n FROM oil_prices o
            JOIN fuel_prices f ON o.date=f.date AND f.product='Diesel'
            JOIN food_prices fp ON o.date=fp.date AND fp.commodity='canned sardines'""",c).n[0]; c.close()
        assert n > 100
    def test_sku_table(self, db):
        c = sqlite3.connect(str(db)); n = pd.read_sql("SELECT COUNT(*) n FROM food_prices_sku",c).n[0]; c.close()
        assert n > 30000

class TestMerged:
    def test_dataframe(self, merged): assert isinstance(merged, pd.DataFrame)
    def test_flags(self, merged):
        for c in ["has_brent","has_doe","has_dti","all_sources"]: assert c in merged.columns
    def test_some_full(self, merged): assert (merged.all_sources==1).sum() > 200
    def test_brent(self, merged): assert "brent_close" in merged.columns
    def test_doe(self, merged): assert "doe_diesel" in merged.columns
    def test_dti(self, merged): assert "dti_canned_sardines" in merged.columns
    def test_fsfi(self, merged): assert "fsfi" in merged.columns
    def test_sorted(self, merged): assert merged.date.is_monotonic_increasing
