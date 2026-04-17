from __future__ import annotations

# """
# brent_pipeline.py
# =================
# Module 1: Global oil prices — extraction, cleaning, and EDA.

# Data Source : U.S. Energy Information Administration (EIA) API v2
# Frequency   : Daily (trading days only)
# Series      : Brent Crude (RBRTE), West Texas Intermediate (RWTC)

# Design Notes:
#     - Log-returns winsorized at 1st/99th percentile to mitigate micro-
#       structure noise while preserving genuine shock events
#       (Cochrane, 2005: Asset Pricing, Ch. 20).
#     - Realized volatility computed via sum of squared intraday returns
#       (Andersen & Bollerslev, 1998: "Answering the Skeptics").
#     - Regime detection via rolling volatility windows
#       (20/60/120/250 days), consistent with the Brent EDA findings.

# References:
#     Andersen, T.G. & Bollerslev, T. (1998). Int'l Economic Review, 39(4).
#     Cochrane, J.H. (2005). Asset Pricing (Rev. Ed.). Princeton.
#     Hamilton, J.D. (2003). J. Monetary Economics, 50(2), 363-398.
# """

# from __future__ import annotations

# import os
# import re
# import warnings
# from pathlib import Path
# from typing import Optional, Tuple

# import numpy as np
# import pandas as pd

# warnings.filterwarnings("ignore", category=FutureWarning)

# # Try importing visualization libraries (optional for headless runs)
# try:
#     import matplotlib.pyplot as plt
#     import seaborn as sns
#     HAS_VIZ = True
# except ImportError:
#     HAS_VIZ = False

# # Optional: EIA API support
# try:
#     import requests
#     from dotenv import load_dotenv
#     load_dotenv()
#     HAS_API = True
# except ImportError:
#     HAS_API = False


# # ============================================================================
# # CONFIGURATION
# # ============================================================================

# BRENT_RAW_DIR = Path("data/brent_prices")
# BRENT_CLEANED_DIR = Path("cleaned_data")
# FIG_DIR = Path("figures/brent")

# # Price plausibility bounds (USD/bbl) for 2020-2026
# PRICE_FLOOR = 5.0    # COVID-era trough was ~$9
# PRICE_CEILING = 200.0


# # ============================================================================
# # 1. EXTRACTION
# # ============================================================================

# def fetch_eia_prices(series_id: str, price_name: str) -> pd.DataFrame:
#     """
#     Fetch daily spot prices from the EIA API v2.

#     Parameters
#     ----------
#     series_id : str
#         EIA series identifier ('RBRTE' for Brent, 'RWTC' for WTI).
#     price_name : str
#         Column name for the price series.

#     Returns
#     -------
#     pd.DataFrame
#         Columns: ['Date', price_name]
#     """
#     if not HAS_API:
#         raise RuntimeError("requests and python-dotenv required for API access.")

#     api_key = os.getenv("EIA_API_KEY")
#     start_date = os.getenv("START_DATE", "2020-01-01")

#     if not api_key:
#         raise ValueError("EIA_API_KEY not found in environment variables.")

#     print(f"  Fetching {price_name} from EIA (start={start_date})...")

#     url = (
#         f"https://api.eia.gov/v2/petroleum/pri/spt/data/"
#         f"?api_key={api_key}"
#         f"&frequency=daily"
#         f"&data[0]=value"
#         f"&facets[series][]={series_id}"
#         f"&start={start_date}"
#         f"&sort[0][column]=period"
#         f"&sort[0][direction]=desc"
#         f"&length=5000"
#     )

#     response = requests.get(url, timeout=30)
#     response.raise_for_status()

#     data = response.json()["response"]["data"]
#     df = pd.DataFrame(data)[["period", "value"]]
#     df.columns = ["Date", price_name]
#     df["Date"] = pd.to_datetime(df["Date"])
#     if df["Date"].dt.tz is not None:
#         df["Date"] = df["Date"].dt.tz_localize(None)

#     df[price_name] = pd.to_numeric(df[price_name], errors="coerce")
#     print(f"    → {len(df)} observations retrieved.")
#     return df


# def download_brent_wti(output_dir: Path = BRENT_RAW_DIR) -> pd.DataFrame:
#     """
#     Download and merge Brent + WTI daily prices from EIA API.
#     Saves to CSV in output_dir.
#     """
#     output_dir.mkdir(parents=True, exist_ok=True)

#     df_brent = fetch_eia_prices("RBRTE", "Brent_Price_USD")
#     df_wti = fetch_eia_prices("RWTC", "WTI_Price_USD")

#     df = pd.merge(df_brent, df_wti, on="Date", how="inner")
#     df = df.sort_values("Date").reset_index(drop=True)

#     out_path = output_dir / "Brent_WTI_Prices_2020_to_Present.csv"
#     df.to_csv(out_path, index=False)
#     print(f"  Saved raw data → {out_path} ({len(df)} rows)")
#     return df


# # ============================================================================
# # 2. LOADING & VALIDATION
# # ============================================================================

# def load_brent(filepath: str | Path) -> pd.DataFrame:
#     """
#     Load Brent/WTI CSV and perform initial validation.

#     Checks:
#         - Date parsing and monotonicity
#         - Price positivity and plausibility ($5-$200 range)
#         - Duplicate dates (keeps last — revision convention)
#     """
#     df = pd.read_csv(filepath)
#     df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
#     df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

#     for col in ["Brent_Price_USD", "WTI_Price_USD"]:
#         df[col] = pd.to_numeric(df[col], errors="coerce")

#     n_before = len(df)
#     df = df.dropna(subset=["Brent_Price_USD", "WTI_Price_USD"])
#     if len(df) < n_before:
#         print(f"  [LOAD] Dropped {n_before - len(df)} rows with unparseable prices.")

#     # Flag implausible prices (but retain — e.g., COVID $9 crash is real)
#     df["price_flag"] = (
#         (df["Brent_Price_USD"] < PRICE_FLOOR) |
#         (df["Brent_Price_USD"] > PRICE_CEILING) |
#         (df["WTI_Price_USD"] < PRICE_FLOOR) |
#         (df["WTI_Price_USD"] > PRICE_CEILING)
#     ).astype(int)

#     n_flagged = df["price_flag"].sum()
#     if n_flagged > 0:
#         print(f"  [LOAD] {n_flagged} observations flagged (outside ${PRICE_FLOOR}-${PRICE_CEILING}).")

#     # Deduplicate on date
#     df = df.drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)

#     # Brent-WTI spread (from EDA: spread dynamics)
#     df["Spread_Brent_WTI"] = df["Brent_Price_USD"] - df["WTI_Price_USD"]

#     print(f"  [LOAD] {len(df)} daily obs: {df.Date.min().date()} → {df.Date.max().date()}")
#     return df


# # ============================================================================
# # 3. CLEANING
# # ============================================================================

# def clean_brent(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Deep cleaning of daily Brent/WTI prices.

#     Operations:
#         1. Remove weekends (Brent trades Mon-Fri only).
#         2. Compute log-returns with 1%/99% winsorization.
#         3. Add ISO week/year for downstream temporal alignment.
#         4. Compute Brent-WTI spread statistics.
#     """
#     df = df.copy()

#     # Remove weekend rows (data errors or carry-forwards)
#     weekday_mask = df["Date"].dt.dayofweek < 5
#     n_weekend = (~weekday_mask).sum()
#     if n_weekend > 0:
#         print(f"  [CLEAN] Removing {n_weekend} weekend observations.")
#         df = df[weekday_mask]

#     # Daily log-returns
#     df["brent_log_return"] = np.log(df["Brent_Price_USD"]).diff()
#     df["wti_log_return"] = np.log(df["WTI_Price_USD"]).diff()

#     # Winsorize at 1st/99th percentile (robust to flash crashes)
#     for col in ["brent_log_return", "wti_log_return"]:
#         valid = df[col].dropna()
#         if len(valid) > 0:
#             p01, p99 = valid.quantile(0.01), valid.quantile(0.99)
#             df[col] = df[col].clip(lower=p01, upper=p99)

#     # ISO week columns for temporal alignment
#     df["iso_year"] = df["Date"].dt.isocalendar().year.astype(int)
#     df["iso_week"] = df["Date"].dt.isocalendar().week.astype(int)
#     df["week_start"] = df["Date"].dt.to_period("W-SUN").apply(lambda x: x.start_time)

#     print(f"  [CLEAN] {len(df)} trading days after cleaning.")
#     return df.reset_index(drop=True)


# def aggregate_weekly(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Aggregate daily Brent data to weekly frequency (Monday-anchored).

#     Computes OHLC, mean, median, realized volatility, and weekly returns.
#     Realized volatility: sqrt(Σ r²_daily) per Andersen & Bollerslev (1998).
#     """
#     weekly = df.groupby("week_start").agg(
#         brent_open=("Brent_Price_USD", "first"),
#         brent_close=("Brent_Price_USD", "last"),
#         brent_high=("Brent_Price_USD", "max"),
#         brent_low=("Brent_Price_USD", "min"),
#         brent_mean=("Brent_Price_USD", "mean"),
#         brent_median=("Brent_Price_USD", "median"),
#         wti_close=("WTI_Price_USD", "last"),
#         wti_mean=("WTI_Price_USD", "mean"),
#         spread_mean=("Spread_Brent_WTI", "mean"),
#         n_trading_days=("Brent_Price_USD", "count"),
#         brent_realized_vol=("brent_log_return", lambda x: np.sqrt((x**2).sum())),
#     ).reset_index()

#     weekly.rename(columns={"week_start": "date"}, inplace=True)

#     # Weekly close-to-close log-returns
#     weekly["brent_wret"] = np.log(weekly["brent_close"]).diff()
#     weekly["wti_wret"] = np.log(weekly["wti_close"]).diff()

#     # Winsorize weekly returns
#     for col in ["brent_wret", "wti_wret"]:
#         valid = weekly[col].dropna()
#         if len(valid) > 10:
#             p01, p99 = valid.quantile(0.01), valid.quantile(0.99)
#             weekly[col] = weekly[col].clip(lower=p01, upper=p99)

#     print(f"  [WEEKLY] {len(weekly)} weeks: {weekly.date.min().date()} → {weekly.date.max().date()}")
#     return weekly


# # ============================================================================
# # 4. EDA (generates and saves figures)
# # ============================================================================

# def run_brent_eda(
#     df: pd.DataFrame,
#     fig_dir: Path = FIG_DIR,
#     save_figs: bool = True,
# ) -> dict:
#     """
#     Run comprehensive EDA on cleaned Brent daily data.

#     Generates and optionally saves figures for:
#         1. Price time series (Brent vs WTI)
#         2. Return distributions with tail statistics
#         3. Autocorrelation analysis (returns + absolute returns)
#         4. Shock clustering (95th percentile threshold)
#         5. Rolling volatility regime detection
#         6. Brent-WTI spread dynamics
#         7. Event-window diagnostics

#     Returns
#     -------
#     dict : EDA summary statistics for downstream reporting.
#     """
#     if not HAS_VIZ:
#         print("  [EDA] matplotlib/seaborn not available. Skipping figures.")
#         return {}

#     fig_dir.mkdir(parents=True, exist_ok=True)
#     sns.set_theme(style="whitegrid")
#     plt.rcParams["figure.figsize"] = (12, 5)
#     plt.rcParams["figure.dpi"] = 150

#     ret = df[["Date", "brent_log_return", "wti_log_return"]].dropna().copy()
#     summary = {}

#     # --- 1. Price Time Series ---
#     fig, ax = plt.subplots(figsize=(13, 5))
#     ax.plot(df["Date"], df["Brent_Price_USD"], label="Brent Crude", linewidth=1.3, color="#1f77b4")
#     ax.plot(df["Date"], df["WTI_Price_USD"], label="WTI", linewidth=1.1, alpha=0.8, color="#ff7f0e")
#     ax.set_title("Brent vs WTI Daily Crude Oil Prices (2020–2026)", fontsize=13, fontweight="bold")
#     ax.set_xlabel("Date")
#     ax.set_ylabel("USD per Barrel")
#     ax.legend()
#     plt.tight_layout()
#     if save_figs:
#         fig.savefig(fig_dir / "01_brent_wti_prices.png", bbox_inches="tight")
#     plt.close(fig)

#     # --- 2. Return Distributions ---
#     fig, axes = plt.subplots(1, 2, figsize=(14, 4))
#     sns.histplot(ret["brent_log_return"], bins=200, kde=True, stat="density",
#                  ax=axes[0], color="#1f77b4")
#     axes[0].set_title("Brent Log-Return Distribution")
#     axes[0].set_xlim(-0.15, 0.15)
#     sns.histplot(ret["wti_log_return"], bins=200, kde=True, stat="density",
#                  ax=axes[1], color="#ff7f0e")
#     axes[1].set_title("WTI Log-Return Distribution")
#     axes[1].set_xlim(-0.15, 0.15)
#     plt.tight_layout()
#     if save_figs:
#         fig.savefig(fig_dir / "02_return_distributions.png", bbox_inches="tight")
#     plt.close(fig)

#     summary["brent_return_stats"] = {
#         "mean": ret["brent_log_return"].mean(),
#         "std": ret["brent_log_return"].std(),
#         "skew": ret["brent_log_return"].skew(),
#         "kurtosis": ret["brent_log_return"].kurtosis(),
#     }

#     # --- 3. Autocorrelation ---
#     def lag_acf(series, max_lag=20):
#         return pd.Series({lag: series.autocorr(lag=lag) for lag in range(1, max_lag + 1)})

#     acf_ret = lag_acf(ret["brent_log_return"])
#     acf_abs = lag_acf(ret["brent_log_return"].abs())

#     fig, axes = plt.subplots(1, 2, figsize=(14, 4))
#     axes[0].bar(acf_ret.index, acf_ret.values, color="#1f77b4")
#     axes[0].axhline(0, color="black", linewidth=0.8)
#     axes[0].set_title("Brent Return ACF (Lags 1–20)")
#     axes[0].set_xlabel("Lag (days)")
#     axes[1].bar(acf_abs.index, acf_abs.values, color="#d62728")
#     axes[1].axhline(0, color="black", linewidth=0.8)
#     axes[1].set_title("Brent |Return| ACF — Volatility Clustering")
#     axes[1].set_xlabel("Lag (days)")
#     plt.tight_layout()
#     if save_figs:
#         fig.savefig(fig_dir / "03_autocorrelation.png", bbox_inches="tight")
#     plt.close(fig)

#     # --- 4. Shock Clustering ---
#     threshold_95 = ret["brent_log_return"].abs().quantile(0.95)
#     ret["large_shock"] = (ret["brent_log_return"].abs() >= threshold_95).astype(int)

#     fig, axes = plt.subplots(2, 1, figsize=(13, 7))
#     axes[0].plot(ret["Date"], ret["brent_log_return"].abs(), color="#1f77b4", linewidth=0.8)
#     axes[0].axhline(threshold_95, color="red", linestyle="--", linewidth=1.2, label="95th pctl")
#     axes[0].set_title("Brent |Returns| with Shock Threshold")
#     axes[0].legend()

#     monthly_shocks = ret.set_index("Date")["large_shock"].resample("ME").sum()
#     axes[1].bar(monthly_shocks.index, monthly_shocks.values, width=25, color="#9467bd", alpha=0.8)
#     axes[1].set_title("Monthly Count of Large Shocks")
#     axes[1].set_ylabel("Count")
#     plt.tight_layout()
#     if save_figs:
#         fig.savefig(fig_dir / "04_shock_clustering.png", bbox_inches="tight")
#     plt.close(fig)

#     summary["shock_threshold_95"] = threshold_95

#     # --- 5. Rolling Volatility ---
#     returns_series = ret.set_index("Date")["brent_log_return"]
#     windows = [20, 60, 120, 250]

#     fig, axes = plt.subplots(len(windows), 1, figsize=(14, 10), sharex=True)
#     for i, w in enumerate(windows):
#         roll = returns_series.rolling(w).std()
#         axes[i].plot(roll.index, roll.values, color="#d62728", linewidth=1)
#         axes[i].set_title(f"{w}-Day Rolling Volatility")
#         axes[i].set_ylabel("σ")
#     axes[-1].set_xlabel("Date")
#     fig.suptitle("Regime Behavior: Rolling Volatility Windows", y=1.01, fontweight="bold")
#     plt.tight_layout()
#     if save_figs:
#         fig.savefig(fig_dir / "05_rolling_volatility.png", bbox_inches="tight")
#     plt.close(fig)

#     # --- 6. Spread Dynamics ---
#     spread = df[["Date", "Spread_Brent_WTI"]].dropna()
#     sp_mean = spread["Spread_Brent_WTI"].mean()
#     sp_std = spread["Spread_Brent_WTI"].std()
#     spread["z_score"] = (spread["Spread_Brent_WTI"] - sp_mean) / sp_std

#     fig, axes = plt.subplots(2, 1, figsize=(13, 7), sharex=True)
#     axes[0].plot(spread["Date"], spread["Spread_Brent_WTI"], color="#8c564b", linewidth=1)
#     axes[0].axhline(sp_mean, color="black", linestyle="--", linewidth=0.8)
#     axes[0].set_title("Brent − WTI Spread (USD)")
#     axes[0].set_ylabel("USD")
#     axes[1].plot(spread["Date"], spread["z_score"], color="#7f7f7f", linewidth=1)
#     axes[1].axhline(2, color="red", linestyle="--"); axes[1].axhline(-2, color="red", linestyle="--")
#     axes[1].axhline(0, color="black", linewidth=0.8)
#     axes[1].set_title("Spread Z-Score (±2σ bands)")
#     axes[1].set_xlabel("Date")
#     plt.tight_layout()
#     if save_figs:
#         fig.savefig(fig_dir / "06_spread_dynamics.png", bbox_inches="tight")
#     plt.close(fig)

#     summary["spread_mean"] = sp_mean
#     summary["spread_std"] = sp_std

#     # --- 7. Event-Window Analysis ---
#     events = {
#         "COVID Demand Collapse": "2020-03-11",
#         "Russia-Ukraine War": "2022-02-24",
#         "OPEC+ Production Cut": "2023-04-03",
#         "Hormuz Tensions 2026": "2026-03-15",
#     }

#     ret_ev = ret.set_index("Date")[["brent_log_return", "wti_log_return"]]
#     event_rows = []
#     for name, dt in events.items():
#         event_date = pd.Timestamp(dt)
#         win = ret_ev.loc[event_date - pd.Timedelta(days=7):event_date + pd.Timedelta(days=7)]
#         if win.empty:
#             continue
#         event_rows.append({
#             "event": name,
#             "date": event_date.date(),
#             "obs": len(win),
#             "avg_brent_ret": round(win["brent_log_return"].mean(), 5),
#             "brent_vol": round(win["brent_log_return"].std(), 5),
#         })
#     summary["events"] = event_rows

#     fig, ax = plt.subplots(figsize=(13, 4))
#     ax.plot(ret["Date"], ret["brent_log_return"].abs(), color="#1f77b4", linewidth=0.8)
#     for name, dt in events.items():
#         x = pd.Timestamp(dt)
#         if x >= ret["Date"].min() and x <= ret["Date"].max():
#             ax.axvline(x, linestyle="--", linewidth=1, color="#d62728", alpha=0.7)
#             ax.text(x, ax.get_ylim()[1] * 0.9, name, rotation=90, va="top", fontsize=7)
#     ax.set_title("Brent |Returns| with Event Markers")
#     ax.set_xlabel("Date"); ax.set_ylabel("|log return|")
#     plt.tight_layout()
#     if save_figs:
#         fig.savefig(fig_dir / "07_event_windows.png", bbox_inches="tight")
#     plt.close(fig)

#     print(f"  [EDA] Saved {7} figures to {fig_dir}/")
#     return summary


# # ============================================================================
# # 5. FULL PIPELINE
# # ============================================================================

# def run_brent_pipeline(
#     raw_path: str | Path = BRENT_RAW_DIR / "Brent_WTI_Prices_2020_to_Present.csv",
#     output_dir: Path = BRENT_CLEANED_DIR,
#     run_eda: bool = True,
# ) -> dict:
#     """
#     Execute the full Brent pipeline: load → clean → aggregate → EDA → export.
#     """
#     print("=" * 60)
#     print("BRENT PIPELINE")
#     print("=" * 60)

#     output_dir.mkdir(parents=True, exist_ok=True)

#     # Load
#     print("\n▶ Loading raw data...")
#     df_raw = load_brent(raw_path)

#     # Clean
#     print("\n▶ Cleaning...")
#     df_clean = clean_brent(df_raw)

#     # Aggregate weekly
#     print("\n▶ Weekly aggregation...")
#     df_weekly = aggregate_weekly(df_clean)

#     # EDA
#     eda_summary = {}
#     if run_eda:
#         print("\n▶ Running EDA...")
#         eda_summary = run_brent_eda(df_clean)

#     # Export
#     print("\n▶ Exporting...")
#     df_clean.to_parquet(output_dir / "brent_daily_cleaned.parquet", index=False)
#     df_weekly.to_parquet(output_dir / "brent_weekly.parquet", index=False)
#     print(f"  Saved brent_daily_cleaned.parquet ({df_clean.shape})")
#     print(f"  Saved brent_weekly.parquet ({df_weekly.shape})")

#     return {
#         "daily": df_clean,
#         "weekly": df_weekly,
#         "eda_summary": eda_summary,
#     }


# if __name__ == "__main__":
#     run_brent_pipeline()


"""
brent_pipeline.py — Brent/WTI cleaning, weekly aggregation, and EDA.

Source: U.S. EIA API v2 (daily Brent/WTI spot prices)
References:
    Andersen & Bollerslev (1998), Int'l Economic Review 39(4).
    Cochrane (2005), Asset Pricing, Princeton.
    Hamilton (2003), J. Monetary Economics 50(2), 363-398.
"""

"""
brent_pipeline.py — Brent/WTI cleaning, weekly aggregation, and EDA.

Source : U.S. Energy Information Administration API v2 (daily spot prices)
Series : Brent Crude (RBRTE), West Texas Intermediate (RWTC)
"""

import warnings
from pathlib import Path
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from scipy import stats as sp_stats
    HAS_VIZ = True
except ImportError:
    HAS_VIZ = False


def _apply_style():
    """Compact, notebook-friendly style. Uses matplotlib defaults for fonts."""
    plt.rcParams.update({
        "figure.figsize": (6.5, 3.2), "figure.dpi": 110, "savefig.dpi": 110,
        "axes.titlesize": 10, "axes.labelsize": 9,
        "xtick.labelsize": 8, "ytick.labelsize": 8, "legend.fontsize": 8,
        "axes.grid": False, "axes.spines.top": False, "axes.spines.right": False,
    })


PRICE_FLOOR, PRICE_CEILING = 5.0, 200.0


def load_brent(filepath):
    """Load EIA CSV, parse dates, validate, compute spread."""
    df = pd.read_csv(filepath)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    for col in ["Brent_Price_USD", "WTI_Price_USD"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["Brent_Price_USD", "WTI_Price_USD"])
    df["price_flag"] = (
        (df["Brent_Price_USD"] < PRICE_FLOOR) |
        (df["Brent_Price_USD"] > PRICE_CEILING) |
        (df["WTI_Price_USD"] < PRICE_FLOOR) |
        (df["WTI_Price_USD"] > PRICE_CEILING)
    ).astype(int)
    df = df.drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)
    df["Spread_Brent_WTI"] = df["Brent_Price_USD"] - df["WTI_Price_USD"]
    print(f"  [BRENT] {len(df)} daily obs: {df.Date.min().date()} to {df.Date.max().date()}")
    return df


def clean_brent(df):
    """Remove weekends, compute log-returns (winsorized 1/99), add ISO week."""
    df = df.copy()
    df = df[df["Date"].dt.dayofweek < 5]
    df["brent_log_return"] = np.log(df["Brent_Price_USD"]).diff()
    df["wti_log_return"] = np.log(df["WTI_Price_USD"]).diff()
    for col in ["brent_log_return", "wti_log_return"]:
        v = df[col].dropna()
        if len(v) > 0:
            df[col] = df[col].clip(lower=v.quantile(0.01), upper=v.quantile(0.99))
    df["iso_year"] = df["Date"].dt.isocalendar().year.astype(int)
    df["iso_week"] = df["Date"].dt.isocalendar().week.astype(int)
    df["week_start"] = df["Date"].dt.to_period("W-SUN").apply(lambda x: x.start_time)
    print(f"  [BRENT CLEAN] {len(df)} trading days")
    return df.reset_index(drop=True)


def aggregate_weekly(df):
    """Daily to weekly: OHLC plus realized volatility (Andersen & Bollerslev 1998)."""
    w = df.groupby("week_start").agg(
        brent_open=("Brent_Price_USD", "first"),
        brent_close=("Brent_Price_USD", "last"),
        brent_high=("Brent_Price_USD", "max"),
        brent_low=("Brent_Price_USD", "min"),
        brent_mean=("Brent_Price_USD", "mean"),
        brent_median=("Brent_Price_USD", "median"),
        wti_close=("WTI_Price_USD", "last"),
        wti_mean=("WTI_Price_USD", "mean"),
        spread_mean=("Spread_Brent_WTI", "mean"),
        n_trading_days=("Brent_Price_USD", "count"),
        brent_realized_vol=("brent_log_return", lambda x: np.sqrt((x**2).sum())),
    ).reset_index()
    w.rename(columns={"week_start": "date"}, inplace=True)
    w["brent_wret"] = np.log(w["brent_close"]).diff()
    w["wti_wret"] = np.log(w["wti_close"]).diff()
    for col in ["brent_wret", "wti_wret"]:
        v = w[col].dropna()
        if len(v) > 10:
            w[col] = w[col].clip(lower=v.quantile(0.01), upper=v.quantile(0.99))
    print(f"  [BRENT WEEKLY] {len(w)} weeks")
    return w


def run_brent_eda(df, fig_dir=Path("figures")):
    """Generate 7 publication-quality EDA figures."""
    if not HAS_VIZ:
        return {}
    fig_dir.mkdir(parents=True, exist_ok=True)
    _apply_style()
    ret = df[["Date", "brent_log_return", "wti_log_return"]].dropna().copy()
    summary = {}

    # Fig 1: Price levels
    fig, ax = plt.subplots(figsize=(6.5, 3))
    ax.plot(df["Date"], df["Brent_Price_USD"], lw=0.8, color="#1f77b4", label="Brent crude")
    ax.plot(df["Date"], df["WTI_Price_USD"], lw=0.7, alpha=0.75, color="#ff7f0e", label="WTI")
    ax.set_title("Daily Brent and WTI Crude Oil Spot Prices, 2020–2026")
    ax.set_xlabel("Date")
    ax.set_ylabel("Spot price (USD per barrel)")
    ax.legend(frameon=False)
    plt.tight_layout()
    fig.savefig(fig_dir / "brent_01_price_levels.png", bbox_inches="tight")
    plt.close()

    # Fig 2: Return distributions with KDE and Gaussian reference
    fig, axes = plt.subplots(1, 2, figsize=(6.8, 2.8))
    for i, (col, label, c) in enumerate([
        ("brent_log_return", "Brent", "#1f77b4"),
        ("wti_log_return", "WTI", "#ff7f0e"),
    ]):
        x = ret[col].dropna()
        axes[i].hist(x, bins=80, density=True, color=c, alpha=0.45,
                     edgecolor="none", label="Empirical")
        kde = sp_stats.gaussian_kde(x, bw_method=0.25)
        grid = np.linspace(x.min(), x.max(), 400)
        axes[i].plot(grid, kde(grid), color=c, lw=1.2, label="KDE")
        mu, sigma = x.mean(), x.std()
        axes[i].plot(grid, sp_stats.norm.pdf(grid, mu, sigma),
                     color="black", ls="--", lw=1, label=r"$N(\mu,\sigma)$ fit")
        axes[i].set_title(f"{label} daily log-return")
        axes[i].set_xlabel("Log-return")
        axes[i].set_xlim(-0.12, 0.12)
        axes[i].legend(frameon=False, loc="upper right", fontsize=7)
    axes[0].set_ylabel("Density")
    plt.tight_layout()
    fig.savefig(fig_dir / "brent_02_return_distributions.png", bbox_inches="tight")
    plt.close()

    summary["brent_return_stats"] = {
        "mean": ret["brent_log_return"].mean(), "std": ret["brent_log_return"].std(),
        "skew": ret["brent_log_return"].skew(), "kurtosis": ret["brent_log_return"].kurtosis(),
    }

    # Fig 3: Autocorrelation with Bartlett 95% bands
    def _acf(s, mx=20):
        return pd.Series({k: s.autocorr(lag=k) for k in range(1, mx + 1)})
    acf_r = _acf(ret["brent_log_return"])
    acf_a = _acf(ret["brent_log_return"].abs())
    n = len(ret)
    ci = 1.96 / np.sqrt(n)

    fig, axes = plt.subplots(1, 2, figsize=(6.8, 2.8))
    for ax_, data, title, color in [
        (axes[0], acf_r, r"Returns  $r_t$", "#1f77b4"),
        (axes[1], acf_a, r"Absolute returns  $|r_t|$", "#d62728"),
    ]:
        ax_.bar(data.index, data.values, color=color, width=0.7)
        ax_.axhline(0, color="black", lw=0.5)
        ax_.axhline(ci, color="gray", ls=":", lw=0.6, label="95% Bartlett band")
        ax_.axhline(-ci, color="gray", ls=":", lw=0.6)
        ax_.set_title(title)
        ax_.set_xlabel("Lag (trading days)")
    axes[0].set_ylabel("Autocorrelation")
    axes[0].legend(frameon=False, fontsize=6)
    plt.tight_layout()
    fig.savefig(fig_dir / "brent_03_autocorrelation.png", bbox_inches="tight")
    plt.close()

    # Fig 4: Shock clustering
    thr = ret["brent_log_return"].abs().quantile(0.95)
    summary["shock_threshold_95"] = thr
    ret["shock"] = (ret["brent_log_return"].abs() >= thr).astype(int)

    fig, ax = plt.subplots(figsize=(6.5, 2.8))
    ax.plot(ret["Date"], ret["brent_log_return"].abs(), lw=0.4, color="#1f77b4")
    ax.axhline(thr, color="#d62728", ls="--", lw=0.8, label=f"95th percentile = {thr:.3f}")
    ax.set_title("Brent absolute daily log-returns with large-shock threshold")
    ax.set_xlabel("Date")
    ax.set_ylabel(r"$|r_t|$")
    ax.legend(frameon=False)
    plt.tight_layout()
    fig.savefig(fig_dir / "brent_04_shock_clustering.png", bbox_inches="tight")
    plt.close()

    # Fig 5: Rolling annualized volatility
    returns = ret.set_index("Date")["brent_log_return"]
    fig, axes = plt.subplots(4, 1, figsize=(6.5, 6), sharex=True)
    for i, w in enumerate([20, 60, 120, 250]):
        r = returns.rolling(w).std() * np.sqrt(252)
        axes[i].plot(r.index, r.values, color="#d62728", lw=0.7)
        axes[i].set_title(f"{w}-day rolling window (~{w/252:.1f} year)")
        axes[i].set_ylabel(r"$\hat{\sigma}_{ann}$")
    axes[-1].set_xlabel("Date")
    plt.tight_layout()
    fig.savefig(fig_dir / "brent_05_rolling_volatility.png", bbox_inches="tight")
    plt.close()

    # Fig 6: Spread and z-score
    sp = df[["Date", "Spread_Brent_WTI"]].dropna()
    mu, sigma = sp["Spread_Brent_WTI"].mean(), sp["Spread_Brent_WTI"].std()
    sp["z"] = (sp["Spread_Brent_WTI"] - mu) / sigma
    fig, axes = plt.subplots(2, 1, figsize=(6.5, 4.5), sharex=True)
    axes[0].plot(sp["Date"], sp["Spread_Brent_WTI"], color="#8c564b", lw=0.7)
    axes[0].axhline(mu, color="black", ls="--", lw=0.5, label=f"Mean = {mu:.2f}")
    axes[0].set_title("Brent minus WTI spread")
    axes[0].set_ylabel("Spread (USD)")
    axes[0].legend(frameon=False, fontsize=7)
    axes[1].plot(sp["Date"], sp["z"], color="#7f7f7f", lw=0.7)
    axes[1].axhline(2, color="#d62728", ls="--", lw=0.5)
    axes[1].axhline(-2, color="#d62728", ls="--", lw=0.5)
    axes[1].axhline(0, color="black", lw=0.5)
    axes[1].set_title(r"Spread standardized z-score with $\pm 2\sigma$ bands")
    axes[1].set_xlabel("Date")
    axes[1].set_ylabel("Z-score")
    plt.tight_layout()
    fig.savefig(fig_dir / "brent_06_spread.png", bbox_inches="tight")
    plt.close()
    summary["spread_mean"] = mu; summary["spread_std"] = sigma

    # Fig 7: Event markers
    events = {
        "COVID demand collapse": "2020-03-11",
        "Russia–Ukraine war": "2022-02-24",
        "OPEC+ production cut": "2023-04-03",
    }
    fig, ax = plt.subplots(figsize=(6.5, 2.8))
    ax.plot(ret["Date"], ret["brent_log_return"].abs(), color="#1f77b4", lw=0.4)
    for name, dt in events.items():
        x = pd.Timestamp(dt)
        if ret["Date"].min() <= x <= ret["Date"].max():
            ax.axvline(x, ls="--", lw=0.7, color="#d62728", alpha=0.7)
            ax.text(x, ax.get_ylim()[1] * 0.92, name, rotation=90,
                    va="top", fontsize=6, color="#d62728")
    ax.set_title("Brent absolute returns with major-event markers")
    ax.set_xlabel("Date"); ax.set_ylabel(r"$|r_t|$")
    plt.tight_layout()
    fig.savefig(fig_dir / "brent_07_events.png", bbox_inches="tight")
    plt.close()

    event_rows = []
    ret_ev = ret.set_index("Date")[["brent_log_return", "wti_log_return"]]
    for name, dt in events.items():
        ed = pd.Timestamp(dt)
        win = ret_ev.loc[ed - pd.Timedelta(days=7):ed + pd.Timedelta(days=7)]
        if not win.empty:
            event_rows.append({
                "event": name, "date": ed.date(), "obs": len(win),
                "avg_brent_ret": win["brent_log_return"].mean(),
                "brent_vol": win["brent_log_return"].std(),
            })
    summary["events"] = event_rows
    print(f"  [BRENT EDA] 7 figures saved to {fig_dir}/")
    return summary