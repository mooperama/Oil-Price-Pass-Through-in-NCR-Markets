"""
doe_pipeline.py — DOE pump price cleaning, aggregation, and EDA.

Source     : Philippine Department of Energy Oil Monitor (weekly PDF reports)
Geography  : Metro Manila (NCR), 13 cities
Imputation : Median (Huber, 1981: Robust Statistics)
"""

from __future__ import annotations
import re, warnings
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
    plt.rcParams.update({
        "figure.figsize": (6.5, 3.2), "figure.dpi": 110, "savefig.dpi": 110,
        "axes.titlesize": 10, "axes.labelsize": 9,
        "xtick.labelsize": 8, "ytick.labelsize": 8, "legend.fontsize": 8,
        "axes.grid": False, "axes.spines.top": False, "axes.spines.right": False,
    })


PRODUCT_NORMALIZE = {
    r"(?i)^Ron\s*91$": "RON 91", r"(?i)^Ron\s*95$": "RON 95",
    r"(?i)^Ron\s*97$": "RON 97", r"(?i)^Ron\s*100$": "RON 100",
    r"(?i)^RON\s*91$": "RON 91", r"(?i)^RON\s*95$": "RON 95",
    r"(?i)^RON\s*97$": "RON 97", r"(?i)^RON\s*100$": "RON 100",
}
CITY_NORMALIZE = {"Paranaque City": "Parañaque City", "Las Pinas City": "Las Piñas City"}
CORE_PRODUCTS = ["Diesel", "RON 91", "RON 95", "RON 97"]


def load_doe(filepath):
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    if "No." in df.columns: df = df.drop(columns=["No."])
    if "Notes" in df.columns and df["Notes"].isna().all(): df = df.drop(columns=["Notes"])
    df["Effectivity Date"] = pd.to_datetime(df["Effectivity Date"], errors="coerce")
    df = df.dropna(subset=["Effectivity Date"])
    def norm_prod(n):
        for p, r in PRODUCT_NORMALIZE.items():
            if re.match(p, n.strip()): return r
        return n.strip()
    df["Product"] = df["Product"].apply(norm_prod)
    df["City"] = df["City"].replace(CITY_NORMALIZE)
    for c in ["Price Low (P/L)", "Price High (P/L)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["Price_Mid"] = (df["Price Low (P/L)"] + df["Price High (P/L)"]) / 2
    df = df.sort_values(["Effectivity Date", "City", "Product", "Brand"]).reset_index(drop=True)
    print(f"  [DOE] {len(df)} obs: {df['Effectivity Date'].min().date()} to "
          f"{df['Effectivity Date'].max().date()}, {sorted(df.Product.unique())}")
    return df


def clean_doe(df):
    df = df.copy()
    n0 = len(df)
    df = df.sort_values("Price_Mid", ascending=False)
    df = df.drop_duplicates(subset=["Effectivity Date", "City", "Product", "Brand"], keep="first")
    print(f"  [DOE CLEAN] Removed {n0 - len(df)} duplicates")
    df["iso_year"] = df["Effectivity Date"].dt.isocalendar().year.astype(int)
    df["iso_week"] = df["Effectivity Date"].dt.isocalendar().week.astype(int)
    df["week_start"] = df["Effectivity Date"].dt.to_period("W-SUN").apply(lambda x: x.start_time)
    for pc in ["Price Low (P/L)", "Price High (P/L)", "Price_Mid"]:
        null_m = df[pc].isna()
        if null_m.sum() > 0:
            med = df.groupby(["iso_year", "iso_week", "Product"])[pc].transform("median")
            df.loc[null_m, pc] = med[null_m]
            if df[pc].isna().sum() > 0:
                df[pc] = df[pc].fillna(df.groupby("Product")[pc].transform("median"))
    df = df.sort_values(["Effectivity Date", "City", "Product", "Brand"]).reset_index(drop=True)
    print(f"  [DOE CLEAN] {len(df)} rows")
    return df


def aggregate_doe_weekly(df):
    w = df.groupby(["week_start", "Product"]).agg(
        price_low_median=("Price Low (P/L)", "median"),
        price_high_median=("Price High (P/L)", "median"),
        price_mid_median=("Price_Mid", "median"),
        price_mid_mean=("Price_Mid", "mean"),
        n_stations=("Price_Mid", "count"),
        price_iqr=("Price_Mid", lambda x: x.quantile(0.75) - x.quantile(0.25)),
    ).reset_index()
    w.rename(columns={"week_start": "date"}, inplace=True)
    w = w.sort_values(["Product", "date"])
    w["doe_log_return"] = w.groupby("Product").apply(
        lambda g: np.log(g["price_mid_median"]).diff(), include_groups=False
    ).reset_index(level=0, drop=True)
    print(f"  [DOE WEEKLY] {len(w)} product-weeks")
    return w


def run_doe_eda(df_clean, df_weekly, fig_dir=Path("figures")):
    if not HAS_VIZ: return {}
    fig_dir.mkdir(parents=True, exist_ok=True)
    _apply_style()

    # Fig 1: Weekly median price trends
    pivot = df_weekly.pivot_table(index="date", columns="Product", values="price_mid_median")
    fig, ax = plt.subplots(figsize=(6.5, 3))
    for col in CORE_PRODUCTS:
        if col in pivot.columns:
            ax.plot(pivot.index, pivot[col], lw=0.9, label=col)
    ax.set_title("Weekly median NCR pump prices by product, 2020–2026")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price (PHP per liter)")
    ax.legend(frameon=False)
    plt.tight_layout()
    fig.savefig(fig_dir / "doe_01_price_trends.png", bbox_inches="tight")
    plt.close()

    # Fig 2: Distribution with KDE + normal fit, per product
    fig, axes = plt.subplots(2, 2, figsize=(7, 5.5))
    axes = axes.flatten()
    summary_rows = []
    for i, prod in enumerate(CORE_PRODUCTS):
        x = df_clean[df_clean["Product"] == prod]["Price_Mid"].dropna()
        if len(x) < 10: continue
        # Histogram
        axes[i].hist(x, bins=60, density=True, color="#2ca02c", alpha=0.45,
                     edgecolor="none", label="Empirical")
        # KDE
        kde = sp_stats.gaussian_kde(x, bw_method=0.3)
        grid = np.linspace(x.min(), x.max(), 400)
        axes[i].plot(grid, kde(grid), color="#2ca02c", lw=1.3, label="KDE")
        # Normal fit
        mu, sigma = x.mean(), x.std()
        axes[i].plot(grid, sp_stats.norm.pdf(grid, mu, sigma),
                     color="black", ls="--", lw=1, label=r"$N(\mu,\sigma)$ fit")
        # Annotations
        sk = x.skew(); ku = x.kurtosis()
        sample = x.sample(min(5000, len(x)), random_state=42)
        _, pval = sp_stats.shapiro(sample)
        summary_rows.append({"product": prod, "n": len(x),
                              "skewness": sk, "excess_kurtosis": ku,
                              "shapiro_p": pval})
        axes[i].set_title(
            f"{prod}  (skew={sk:.2f}, kurt={ku:.2f})")
        axes[i].set_xlabel("Price (PHP/L)")
        axes[i].legend(frameon=False, fontsize=6)
        if i % 2 == 0:
            axes[i].set_ylabel("Density")
    plt.tight_layout()
    fig.savefig(fig_dir / "doe_02_distribution.png", bbox_inches="tight")
    plt.close()

    # Fig 3: Mean-median gap over time
    core_w = df_weekly[df_weekly["Product"].isin(CORE_PRODUCTS)].copy()
    core_w["gap_pct"] = (core_w["price_mid_mean"] - core_w["price_mid_median"]) / \
                        core_w["price_mid_median"] * 100
    fig, ax = plt.subplots(figsize=(6.5, 2.8))
    for prod in CORE_PRODUCTS:
        pdata = core_w[core_w["Product"] == prod]
        ax.plot(pdata["date"], pdata["gap_pct"], lw=0.7, label=prod)
    ax.axhline(0, color="black", lw=0.5, ls="--")
    ax.set_title("Weekly mean-median price gap relative to median, NCR")
    ax.set_xlabel("Date")
    ax.set_ylabel("(Mean – Median) / Median  ×100")
    ax.legend(frameon=False, ncol=2)
    plt.tight_layout()
    fig.savefig(fig_dir / "doe_03_mean_median_gap.png", bbox_inches="tight")
    plt.close()

    # Fig 4: Median diesel price by city
    diesel = df_clean[df_clean["Product"] == "Diesel"]
    city_med = diesel.groupby("City")["Price_Mid"].median().sort_values()
    fig, ax = plt.subplots(figsize=(6.5, 3.5))
    ax.barh(city_med.index, city_med.values, color="#2ca02c", height=0.6)
    ax.set_title("Median diesel price by NCR city, full sample")
    ax.set_xlabel("Price (PHP per liter)")
    ax.set_ylabel("City")
    plt.tight_layout()
    fig.savefig(fig_dir / "doe_04_diesel_by_city.png", bbox_inches="tight")
    plt.close()

    # Fig 5: Cross-station dispersion (IQR)
    iqr_pivot = df_weekly.pivot_table(index="date", columns="Product", values="price_iqr")
    fig, ax = plt.subplots(figsize=(6.5, 2.8))
    for c in CORE_PRODUCTS:
        if c in iqr_pivot.columns:
            ax.plot(iqr_pivot.index, iqr_pivot[c], lw=0.7, label=c)
    ax.set_title("Cross-station interquartile range of pump prices, NCR")
    ax.set_xlabel("Date")
    ax.set_ylabel("IQR of station prices (PHP/L)")
    ax.legend(frameon=False)
    plt.tight_layout()
    fig.savefig(fig_dir / "doe_05_price_dispersion.png", bbox_inches="tight")
    plt.close()

    print(f"  [DOE EDA] 5 figures saved to {fig_dir}/")
    return {"normality": summary_rows}