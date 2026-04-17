# """
# integration_pipeline.py
# =======================
# Module 4: Data integration, SQL storage, FSFI, and econometric modeling.

# This module orchestrates the three source pipelines (Brent, DOE, DTI),
# performs temporal alignment, constructs the Fuel-Sensitive Food Index,
# stores everything in SQLite, and runs the quantile regression and VAR
# models for oil-to-food pass-through estimation.

# Econometric Framework:
#     1. Quantile Regression (Koenker & Bassett, 1978):
#        Δlog(food_price_t) = α(τ) + Σ β_k(τ) Δlog(oil_{t-k}) + ε_t(τ)
#        Estimated at τ ∈ {0.10, 0.25, 0.50, 0.75, 0.90} to capture
#        asymmetric pass-through (tail behavior for inflation spikes).

#     2. Vector Autoregression (Sims, 1980):
#        Y_t = c + A_1 Y_{t-1} + ... + A_p Y_{t-p} + u_t
#        where Y = [brent_ret, diesel_ret, fsfi].
#        Impulse Response Functions trace how a Brent shock propagates
#        to domestic fuel then food prices over 12 weeks.

# References:
#     Koenker, R. & Bassett, G. (1978). Econometrica, 46(1), 33-50.
#     Sims, C.A. (1980). Econometrica, 48(1), 1-48.
#     Hamilton, J.D. (2003). J. Monetary Economics, 50(2), 363-398.
# """

# from __future__ import annotations

# import sqlite3
# import warnings
# from datetime import datetime
# from pathlib import Path
# from typing import Dict, List, Optional, Tuple

# import numpy as np
# import pandas as pd
# # SQL functions are in sql_pipeline.py
# # from sqlalchemy import create_engine, text

# warnings.filterwarnings("ignore")

# try:
#     import matplotlib
#     matplotlib.use("Agg")
#     import matplotlib.pyplot as plt
#     import seaborn as sns
#     HAS_VIZ = True
# except ImportError:
#     HAS_VIZ = False

# try:
#     import statsmodels.api as sm
#     from statsmodels.regression.quantile_regression import QuantReg
#     from statsmodels.tsa.api import VAR
#     HAS_STATSMODELS = True
# except ImportError:
#     HAS_STATSMODELS = False


# # ============================================================================
# # CONFIGURATION
# # ============================================================================

# CLEANED_DIR = Path("cleaned_data")
# DB_PATH = CLEANED_DIR / "oil_passthrough.db"
# FIG_DIR = Path("figures/analysis")

# MAX_LAG_WEEKS = 8

# # Food basket for FSFI
# FOOD_BASKET = [
#     "canned sardines", "instant noodles",
#     "milk condensada", "milk evaporada", "milk powdered",
#     "coffee", "salt iodized",
#     "bread loaf", "bread pandesal", "vinegar", "patis",
#     "soy sauce", "canned beef corned", "canned pork luncheon meat",
#     "canned beef loaf", "canned pork meat loaf",
# ]

# # Quantile regression quantiles
# QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90]


# # ============================================================================
# # 1. TEMPORAL ALIGNMENT & LAG FEATURES
# # ============================================================================

# def create_lag_features(
#     brent_w: pd.DataFrame,
#     doe_w: pd.DataFrame,
#     max_lags: int = MAX_LAG_WEEKS,
# ) -> pd.DataFrame:
#     """
#     Construct lagged oil/fuel price features for regression.

#     For week t, generates:
#         - brent_ret_lag{k}: Brent weekly return at t-k
#         - brent_level_lag{k}: Brent price level at t-k
#         - brent_vol_lag{k}: Realized volatility at t-k
#         - diesel_ret_lag{k}: Diesel weekly return at t-k
#         - diesel_level_lag{k}: Diesel price level at t-k
#     """
#     # Brent features
#     brent_lags = brent_w[["date", "brent_wret", "brent_close",
#                            "brent_realized_vol"]].copy()
#     for k in range(0, max_lags + 1):
#         brent_lags[f"brent_ret_lag{k}"] = brent_lags["brent_wret"].shift(k)
#         brent_lags[f"brent_level_lag{k}"] = brent_lags["brent_close"].shift(k)
#         brent_lags[f"brent_vol_lag{k}"] = brent_lags["brent_realized_vol"].shift(k)

#     # Diesel features
#     diesel = doe_w[doe_w["Product"] == "Diesel"][
#         ["date", "doe_log_return", "price_mid_median"]
#     ].copy()
#     diesel.rename(columns={
#         "doe_log_return": "diesel_ret",
#         "price_mid_median": "diesel_price",
#     }, inplace=True)

#     for k in range(0, max_lags + 1):
#         diesel[f"diesel_ret_lag{k}"] = diesel["diesel_ret"].shift(k)
#         diesel[f"diesel_level_lag{k}"] = diesel["diesel_price"].shift(k)

#     # Merge
#     lag_features = pd.merge(
#         brent_lags.drop(columns=["brent_wret", "brent_close", "brent_realized_vol"]),
#         diesel.drop(columns=["diesel_ret", "diesel_price"]),
#         on="date", how="outer",
#     )

#     print(f"  [LAGS] {max_lags + 1} lags × 5 feature groups = "
#           f"{len([c for c in lag_features.columns if c != 'date'])} features")
#     return lag_features


# # ============================================================================
# # 2. FUEL-SENSITIVE FOOD INDEX (FSFI)
# # ============================================================================

# def compute_fsfi(
#     dti_w: pd.DataFrame,
#     brent_w: pd.DataFrame,
#     food_basket: List[str] = FOOD_BASKET,
#     lag_for_corr: int = 4,
# ) -> Tuple[pd.DataFrame, Dict[str, float], Dict[str, float]]:
#     """
#     Construct the Fuel-Sensitive Food Index.

#     Methodology:
#         1. Compute lagged Pearson correlation between Brent returns
#            and each food commodity's log-price change at lag=4 weeks.
#         2. Retain only positively correlated commodities
#            (fuel-sensitive goods — a negative correlation would imply
#            counter-cyclical pricing, e.g., from government subsidies).
#         3. Assign weights ∝ |correlation|, normalized to sum to 1.
#         4. FSFI_t = Σ_i w_i × Δlog(price_i_t)

#     Returns
#     -------
#     fsfi_df : pd.DataFrame
#         Weekly FSFI series.
#     weights : dict
#         Commodity → weight mapping.
#     correlations : dict
#         Commodity → lagged correlation.
#     """
#     dti_food = dti_w[dti_w["commodity"].isin(food_basket)].copy()

#     pivot = dti_food.pivot_table(
#         index="date", columns="commodity",
#         values="dti_dlog", aggfunc="first"
#     )

#     brent_ret = brent_w[["date", "brent_wret"]].copy()
#     merged = pd.merge(pivot, brent_ret, left_index=True, right_on="date", how="inner")
#     merged = merged.set_index("date")

#     # Lagged correlations
#     correlations = {}
#     brent_shifted = merged["brent_wret"].shift(lag_for_corr)
#     for commodity in pivot.columns:
#         valid = merged[[commodity]].join(brent_shifted.rename("brent_lag")).dropna()
#         if len(valid) > 20:
#             correlations[commodity] = valid[commodity].corr(valid["brent_lag"])

#     # Filter positively correlated
#     fuel_sensitive = {k: v for k, v in correlations.items() if v > 0}

#     if not fuel_sensitive:
#         print("  [FSFI] WARNING: No positively correlated commodities. "
#               "Using equal weights.")
#         fuel_sensitive = {k: 1.0 / len(correlations) for k in correlations}

#     # Normalize weights
#     total = sum(abs(v) for v in fuel_sensitive.values())
#     weights = {k: abs(v) / total for k, v in fuel_sensitive.items()}

#     print("  [FSFI] Components:")
#     for c, w in sorted(weights.items(), key=lambda x: -x[1]):
#         print(f"    {c:35s}  ρ={fuel_sensitive[c]:+.4f}  w={w:.4f}")

#     # Build index
#     fsfi = pd.Series(0.0, index=pivot.index, name="fsfi")
#     for commodity, w in weights.items():
#         if commodity in pivot.columns:
#             fsfi += w * pivot[commodity].fillna(0)

#     fsfi_df = fsfi.reset_index()
#     fsfi_df.columns = ["date", "fsfi"]
#     fsfi_df["fsfi_cumulative"] = fsfi_df["fsfi"].cumsum()

#     return fsfi_df, weights, correlations


# # ============================================================================
# # 4. MERGED ANALYSIS DATASET
# # ============================================================================

# def build_merged_dataset(
#     brent_w: pd.DataFrame,
#     doe_w: pd.DataFrame,
#     dti_w: pd.DataFrame,
#     fsfi: pd.DataFrame,
#     lag_features: pd.DataFrame,
# ) -> pd.DataFrame:
#     """
#     Build the final analysis-ready merged panel.

#     Outer join on date → flag source availability → save.
#     """
#     # Pivot DOE to wide
#     doe_wide = doe_w.pivot_table(
#         index="date", columns="Product",
#         values="price_mid_median", aggfunc="first"
#     )
#     doe_wide.columns = [f"doe_{c.lower().replace(' ', '_')}" for c in doe_wide.columns]
#     doe_wide = doe_wide.reset_index()

#     doe_ret = doe_w.pivot_table(
#         index="date", columns="Product",
#         values="doe_log_return", aggfunc="first"
#     )
#     doe_ret.columns = [f"doe_{c.lower().replace(' ', '_')}_ret" for c in doe_ret.columns]
#     doe_ret = doe_ret.reset_index()

#     # Pivot DTI to wide
#     dti_price = dti_w.pivot_table(
#         index="date", columns="commodity",
#         values="price_median", aggfunc="first"
#     )
#     dti_price.columns = [f"dti_{c.replace(' ', '_')}" for c in dti_price.columns]
#     dti_price = dti_price.reset_index()

#     dti_ret = dti_w.pivot_table(
#         index="date", columns="commodity",
#         values="dti_dlog", aggfunc="first"
#     )
#     dti_ret.columns = [f"dti_{c.replace(' ', '_')}_ret" for c in dti_ret.columns]
#     dti_ret = dti_ret.reset_index()

#     # Merge
#     merged = brent_w[["date", "brent_close", "brent_mean", "brent_wret",
#                        "brent_realized_vol", "wti_close", "spread_mean"]].copy()

#     for df_m in [merged, doe_wide, doe_ret, dti_price, dti_ret, fsfi, lag_features]:
#         df_m["date"] = pd.to_datetime(df_m["date"])

#     merged = (merged
#               .merge(doe_wide, on="date", how="outer")
#               .merge(doe_ret, on="date", how="outer")
#               .merge(dti_price, on="date", how="outer")
#               .merge(dti_ret, on="date", how="outer")
#               .merge(fsfi, on="date", how="outer")
#               .merge(lag_features, on="date", how="outer"))

#     merged = merged.sort_values("date").reset_index(drop=True)

#     # Source flags
#     b_dates = set(brent_w["date"].dt.date)
#     d_dates = set(doe_w["date"].dt.date)
#     t_dates = set(dti_w["date"].dt.date)
#     merged["has_brent"] = merged["date"].dt.date.isin(b_dates).astype(int)
#     merged["has_doe"] = merged["date"].dt.date.isin(d_dates).astype(int)
#     merged["has_dti"] = merged["date"].dt.date.isin(t_dates).astype(int)
#     merged["all_sources"] = (
#         (merged["has_brent"] == 1) &
#         (merged["has_doe"] == 1) &
#         (merged["has_dti"] == 1)
#     ).astype(int)

#     n_full = merged["all_sources"].sum()
#     print(f"  [MERGED] {len(merged)} weeks, {n_full} with all sources.")
#     return merged


# # ============================================================================
# # 5. QUANTILE REGRESSION
# # ============================================================================

# def run_quantile_regression(
#     merged: pd.DataFrame,
#     target_commodity: str = "canned_sardines",
#     quantiles: List[float] = QUANTILES,
#     max_lag: int = 4,
#     fig_dir: Path = FIG_DIR,
# ) -> pd.DataFrame:
#     """
#     Distributed-lag quantile regression: food price on oil lags.

#     Model:
#         Δlog(food_t) = α(τ) + Σ_{k=0}^{K} β_k(τ) Δlog(brent_{t-k}) + ε(τ)

#     Estimates at multiple quantiles to capture tail behavior.

#     Returns
#     -------
#     results_df : pd.DataFrame
#         Coefficients, std errors, p-values for each quantile × lag.
#     """
#     if not HAS_STATSMODELS:
#         print("  [QR] statsmodels not installed. Skipping.")
#         return pd.DataFrame()

#     fig_dir.mkdir(parents=True, exist_ok=True)

#     # Prepare regression data
#     y_col = f"dti_{target_commodity}_ret"
#     if y_col not in merged.columns:
#         print(f"  [QR] Column '{y_col}' not found. Available dti_*_ret columns:")
#         dti_cols = [c for c in merged.columns if c.startswith("dti_") and c.endswith("_ret")]
#         print(f"    {dti_cols[:10]}")
#         return pd.DataFrame()

#     x_cols = [f"brent_ret_lag{k}" for k in range(0, max_lag + 1)]
#     keep_cols = [y_col] + x_cols
#     reg_data = merged[merged["all_sources"] == 1][keep_cols].dropna()

#     if len(reg_data) < 30:
#         print(f"  [QR] Only {len(reg_data)} obs. Need ≥30. Skipping.")
#         return pd.DataFrame()

#     y = reg_data[y_col]
#     X = sm.add_constant(reg_data[x_cols])

#     print(f"\n  [QR] Target: {target_commodity} | N={len(reg_data)} | Lags=0..{max_lag}")

#     results = []
#     models = {}

#     for tau in quantiles:
#         model = QuantReg(y, X)
#         res = model.fit(q=tau, max_iter=1000)
#         models[tau] = res

#         for var in X.columns:
#             results.append({
#                 "quantile": tau,
#                 "variable": var,
#                 "coef": res.params[var],
#                 "std_err": res.bse[var],
#                 "t_stat": res.tvalues[var],
#                 "p_value": res.pvalues[var],
#                 "ci_lower": res.conf_int().loc[var, 0],
#                 "ci_upper": res.conf_int().loc[var, 1],
#             })

#         print(f"    τ={tau:.2f}  pseudo-R²={res.prsquared:.4f}  "
#               f"β_lag0={res.params.get('brent_ret_lag0', np.nan):.5f}")

#     results_df = pd.DataFrame(results)

#     # --- Visualization: Coefficient plots by quantile ---
#     if HAS_VIZ:
#         lag_vars = [c for c in x_cols if c in X.columns]

#         fig, axes = plt.subplots(1, len(lag_vars), figsize=(4 * len(lag_vars), 4),
#                                  sharey=True)
#         if len(lag_vars) == 1:
#             axes = [axes]

#         for i, var in enumerate(lag_vars):
#             var_data = results_df[results_df["variable"] == var]
#             axes[i].plot(var_data["quantile"], var_data["coef"],
#                         "o-", color="#1f77b4", linewidth=1.5)
#             axes[i].fill_between(var_data["quantile"],
#                                 var_data["ci_lower"], var_data["ci_upper"],
#                                 alpha=0.2, color="#1f77b4")
#             axes[i].axhline(0, color="red", linestyle="--", linewidth=0.8)
#             axes[i].set_title(var, fontsize=9)
#             axes[i].set_xlabel("Quantile (τ)")
#             if i == 0:
#                 axes[i].set_ylabel("Coefficient (elasticity)")

#         fig.suptitle(f"Quantile Regression: Brent → {target_commodity.replace('_', ' ').title()}",
#                     fontweight="bold")
#         plt.tight_layout()
#         fig.savefig(fig_dir / f"qr_coefficients_{target_commodity}.png", bbox_inches="tight")
#         plt.close(fig)

#     return results_df


# def run_multi_commodity_qr(
#     merged: pd.DataFrame,
#     commodities: Optional[List[str]] = None,
#     fig_dir: Path = FIG_DIR,
# ) -> Dict[str, pd.DataFrame]:
#     """
#     Run quantile regressions for multiple commodities.
#     """
#     if commodities is None:
#         # Auto-detect available commodity return columns
#         commodities = []
#         for c in merged.columns:
#             if c.startswith("dti_") and c.endswith("_ret"):
#                 comm = c.replace("dti_", "").replace("_ret", "")
#                 commodities.append(comm)

#     all_results = {}
#     summary_rows = []

#     for comm in commodities:
#         res = run_quantile_regression(merged, comm, fig_dir=fig_dir)
#         if not res.empty:
#             all_results[comm] = res
#             # Extract median regression lag0 coefficient
#             median_lag0 = res[
#                 (res["quantile"] == 0.50) & (res["variable"] == "brent_ret_lag0")
#             ]
#             if not median_lag0.empty:
#                 summary_rows.append({
#                     "commodity": comm,
#                     "median_elasticity_lag0": median_lag0["coef"].values[0],
#                     "p_value": median_lag0["p_value"].values[0],
#                 })

#     if summary_rows and HAS_VIZ:
#         summary = pd.DataFrame(summary_rows).sort_values("median_elasticity_lag0")

#         fig, ax = plt.subplots(figsize=(10, max(4, len(summary) * 0.4)))
#         colors = ["#d62728" if p < 0.10 else "#aec7e8"
#                   for p in summary["p_value"]]
#         ax.barh(summary["commodity"].str.replace("_", " ").str.title(),
#                 summary["median_elasticity_lag0"], color=colors)
#         ax.axvline(0, color="black", linewidth=0.8)
#         ax.set_xlabel("Median Elasticity (β₀ at τ=0.50)")
#         ax.set_title("Oil-to-Food Pass-Through Elasticities (Lag 0)",
#                     fontweight="bold")
#         from matplotlib.patches import Patch
#         ax.legend(handles=[
#             Patch(color="#d62728", label="p < 0.10"),
#             Patch(color="#aec7e8", label="p ≥ 0.10"),
#         ])
#         plt.tight_layout()
#         fig.savefig(fig_dir / "qr_elasticity_summary.png", bbox_inches="tight")
#         plt.close(fig)

#     return all_results


# # ============================================================================
# # 6. VAR MODEL & IMPULSE RESPONSE
# # ============================================================================

# def run_var_model(
#     merged: pd.DataFrame,
#     fig_dir: Path = FIG_DIR,
#     max_order: int = 4,
#     irf_periods: int = 12,
# ) -> dict:
#     """
#     Vector Autoregression: [Brent_ret, Diesel_ret, FSFI].

#     Ordering follows Cholesky decomposition:
#         Brent → Diesel → FSFI
#     This assumes oil shocks are exogenous to the Philippine domestic
#     market (reasonable for a small open economy that imports 90% of oil).

#     Generates Impulse Response Functions (IRFs) showing how a 1-SD
#     Brent shock propagates through the price chain over 12 weeks.
#     """
#     if not HAS_STATSMODELS:
#         print("  [VAR] statsmodels not installed. Skipping.")
#         return {}

#     fig_dir.mkdir(parents=True, exist_ok=True)

#     # Prepare VAR data
#     var_cols = ["brent_wret", "doe_diesel_ret", "fsfi"]
#     available = [c for c in var_cols if c in merged.columns]

#     if len(available) < 3:
#         print(f"  [VAR] Missing columns. Available: {available}")
#         return {}

#     var_data = merged[merged["all_sources"] == 1][["date"] + var_cols].dropna()

#     if len(var_data) < 40:
#         print(f"  [VAR] Only {len(var_data)} obs. Need ≥40. Skipping.")
#         return {}

#     var_data = var_data.set_index("date")[var_cols]

#     # Fit VAR with AIC-optimal lag order
#     model = VAR(var_data)

#     # Determine optimal lag
#     lag_order = model.select_order(maxlags=max_order)
#     optimal_lag = lag_order.aic
#     print(f"\n  [VAR] AIC-optimal lag: {optimal_lag}")
#     print(f"  [VAR] N={len(var_data)}, variables={var_cols}")

#     results = model.fit(optimal_lag)
#     print(f"  [VAR] Fitted. AIC={results.aic:.4f}")

#     # Summary
#     print("\n  Granger causality tests (Brent → FSFI):")
#     try:
#         gc = results.test_causality("fsfi", "brent_wret", kind="f")
#         print(f"    F={gc.test_statistic:.3f}, p={gc.pvalue:.4f}")
#     except Exception as e:
#         print(f"    Could not compute: {e}")

#     # IRF
#     irf = results.irf(irf_periods)

#     if HAS_VIZ:
#         fig, axes = plt.subplots(1, 3, figsize=(15, 4))

#         targets = var_cols
#         shock_idx = 0  # Brent shock

#         for i, target in enumerate(targets):
#             target_idx = var_cols.index(target)
#             response = irf.irfs[:, target_idx, shock_idx]
#             lower = irf.ci[:, target_idx, shock_idx, 0] if hasattr(irf, 'ci') else None
#             upper = irf.ci[:, target_idx, shock_idx, 1] if hasattr(irf, 'ci') else None

#             axes[i].plot(range(irf_periods + 1), response,
#                         "o-", color="#1f77b4", linewidth=1.5, markersize=3)
#             if lower is not None:
#                 axes[i].fill_between(range(irf_periods + 1), lower, upper,
#                                     alpha=0.15, color="#1f77b4")
#             axes[i].axhline(0, color="red", linestyle="--", linewidth=0.8)
#             axes[i].set_title(f"Brent → {target}", fontsize=10)
#             axes[i].set_xlabel("Weeks")
#             if i == 0:
#                 axes[i].set_ylabel("Response")

#         fig.suptitle("Impulse Response: 1-SD Brent Shock",
#                     fontweight="bold", fontsize=12)
#         plt.tight_layout()
#         fig.savefig(fig_dir / "var_impulse_response.png", bbox_inches="tight")
#         plt.close(fig)

#         # Forecast Error Variance Decomposition
#         fevd = results.fevd(irf_periods)

#         fig, axes = plt.subplots(1, 3, figsize=(15, 4))
#         for i, target in enumerate(targets):
#             decomp = fevd.decomp[i]
#             n_steps = decomp.shape[0]
#             for j, source in enumerate(targets):
#                 axes[i].plot(range(n_steps), decomp[:, j],
#                             label=source, linewidth=1.2)
#             axes[i].set_title(f"FEVD: {target}", fontsize=10)
#             axes[i].set_xlabel("Weeks")
#             axes[i].set_ylim(0, 1)
#             if i == 0:
#                 axes[i].set_ylabel("Share")
#             axes[i].legend(fontsize=7)

#         fig.suptitle("Forecast Error Variance Decomposition", fontweight="bold")
#         plt.tight_layout()
#         fig.savefig(fig_dir / "var_fevd.png", bbox_inches="tight")
#         plt.close(fig)

#     return {
#         "results": results,
#         "irf": irf,
#         "optimal_lag": optimal_lag,
#     }


# # ============================================================================
# # 7. CORRELATION HEATMAP
# # ============================================================================

# def plot_correlation_heatmap(
#     merged: pd.DataFrame,
#     brent_w: pd.DataFrame,
#     dti_w: pd.DataFrame,
#     fig_dir: Path = FIG_DIR,
# ):
#     """
#     Lagged cross-correlation heatmap: Brent returns vs food price changes
#     at lags 0-8 weeks.
#     """
#     if not HAS_VIZ:
#         return

#     fig_dir.mkdir(parents=True, exist_ok=True)

#     brent = brent_w[["date", "brent_wret"]].dropna().copy()
#     brent["date"] = pd.to_datetime(brent["date"])

#     food_commodities = [c for c in FOOD_BASKET
#                         if c in dti_w["commodity"].unique()]

#     pivot = dti_w[dti_w["commodity"].isin(food_commodities)].pivot_table(
#         index="date", columns="commodity", values="dti_dlog", aggfunc="first"
#     )
#     pivot.index = pd.to_datetime(pivot.index)

#     combined = pivot.join(brent.set_index("date")["brent_wret"], how="inner")

#     corr_matrix = pd.DataFrame(index=food_commodities, columns=range(0, 9))
#     for lag in range(0, 9):
#         shifted = combined["brent_wret"].shift(lag)
#         for comm in food_commodities:
#             if comm in combined.columns:
#                 valid = combined[[comm]].join(shifted.rename("brent")).dropna()
#                 if len(valid) > 15:
#                     corr_matrix.loc[comm, lag] = valid[comm].corr(valid["brent"])

#     corr_matrix = corr_matrix.astype(float)

#     fig, ax = plt.subplots(figsize=(10, max(5, len(food_commodities) * 0.35)))
#     sns.heatmap(corr_matrix, annot=True, fmt=".3f", cmap="RdYlGn",
#                 center=0, ax=ax, linewidths=0.5,
#                 xticklabels=[f"Lag {i}" for i in range(9)],
#                 cbar_kws={"label": "Pearson ρ"})
#     ax.set_title("Lagged Cross-Correlation: Brent Returns → Food Price Changes",
#                 fontweight="bold")
#     ax.set_xlabel("Oil Price Lag (weeks)")
#     ax.set_ylabel("")
#     plt.tight_layout()
#     fig.savefig(fig_dir / "correlation_heatmap.png", bbox_inches="tight")
#     plt.close(fig)
#     print(f"  [HEATMAP] Saved correlation heatmap.")


# # ============================================================================
# # 8. FULL INTEGRATION PIPELINE
# # ============================================================================

# def run_integration_pipeline(
#     brent_weekly: pd.DataFrame,
#     doe_weekly: pd.DataFrame,
#     dti_weekly: pd.DataFrame,
#     output_dir: Path = CLEANED_DIR,
#     run_models: bool = True,
# ) -> dict:
#     """
#     Full integration: lags → FSFI → SQL → merge → model.
#     """
#     print("=" * 60)
#     print("INTEGRATION & MODELING PIPELINE")
#     print("=" * 60)

#     output_dir.mkdir(parents=True, exist_ok=True)

#     # Lag features
#     print("\n▶ Creating lag features...")
#     lag_features = create_lag_features(brent_weekly, doe_weekly)

#     # FSFI
#     print("\n▶ Computing FSFI...")
#     fsfi, fsfi_weights, fsfi_corrs = compute_fsfi(dti_weekly, brent_weekly)

#     # SQLite (delegated to sql_pipeline.py)
#     print("\n▶ Creating SQLite database...")
#     from functions.sql_pipeline import create_database, validate_database
#     create_database(brent_weekly, doe_weekly, dti_weekly,
#                     lag_features, fsfi, fsfi_weights,
#                     db_path=output_dir / "oil_passthrough.db")
#     validate_database(db_path=output_dir / "oil_passthrough.db")

#     # Merged dataset
#     print("\n▶ Building merged dataset...")
#     merged = build_merged_dataset(
#         brent_weekly, doe_weekly, dti_weekly, fsfi, lag_features
#     )

#     # Export
#     merged.to_parquet(output_dir / "merged_analysis_ready.parquet", index=False)
#     merged.to_csv(output_dir / "merged_analysis_ready.csv", index=False)
#     lag_features.to_parquet(output_dir / "lag_features.parquet", index=False)
#     fsfi.to_parquet(output_dir / "fsfi.parquet", index=False)
#     print(f"  Saved merged_analysis_ready.parquet ({merged.shape})")

#     results = {
#         "merged": merged,
#         "lag_features": lag_features,
#         "fsfi": fsfi,
#         "fsfi_weights": fsfi_weights,
#     }

#     if run_models:
#         # Correlation heatmap
#         print("\n▶ Correlation heatmap...")
#         plot_correlation_heatmap(merged, brent_weekly, dti_weekly)

#         # Quantile regressions
#         print("\n▶ Quantile regressions...")
#         qr_results = run_multi_commodity_qr(merged)
#         results["qr_results"] = qr_results

#         # Save QR results
#         if qr_results:
#             all_qr = pd.concat(
#                 [df.assign(commodity=k) for k, df in qr_results.items()],
#                 ignore_index=True
#             )
#             all_qr.to_csv(output_dir / "quantile_regression_results.csv", index=False)
#             print(f"  Saved quantile_regression_results.csv")

#         # VAR model
#         print("\n▶ VAR model...")
#         var_results = run_var_model(merged)
#         results["var_results"] = var_results

#     return results

"""
integration_pipeline.py — Data integration, FSFI, and econometric modeling.

Key functions:
    create_lag_features     — build distributed lag regressors
    compute_fsfi            — Fuel-Sensitive Food Index (correlation-weighted)
    run_dti_eda             — DTI-specific EDA figures
    build_merged_dataset    — merge brent/doe/dti on weekly date
    plot_correlation_heatmap— lagged Brent × commodity correlations
    run_quantile_regression — per-commodity distributed-lag QR
    run_multi_commodity_qr  — sweep all commodities, produce summary chart
    run_var_model           — trivariate VAR + IRF + FEVD
"""

from __future__ import annotations
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

try:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_VIZ = True
except ImportError:
    HAS_VIZ = False

try:
    import statsmodels.api as sm
    from statsmodels.regression.quantile_regression import QuantReg
    from statsmodels.tsa.api import VAR
    HAS_SM = True
except ImportError:
    HAS_SM = False


def _apply_style():
    plt.rcParams.update({
        "figure.figsize": (6.5, 3.2), "figure.dpi": 110, "savefig.dpi": 110,
        "axes.titlesize": 10, "axes.labelsize": 9,
        "xtick.labelsize": 8, "ytick.labelsize": 8, "legend.fontsize": 8,
        "axes.grid": False, "axes.spines.top": False, "axes.spines.right": False,
    })


MAX_LAG = 8
FOOD_BASKET = [
    "canned sardines", "instant noodles", "milk condensada", "milk evaporada",
    "milk powdered", "coffee", "salt iodized", "bread loaf", "bread pandesal",
    "vinegar", "patis", "soy sauce", "canned beef corned",
    "canned pork luncheon meat", "canned beef loaf", "canned pork meat loaf",
]
QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90]


# ============================================================================
# LAG FEATURES
# ============================================================================

def create_lag_features(brent_w, doe_w, max_lags=MAX_LAG):
    """Distributed-lag regressors for oil-to-food pass-through estimation."""
    bl = brent_w[["date", "brent_wret", "brent_close", "brent_realized_vol"]].copy()
    for k in range(max_lags + 1):
        bl[f"brent_ret_lag{k}"] = bl["brent_wret"].shift(k)
        bl[f"brent_level_lag{k}"] = bl["brent_close"].shift(k)
        bl[f"brent_vol_lag{k}"] = bl["brent_realized_vol"].shift(k)
    d = doe_w[doe_w["Product"] == "Diesel"][
        ["date", "doe_log_return", "price_mid_median"]].copy()
    d.rename(columns={"doe_log_return": "diesel_ret",
                       "price_mid_median": "diesel_price"}, inplace=True)
    for k in range(max_lags + 1):
        d[f"diesel_ret_lag{k}"] = d["diesel_ret"].shift(k)
        d[f"diesel_level_lag{k}"] = d["diesel_price"].shift(k)
    lf = pd.merge(
        bl.drop(columns=["brent_wret", "brent_close", "brent_realized_vol"]),
        d.drop(columns=["diesel_ret", "diesel_price"]),
        on="date", how="outer")
    print(f"  [LAGS] {len([c for c in lf.columns if c != 'date'])} features")
    return lf


# ============================================================================
# FSFI
# ============================================================================

def compute_fsfi(dti_w, brent_w, food_basket=FOOD_BASKET, lag=4):
    """Construct correlation-weighted composite of fuel-sensitive food items."""
    dti_f = dti_w[dti_w["commodity"].isin(food_basket)]
    piv = dti_f.pivot_table(index="date", columns="commodity",
                             values="dti_dlog", aggfunc="first")
    br = brent_w[["date", "brent_wret"]].copy()
    m = pd.merge(piv, br, left_index=True, right_on="date",
                  how="inner").set_index("date")
    corrs = {}
    bs = m["brent_wret"].shift(lag)
    for c in piv.columns:
        v = m[[c]].join(bs.rename("bl")).dropna()
        if len(v) > 20:
            corrs[c] = v[c].corr(v["bl"])
    fs = {k: v for k, v in corrs.items() if v > 0}
    if not fs:
        fs = {k: 1 / len(corrs) for k in corrs}
    tot = sum(abs(v) for v in fs.values())
    weights = {k: abs(v) / tot for k, v in fs.items()}
    print("  [FSFI] Components:")
    for c, w in sorted(weights.items(), key=lambda x: -x[1]):
        print(f"    {c:35s} rho={fs[c]:+.4f}  w={w:.4f}")
    idx = pd.Series(0.0, index=piv.index, name="fsfi")
    for c, w in weights.items():
        if c in piv.columns:
            idx += w * piv[c].fillna(0)
    fdf = idx.reset_index()
    fdf.columns = ["date", "fsfi"]
    fdf["fsfi_cumulative"] = fdf["fsfi"].cumsum()
    return fdf, weights, corrs


# ============================================================================
# DTI EDA
# ============================================================================

def run_dti_eda(dti_clean, dti_weekly, fig_dir=Path("figures")):
    if not HAS_VIZ:
        return {}
    fig_dir.mkdir(parents=True, exist_ok=True)
    _apply_style()

    basket = [c for c in FOOD_BASKET if c in dti_weekly.commodity.unique()][:8]
    bd = dti_weekly[dti_weekly.commodity.isin(basket)]

    # Fig 1: Key food basket price trends
    fig, axes = plt.subplots(2, 4, figsize=(9, 4.5), sharex=True)
    axes = axes.flatten()
    for i, comm in enumerate(basket):
        d = bd[bd.commodity == comm]
        axes[i].plot(d.date, d.price_median, lw=0.8, color="#2ca02c")
        axes[i].set_title(comm.replace("canned ", "c. ").title(), fontsize=8)
        if i % 4 == 0:
            axes[i].set_ylabel("PHP")
    fig.suptitle("Weekly median prices of key food basket items, NCR 2020–2026",
                  fontsize=10)
    plt.tight_layout()
    fig.savefig(fig_dir / "dti_01_basket_trends.png", bbox_inches="tight")
    plt.close()

    # Fig 2: Category breakdown
    cat = dti_weekly.groupby(["category", "commodity"]).agg(
        avg=("price_median", "mean")).reset_index().sort_values("avg")
    fig, ax = plt.subplots(figsize=(6.5, 5))
    colors = {"basic": "#1f77b4", "prime": "#ff7f0e"}
    ax.barh(cat.commodity, cat.avg,
             color=[colors.get(c, "#999") for c in cat.category], height=0.6)
    ax.set_title("Average weekly median price by commodity, NCR 2020–2026")
    ax.set_xlabel("Average price (PHP)")
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=c, label=l.title()) for l, c in colors.items()],
               frameon=False)
    plt.tight_layout()
    fig.savefig(fig_dir / "dti_02_category.png", bbox_inches="tight")
    plt.close()

    # Fig 3: Cumulative log-price
    fig, ax = plt.subplots(figsize=(6.5, 3))
    for comm in basket:
        d = dti_weekly[dti_weekly.commodity == comm].sort_values("date")
        cum = d.dti_dlog.cumsum()
        ax.plot(d.date.values, cum.values, lw=0.8,
                 label=comm.replace("canned ", "c. ").title())
    ax.axhline(0, color="black", lw=0.5, ls="--")
    ax.set_title("Cumulative log-price change of food basket items, 2020–2026")
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative log-return")
    ax.legend(fontsize=6, ncol=2, frameon=False)
    plt.tight_layout()
    fig.savefig(fig_dir / "dti_03_cumulative.png", bbox_inches="tight")
    plt.close()

    # Fig 4: SKU counts per commodity
    sku_counts = dti_clean.groupby("commodity")["series_id"].nunique().sort_values()
    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    ax.barh(sku_counts.index, sku_counts.values, color="#1f77b4", height=0.6)
    ax.set_title("Market-leader SKU series per commodity")
    ax.set_xlabel("Number of SKU series")
    plt.tight_layout()
    fig.savefig(fig_dir / "dti_04_sku_counts.png", bbox_inches="tight")
    plt.close()

    print(f"  [DTI EDA] 4 figures saved to {fig_dir}/")
    return {}


# ============================================================================
# MERGED DATASET
# ============================================================================

def build_merged_dataset(brent_w, doe_w, dti_w, fsfi, lag_features):
    doe_wide = doe_w.pivot_table(index="date", columns="Product",
                                  values="price_mid_median", aggfunc="first")
    doe_wide.columns = [f"doe_{c.lower().replace(' ', '_')}" for c in doe_wide.columns]
    doe_wide = doe_wide.reset_index()
    doe_ret = doe_w.pivot_table(index="date", columns="Product",
                                 values="doe_log_return", aggfunc="first")
    doe_ret.columns = [f"doe_{c.lower().replace(' ', '_')}_ret" for c in doe_ret.columns]
    doe_ret = doe_ret.reset_index()
    dp = dti_w.pivot_table(index="date", columns="commodity",
                            values="price_median", aggfunc="first")
    dp.columns = [f"dti_{c.replace(' ', '_')}" for c in dp.columns]
    dp = dp.reset_index()
    dr = dti_w.pivot_table(index="date", columns="commodity",
                            values="dti_dlog", aggfunc="first")
    dr.columns = [f"dti_{c.replace(' ', '_')}_ret" for c in dr.columns]
    dr = dr.reset_index()
    mg = brent_w[["date", "brent_close", "brent_mean", "brent_wret",
                   "brent_realized_vol", "wti_close", "spread_mean"]].copy()
    for d in [mg, doe_wide, doe_ret, dp, dr, fsfi, lag_features]:
        d["date"] = pd.to_datetime(d["date"])
    mg = mg.merge(doe_wide, on="date", how="outer").merge(doe_ret, on="date", how="outer")
    mg = mg.merge(dp, on="date", how="outer").merge(dr, on="date", how="outer")
    mg = mg.merge(fsfi, on="date", how="outer").merge(lag_features, on="date", how="outer")
    mg = mg.sort_values("date").reset_index(drop=True)
    bd = set(brent_w.date.dt.date); dd = set(doe_w.date.dt.date); td = set(dti_w.date.dt.date)
    mg["has_brent"] = mg.date.dt.date.isin(bd).astype(int)
    mg["has_doe"] = mg.date.dt.date.isin(dd).astype(int)
    mg["has_dti"] = mg.date.dt.date.isin(td).astype(int)
    mg["all_sources"] = ((mg.has_brent == 1) & (mg.has_doe == 1) & (mg.has_dti == 1)).astype(int)
    print(f"  [MERGED] {len(mg)} weeks, {mg.all_sources.sum()} with all sources")
    return mg


# ============================================================================
# CORRELATION HEATMAP
# ============================================================================

def plot_correlation_heatmap(merged, brent_w, dti_w, fig_dir=Path("figures")):
    if not HAS_VIZ:
        return
    fig_dir.mkdir(parents=True, exist_ok=True)
    _apply_style()
    br = brent_w[["date", "brent_wret"]].dropna().copy()
    br["date"] = pd.to_datetime(br["date"])
    comms = [c for c in FOOD_BASKET if c in dti_w.commodity.unique()]
    piv = dti_w[dti_w.commodity.isin(comms)].pivot_table(
        index="date", columns="commodity", values="dti_dlog", aggfunc="first")
    piv.index = pd.to_datetime(piv.index)
    comb = piv.join(br.set_index("date")["brent_wret"], how="inner")
    cm = pd.DataFrame(index=comms, columns=range(9))
    for lag in range(9):
        sh = comb["brent_wret"].shift(lag)
        for comm in comms:
            if comm in comb.columns:
                v = comb[[comm]].join(sh.rename("b")).dropna()
                if len(v) > 15:
                    cm.loc[comm, lag] = v[comm].corr(v["b"])
    cm = cm.astype(float)
    fig, ax = plt.subplots(figsize=(7, max(3.5, len(comms) * 0.28)))
    sns.heatmap(cm, annot=True, fmt=".3f", cmap="RdYlGn", center=0, ax=ax,
                 linewidths=0.3,
                 xticklabels=[f"Lag {i}" for i in range(9)],
                 cbar_kws={"label": "Pearson correlation"})
    ax.set_title("Lagged Pearson correlation: Brent weekly returns and commodity price changes")
    ax.set_ylabel("")
    plt.tight_layout()
    fig.savefig(fig_dir / "analysis_01_heatmap.png", bbox_inches="tight")
    plt.close()
    print("  [HEATMAP] Saved")


# ============================================================================
# QUANTILE REGRESSION
# ============================================================================

def run_quantile_regression(merged, target="canned_sardines", quantiles=QUANTILES,
                              max_lag=4, fig_dir=Path("figures")):
    if not HAS_SM:
        return pd.DataFrame()
    fig_dir.mkdir(parents=True, exist_ok=True)
    _apply_style()
    ycol = f"dti_{target}_ret"
    if ycol not in merged.columns:
        return pd.DataFrame()
    xcols = [f"brent_ret_lag{k}" for k in range(max_lag + 1)]
    rd = merged[merged.all_sources == 1][[ycol] + xcols].dropna()
    if len(rd) < 30:
        return pd.DataFrame()
    y = rd[ycol]
    X = sm.add_constant(rd[xcols])
    results = []
    for tau in quantiles:
        res = QuantReg(y, X).fit(q=tau, max_iter=1000)
        for v in X.columns:
            results.append({
                "quantile": tau, "variable": v,
                "coef": res.params[v], "std_err": res.bse[v],
                "p_value": res.pvalues[v],
                "ci_lower": res.conf_int().loc[v, 0],
                "ci_upper": res.conf_int().loc[v, 1],
            })
    rdf = pd.DataFrame(results)

    if HAS_VIZ:
        lvars = [c for c in xcols if c in X.columns]
        fig, axes = plt.subplots(1, len(lvars), figsize=(min(9, 2 * len(lvars)), 2.8),
                                   sharey=True)
        if len(lvars) == 1:
            axes = [axes]
        for i, v in enumerate(lvars):
            vd = rdf[rdf.variable == v]
            # Access via bracket notation to avoid .quantile() method conflict
            x_vals = vd["quantile"].values
            y_vals = vd["coef"].values
            low = vd["ci_lower"].values
            high = vd["ci_upper"].values
            axes[i].plot(x_vals, y_vals, "o-", color="#1f77b4", lw=1, ms=3)
            axes[i].fill_between(x_vals, low, high, alpha=0.15, color="#1f77b4")
            axes[i].axhline(0, color="#d62728", ls="--", lw=0.5)
            axes[i].set_title(v.replace("brent_ret_lag", "Lag "), fontsize=8)
            axes[i].set_xlabel("Quantile τ")
            if i == 0:
                axes[i].set_ylabel("Coefficient")
        fig.suptitle(f"Quantile regression: Brent returns on {target.replace('_', ' ').title()}",
                      fontsize=9)
        plt.tight_layout()
        fig.savefig(fig_dir / f"qr_{target}.png", bbox_inches="tight")
        plt.close()
    return rdf


def run_multi_commodity_qr(merged, commodities=None, fig_dir=Path("figures")):
    if commodities is None:
        commodities = [c.replace("dti_", "").replace("_ret", "")
                        for c in merged.columns
                        if c.startswith("dti_") and c.endswith("_ret")]
    all_res = {}
    rows = []
    for c in commodities:
        r = run_quantile_regression(merged, c, fig_dir=fig_dir)
        if not r.empty:
            all_res[c] = r
            # BUGFIX: use bracket access, not attribute access (.quantile() is pd method)
            m50 = r[(r["quantile"] == 0.50) & (r["variable"] == "brent_ret_lag0")]
            if not m50.empty:
                rows.append({
                    "commodity": c,
                    "elasticity_lag0": m50["coef"].values[0],
                    "p_value": m50["p_value"].values[0],
                })
    if rows and HAS_VIZ:
        _apply_style()
        s = pd.DataFrame(rows).sort_values("elasticity_lag0")
        fig, ax = plt.subplots(figsize=(6.5, max(3.5, len(s) * 0.22)))
        colors = ["#d62728" if p < 0.10 else "#aec7e8" for p in s["p_value"]]
        ax.barh(s["commodity"].str.replace("_", " ").str.title(),
                 s["elasticity_lag0"], color=colors, height=0.6)
        ax.axvline(0, color="black", lw=0.5)
        ax.set_title("Contemporaneous Brent elasticity by commodity, median quantile")
        ax.set_xlabel(r"$\hat\beta$ at $\tau=0.50$")
        from matplotlib.patches import Patch
        ax.legend(handles=[
            Patch(color="#d62728", label="p < 0.10"),
            Patch(color="#aec7e8", label="p ≥ 0.10"),
        ], frameon=False)
        plt.tight_layout()
        fig.savefig(fig_dir / "qr_summary.png", bbox_inches="tight")
        plt.close()
        print(f"  [QR SUMMARY] Saved {fig_dir}/qr_summary.png with {len(rows)} commodities")
    return all_res


# ============================================================================
# VAR MODEL
# ============================================================================

def run_var_model(merged, fig_dir=Path("figures"), max_order=4, irf_periods=12):
    if not HAS_SM:
        return {}
    fig_dir.mkdir(parents=True, exist_ok=True)
    _apply_style()
    vcols = ["brent_wret", "doe_diesel_ret", "fsfi"]
    avail = [c for c in vcols if c in merged.columns]
    if len(avail) < 3:
        return {}
    vd = merged[merged.all_sources == 1][["date"] + vcols].dropna()
    if len(vd) < 40:
        return {}
    vd = vd.set_index("date")[vcols]
    model = VAR(vd)
    lo = model.select_order(maxlags=max_order)
    opt = lo.aic
    print(f"  [VAR] AIC-optimal lag: {opt}, N={len(vd)}")
    res = model.fit(opt)
    print(f"  [VAR] AIC={res.aic:.4f}")
    try:
        gc = res.test_causality("fsfi", "brent_wret", kind="f")
        print(f"  [VAR] Granger (Brent → FSFI): F={gc.test_statistic:.3f}, p={gc.pvalue:.4f}")
    except Exception as e:
        print(f"  [VAR] Granger failed: {e}")

    irf = res.irf(irf_periods)
    if HAS_VIZ:
        labels = ["Brent returns", "Diesel returns", "FSFI"]
        fig, axes = plt.subplots(1, 3, figsize=(8, 2.8))
        for i, t in enumerate(vcols):
            resp = irf.irfs[:, i, 0]
            axes[i].plot(range(len(resp)), resp, "o-",
                          color="#1f77b4", lw=1, ms=2.5)
            axes[i].axhline(0, color="#d62728", ls="--", lw=0.5)
            axes[i].set_title(f"Response of {labels[i]}", fontsize=8)
            axes[i].set_xlabel("Weeks after shock")
            if i == 0:
                axes[i].set_ylabel("Response")
        fig.suptitle("Impulse response: one standard deviation Brent return shock",
                     fontsize=9)
        plt.tight_layout()
        fig.savefig(fig_dir / "var_irf.png", bbox_inches="tight")
        plt.close()

        fevd = res.fevd(irf_periods)
        fig, axes = plt.subplots(1, 3, figsize=(8, 2.8))
        for i, t in enumerate(vcols):
            dc = fevd.decomp[i]
            ns = dc.shape[0]
            for j, s in enumerate(vcols):
                axes[i].plot(range(ns), dc[:, j], lw=0.9, label=labels[j])
            axes[i].set_title(f"FEVD of {labels[i]}", fontsize=8)
            axes[i].set_xlabel("Weeks after shock")
            axes[i].set_ylim(0, 1)
            if i == 0:
                axes[i].set_ylabel("Variance share")
                axes[i].legend(frameon=False, fontsize=6)
        fig.suptitle("Forecast error variance decomposition", fontsize=9)
        plt.tight_layout()
        fig.savefig(fig_dir / "var_fevd.png", bbox_inches="tight")
        plt.close()

    return {"results": res, "irf": irf, "optimal_lag": opt}