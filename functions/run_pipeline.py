# """
# run_pipeline.py
# ===============
# Execute the full end-to-end Fuel-to-Food pipeline.

# Usage:
#     python run_pipeline.py

# This script orchestrates all 6 modules in sequence:
#     1. utils.py          — (extraction functions, called separately)
#     2. brent_pipeline.py — Clean + EDA for Brent/WTI
#     3. doe_pipeline.py   — Clean + EDA for DOE pump prices
#     4. dti_pipeline.py   — Load weekly panel + coverage filter + aggregate
#     5. sql_pipeline.py   — Create SQLite database
#     6. integration_pipeline.py — Merge + FSFI + QR + VAR
# """

# import sys
# from pathlib import Path

# sys.path.insert(0, str(Path(__file__).parent))

# from functions.brent_pipeline import load_brent, clean_brent, aggregate_weekly, run_brent_eda
# from functions.doe_pipeline import load_doe, clean_doe, aggregate_doe_weekly, run_doe_eda
# from functions.dti_pipeline import (
#     load_weekly_panel, filter_by_coverage,
#     clean_weekly_panel, aggregate_commodity_weekly,
# )
# from functions.sql_pipeline import create_database, validate_database
# from functions.integration_pipeline import (
#     create_lag_features, compute_fsfi,
#     build_merged_dataset, plot_correlation_heatmap,
#     run_multi_commodity_qr, run_var_model,
# )

# # Paths
# SEMI = Path("data/semi_cleaned_data")
# CLEAN = Path("data/cleaned_data")
# FIG = Path("figures")


# def main():
#     CLEAN.mkdir(parents=True, exist_ok=True)

#     # ================================================================
#     # MODULE 2: BRENT
#     # ================================================================
#     print("=" * 60)
#     print("MODULE 2: BRENT PIPELINE")
#     print("=" * 60)

#     print("\n▶ Loading...")
#     brent_raw = load_brent(SEMI / "Brent_WTI_Prices_2020_to_Present.csv")

#     print("\n▶ Cleaning...")
#     brent_clean = clean_brent(brent_raw)

#     print("\n▶ Weekly aggregation...")
#     brent_weekly = aggregate_weekly(brent_clean)

#     print("\n▶ EDA...")
#     brent_eda = run_brent_eda(brent_clean, fig_dir=FIG / "brent")

#     print("\n▶ Exporting...")
#     brent_clean.to_parquet(CLEAN / "brent_daily_cleaned.parquet", index=False)
#     brent_weekly.to_parquet(CLEAN / "brent_weekly.parquet", index=False)

#     # ================================================================
#     # MODULE 3: DOE
#     # ================================================================
#     print("\n" + "=" * 60)
#     print("MODULE 3: DOE PIPELINE")
#     print("=" * 60)

#     print("\n▶ Loading...")
#     doe_raw = load_doe(SEMI / "DOE_NCR_OilPrices_Compiled.csv")

#     print("\n▶ Cleaning...")
#     doe_clean = clean_doe(doe_raw)

#     print("\n▶ Weekly aggregation...")
#     doe_weekly = aggregate_doe_weekly(doe_clean)

#     print("\n▶ EDA...")
#     doe_eda = run_doe_eda(doe_clean, doe_weekly, fig_dir=FIG / "doe")

#     print("\n▶ Exporting...")
#     doe_clean.to_parquet(CLEAN / "doe_ncr_cleaned.parquet", index=False)
#     doe_weekly.to_parquet(CLEAN / "doe_weekly_ncr.parquet", index=False)

#     # ================================================================
#     # MODULE 4: DTI
#     # ================================================================
#     print("\n" + "=" * 60)
#     print("MODULE 4: DTI PIPELINE")
#     print("=" * 60)

#     print("\n▶ Loading weekly panel (output of dti_phase v3)...")
#     dti_raw = load_weekly_panel(SEMI / "dti_weekly_panel.csv")

#     print("\n▶ Coverage filtering...")
#     dti_filtered, coverage_report = filter_by_coverage(dti_raw)

#     print("\n▶ Cleaning...")
#     dti_clean = clean_weekly_panel(dti_filtered)

#     print("\n▶ Commodity-level aggregation...")
#     dti_weekly = aggregate_commodity_weekly(dti_clean)

#     print("\n▶ Exporting...")
#     dti_clean.to_parquet(CLEAN / "dti_panel_cleaned.parquet", index=False)
#     dti_weekly.to_parquet(CLEAN / "dti_weekly_commodity.parquet", index=False)
#     coverage_report.to_csv(CLEAN / "dti_coverage_report.csv", index=False)

#     # ================================================================
#     # MODULE 6: INTEGRATION + MODELING
#     # ================================================================
#     print("\n" + "=" * 60)
#     print("MODULE 6: INTEGRATION & MODELING")
#     print("=" * 60)

#     print("\n▶ Creating lag features...")
#     lag_features = create_lag_features(brent_weekly, doe_weekly)

#     print("\n▶ Computing FSFI...")
#     fsfi, fsfi_weights, fsfi_corrs = compute_fsfi(dti_weekly, brent_weekly)

#     # ================================================================
#     # MODULE 5: SQL DATABASE
#     # ================================================================
#     print("\n▶ Creating SQLite database...")
#     db_path = CLEAN / "oil_passthrough.db"
#     create_database(
#         brent_weekly, doe_weekly, dti_weekly,
#         lag_features, fsfi, fsfi_weights,
#         db_path=db_path,
#     )
#     validate_database(db_path)

#     # ================================================================
#     # MERGE + MODEL
#     # ================================================================
#     print("\n▶ Building merged dataset...")
#     merged = build_merged_dataset(
#         brent_weekly, doe_weekly, dti_weekly, fsfi, lag_features
#     )
#     merged.to_parquet(CLEAN / "merged_analysis_ready.parquet", index=False)
#     merged.to_csv(CLEAN / "merged_analysis_ready.csv", index=False)

#     # Save lag features and FSFI
#     lag_features.to_parquet(CLEAN / "lag_features.parquet", index=False)
#     fsfi.to_parquet(CLEAN / "fsfi.parquet", index=False)

#     print("\n▶ Correlation heatmap...")
#     plot_correlation_heatmap(merged, brent_weekly, dti_weekly, fig_dir=FIG / "analysis")

#     print("\n▶ Quantile regressions...")
#     qr_results = run_multi_commodity_qr(merged, fig_dir=FIG / "analysis")
#     if qr_results:
#         import pandas as pd
#         all_qr = pd.concat(
#             [df.assign(commodity=k) for k, df in qr_results.items()],
#             ignore_index=True
#         )
#         all_qr.to_csv(CLEAN / "quantile_regression_results.csv", index=False)

#     print("\n▶ VAR model...")
#     var_results = run_var_model(merged, fig_dir=FIG / "analysis")

#     # ================================================================
#     print("\n" + "=" * 60)
#     print("ALL PIPELINES COMPLETE")
#     print("=" * 60)
#     print(f"\nOutputs: {CLEAN.absolute()}")
#     print(f"Figures: {FIG.absolute()}")
#     print(f"Database: {db_path.absolute()}")


# if __name__ == "__main__":
#     main()

"""run_pipeline.py — Execute the full Fuel-to-Food pipeline."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from functions.brent_pipeline import load_brent, clean_brent, aggregate_weekly, run_brent_eda
from functions.doe_pipeline import load_doe, clean_doe, aggregate_doe_weekly, run_doe_eda
from functions.dti_pipeline import (
    load_weekly_panel, filter_by_coverage, clean_weekly_panel, aggregate_commodity_weekly)
from functions.sql_pipeline import create_database, validate_database
from functions.integration_pipeline import (
    create_lag_features, compute_fsfi, run_dti_eda,
    build_merged_dataset, plot_correlation_heatmap,
    run_multi_commodity_qr, run_var_model)

SEMI = Path("data/semi_cleaned_data")
CLEAN = Path("data/cleaned_data")
FIG = Path("figures")

def main():
    CLEAN.mkdir(parents=True, exist_ok=True)

    # == BRENT ==
    print("="*60+"\nBRENT\n"+"="*60)
    b = clean_brent(load_brent(SEMI/"Brent_WTI_Prices_2020_to_Present.csv"))
    bw = aggregate_weekly(b)
    run_brent_eda(b, fig_dir=FIG)
    b.to_parquet(CLEAN/"brent_daily_cleaned.parquet", index=False)
    bw.to_parquet(CLEAN/"brent_weekly.parquet", index=False)

    # == DOE ==
    print("\n"+"="*60+"\nDOE\n"+"="*60)
    d = clean_doe(load_doe(SEMI/"DOE_NCR_OilPrices_Compiled.csv"))
    dw = aggregate_doe_weekly(d)
    run_doe_eda(d, dw, fig_dir=FIG)
    d.to_parquet(CLEAN/"doe_ncr_cleaned.parquet", index=False)
    dw.to_parquet(CLEAN/"doe_weekly_ncr.parquet", index=False)

    # == DTI ==
    print("\n"+"="*60+"\nDTI\n"+"="*60)
    t_raw = load_weekly_panel(SEMI/"dti_weekly_panel.csv")
    t_filt, cov_report = filter_by_coverage(t_raw)
    t_clean = clean_weekly_panel(t_filt)  # SKU-level panel (all 215 series)
    t_weekly = aggregate_commodity_weekly(t_clean)  # commodity-level (for FSFI/heatmap)
    run_dti_eda(t_clean, t_weekly, fig_dir=FIG)
    t_clean.to_parquet(CLEAN/"dti_panel_cleaned.parquet", index=False)
    t_weekly.to_parquet(CLEAN/"dti_weekly_commodity.parquet", index=False)
    cov_report.to_csv(CLEAN/"dti_coverage_report.csv", index=False)

    # == INTEGRATION ==
    print("\n"+"="*60+"\nINTEGRATION & MODELING\n"+"="*60)
    lf = create_lag_features(bw, dw)
    fsfi, fw, fc = compute_fsfi(t_weekly, bw)
    db = CLEAN/"oil_passthrough.db"
    create_database(bw, dw, t_weekly, t_clean, lf, fsfi, fw, db_path=db)
    validate_database(db)
    mg = build_merged_dataset(bw, dw, t_weekly, fsfi, lf)
    mg.to_parquet(CLEAN/"merged_analysis_ready.parquet", index=False)
    mg.to_csv(CLEAN/"merged_analysis_ready.csv", index=False)
    lf.to_parquet(CLEAN/"lag_features.parquet", index=False)
    fsfi.to_parquet(CLEAN/"fsfi.parquet", index=False)
    plot_correlation_heatmap(mg, bw, t_weekly, fig_dir=FIG)
    qr = run_multi_commodity_qr(mg, fig_dir=FIG)
    if qr:
        import pandas as pd
        pd.concat([df.assign(commodity=k) for k,df in qr.items()], ignore_index=True
                  ).to_csv(CLEAN/"quantile_regression_results.csv", index=False)
    run_var_model(mg, fig_dir=FIG)

    print("\n"+"="*60+"\nDONE\n"+"="*60)
    print(f"Outputs: {CLEAN.absolute()}")
    print(f"Figures: {FIG.absolute()}")

if __name__ == "__main__":
    main()