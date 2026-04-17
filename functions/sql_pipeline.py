# """
# sql_pipeline.py
# ===============
# Module 5: SQLite database creation, validation, and query utilities.

# Creates a normalized relational database from the cleaned weekly datasets.
# All tables are indexed on `date` for efficient JOINs across the oil→fuel→food
# price chain.

# Schema:
#     oil_prices    — Weekly Brent/WTI (PK: date)
#     fuel_prices   — Weekly DOE by product (PK: date, product)
#     food_prices   — Weekly DTI by commodity (PK: date, commodity)
#     lag_features  — Precomputed distributed lag variables (PK: date)
#     fsfi          — Fuel-Sensitive Food Index (PK: date)
#     fsfi_weights  — FSFI component weights (reference table)
#     metadata      — Pipeline provenance and run parameters
# """

# from __future__ import annotations

# import sqlite3
# from datetime import datetime
# from pathlib import Path
# from typing import Dict, Optional

# import numpy as np
# import pandas as pd
# from sqlalchemy import create_engine, text


# # ============================================================================
# # CONFIGURATION
# # ============================================================================

# DEFAULT_DB_PATH = Path("data/cleaned_data/oil_passthrough.db")


# # ============================================================================
# # DATABASE CREATION
# # ============================================================================

# def create_database(
#     brent_w: pd.DataFrame,
#     doe_w: pd.DataFrame,
#     dti_w: pd.DataFrame,
#     lag_features: pd.DataFrame,
#     fsfi: pd.DataFrame,
#     fsfi_weights: Dict[str, float],
#     db_path: Path = DEFAULT_DB_PATH,
# ):
#     """
#     Create normalized SQLite database with 7 tables.

#     All date columns stored as ISO-8601 strings for SQLite compatibility.
#     Indices on date columns for fast JOINs.
#     """
#     db_path.parent.mkdir(parents=True, exist_ok=True)
#     if db_path.exists():
#         db_path.unlink()

#     engine = create_engine(f"sqlite:///{db_path}")

#     with engine.connect() as conn:
#         # ── oil_prices ──
#         oil = brent_w.copy()
#         oil["date"] = pd.to_datetime(oil["date"]).dt.strftime("%Y-%m-%d")
#         oil.to_sql("oil_prices", conn, if_exists="replace", index=False)
#         conn.execute(text("CREATE INDEX idx_oil_date ON oil_prices(date)"))

#         # ── fuel_prices ──
#         fuel = doe_w.copy()
#         fuel["date"] = pd.to_datetime(fuel["date"]).dt.strftime("%Y-%m-%d")
#         fuel.rename(columns={"Product": "product"}, inplace=True)
#         fuel.to_sql("fuel_prices", conn, if_exists="replace", index=False)
#         conn.execute(text("CREATE INDEX idx_fuel_date ON fuel_prices(date)"))
#         conn.execute(text("CREATE INDEX idx_fuel_prod ON fuel_prices(product)"))

#         # ── food_prices ──
#         food = dti_w.copy()
#         food["date"] = pd.to_datetime(food["date"]).dt.strftime("%Y-%m-%d")
#         food.to_sql("food_prices", conn, if_exists="replace", index=False)
#         conn.execute(text("CREATE INDEX idx_food_date ON food_prices(date)"))
#         conn.execute(text("CREATE INDEX idx_food_comm ON food_prices(commodity)"))

#         # ── lag_features ──
#         lags = lag_features.copy()
#         lags["date"] = pd.to_datetime(lags["date"]).dt.strftime("%Y-%m-%d")
#         lags.to_sql("lag_features", conn, if_exists="replace", index=False)
#         conn.execute(text("CREATE INDEX idx_lag_date ON lag_features(date)"))

#         # ── fsfi ──
#         f = fsfi.copy()
#         f["date"] = pd.to_datetime(f["date"]).dt.strftime("%Y-%m-%d")
#         f.to_sql("fsfi", conn, if_exists="replace", index=False)
#         conn.execute(text("CREATE INDEX idx_fsfi_date ON fsfi(date)"))

#         # ── fsfi_weights ──
#         w_df = pd.DataFrame([
#             {"commodity": k, "weight": v}
#             for k, v in fsfi_weights.items()
#         ])
#         w_df.to_sql("fsfi_weights", conn, if_exists="replace", index=False)

#         # ── metadata ──
#         meta = pd.DataFrame([{
#             "pipeline_version": "3.0",
#             "run_timestamp": datetime.now().isoformat(),
#             "brent_range": f"{brent_w.date.min()} to {brent_w.date.max()}",
#             "doe_range": f"{doe_w.date.min()} to {doe_w.date.max()}",
#             "dti_range": f"{dti_w.date.min()} to {dti_w.date.max()}",
#             "imputation": "median (Huber, 1981)",
#             "lag_max": 8,
#             "coverage_threshold": "70%",
#         }])
#         meta.to_sql("metadata", conn, if_exists="replace", index=False)

#         conn.commit()

#     print(f"  [SQL] Created {db_path} with 7 tables.")


# # ============================================================================
# # VALIDATION
# # ============================================================================

# def validate_database(db_path: Path = DEFAULT_DB_PATH) -> pd.DataFrame:
#     """Run validation queries on the database."""
#     engine = create_engine(f"sqlite:///{db_path}")

#     with engine.connect() as conn:
#         counts = pd.read_sql("""
#             SELECT 'oil_prices' AS tbl, COUNT(*) AS n FROM oil_prices
#             UNION ALL SELECT 'fuel_prices', COUNT(*) FROM fuel_prices
#             UNION ALL SELECT 'food_prices', COUNT(*) FROM food_prices
#             UNION ALL SELECT 'lag_features', COUNT(*) FROM lag_features
#             UNION ALL SELECT 'fsfi', COUNT(*) FROM fsfi
#             UNION ALL SELECT 'fsfi_weights', COUNT(*) FROM fsfi_weights
#         """, conn)

#         sample = pd.read_sql("""
#             SELECT o.date, ROUND(o.brent_close, 2) AS brent,
#                    f.product, ROUND(f.price_mid_median, 2) AS fuel_php,
#                    fp.commodity, ROUND(fp.price_median, 2) AS food_php
#             FROM oil_prices o
#             JOIN fuel_prices f ON o.date = f.date AND f.product = 'Diesel'
#             JOIN food_prices fp ON o.date = fp.date
#                 AND fp.commodity = 'canned sardines'
#             ORDER BY o.date DESC LIMIT 5
#         """, conn)

#     print("\n  Table row counts:")
#     for _, row in counts.iterrows():
#         print(f"    {row['tbl']:20s} {row['n']:>6d}")
#     print("\n  Sample JOIN (oil→fuel→food):")
#     print("  " + sample.to_string(index=False).replace("\n", "\n  "))
#     return counts


# # ============================================================================
# # QUERY UTILITIES
# # ============================================================================

# def query_db(query: str, db_path: Path = DEFAULT_DB_PATH) -> pd.DataFrame:
#     """Execute arbitrary SQL query and return DataFrame."""
#     engine = create_engine(f"sqlite:///{db_path}")
#     with engine.connect() as conn:
#         return pd.read_sql(query, conn)


# def get_pass_through_data(
#     commodity: str = "canned sardines",
#     fuel: str = "Diesel",
#     db_path: Path = DEFAULT_DB_PATH,
# ) -> pd.DataFrame:
#     """
#     Convenience: get aligned oil→fuel→food data for a single commodity.
#     Ready for regression.
#     """
#     query = f"""
#         SELECT o.date,
#                o.brent_close, o.brent_wret,
#                f.price_mid_median AS fuel_price, f.doe_log_return AS fuel_ret,
#                fp.price_median AS food_price, fp.dti_dlog AS food_ret
#         FROM oil_prices o
#         JOIN fuel_prices f ON o.date = f.date AND f.product = '{fuel}'
#         JOIN food_prices fp ON o.date = fp.date AND fp.commodity = '{commodity}'
#         ORDER BY o.date
#     """
#     return query_db(query, db_path)


"""
sql_pipeline.py — SQLite database creation and validation.

Schema: oil_prices, fuel_prices, food_prices, food_prices_sku,
        lag_features, fsfi, fsfi_weights, metadata
"""

from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import Dict
import numpy as np, pandas as pd
from sqlalchemy import create_engine, text

DEFAULT_DB = Path("data/cleaned_data/oil_passthrough.db")


def create_database(brent_w, doe_w, dti_w, dti_sku, lag_features, fsfi, fsfi_weights, db_path=DEFAULT_DB):
    """Create normalized SQLite DB. dti_w = commodity-level, dti_sku = SKU-level."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists(): db_path.unlink()
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as c:
        def _store(df, name, idx_cols):
            d = df.copy()
            for col in d.columns:
                if pd.api.types.is_datetime64_any_dtype(d[col]):
                    d[col] = d[col].dt.strftime("%Y-%m-%d")
                if hasattr(d[col].dtype, "freq"):
                    d[col] = d[col].dt.to_timestamp().dt.strftime("%Y-%m-%d")
            if "Product" in d.columns: d = d.rename(columns={"Product": "product"})
            d.to_sql(name, c, if_exists="replace", index=False)
            for col in idx_cols:
                c.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{name}_{col} ON {name}({col})"))

        _store(brent_w, "oil_prices", ["date"])
        _store(doe_w, "fuel_prices", ["date", "product"])
        _store(dti_w, "food_prices", ["date", "commodity"])
        _store(dti_sku, "food_prices_sku", ["date", "series_id"])
        _store(lag_features, "lag_features", ["date"])
        _store(fsfi, "fsfi", ["date"])

        w_df = pd.DataFrame([{"commodity": k, "weight": v} for k, v in fsfi_weights.items()])
        w_df.to_sql("fsfi_weights", c, if_exists="replace", index=False)

        pd.DataFrame([{
            "pipeline_version": "4.0", "run_timestamp": datetime.now().isoformat(),
            "brent_range": f"{brent_w.date.min()} to {brent_w.date.max()}",
            "doe_range": f"{doe_w.date.min()} to {doe_w.date.max()}",
            "dti_range": f"{dti_w.date.min()} to {dti_w.date.max()}",
            "imputation": "median (Huber 1981)", "lag_max": 8,
        }]).to_sql("metadata", c, if_exists="replace", index=False)
        c.commit()
    print(f"  [SQL] Created {db_path} with 8 tables")


def validate_database(db_path=DEFAULT_DB):
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as c:
        counts = pd.read_sql("""
            SELECT 'oil_prices' t, COUNT(*) n FROM oil_prices UNION ALL
            SELECT 'fuel_prices', COUNT(*) FROM fuel_prices UNION ALL
            SELECT 'food_prices', COUNT(*) FROM food_prices UNION ALL
            SELECT 'food_prices_sku', COUNT(*) FROM food_prices_sku UNION ALL
            SELECT 'lag_features', COUNT(*) FROM lag_features UNION ALL
            SELECT 'fsfi', COUNT(*) FROM fsfi UNION ALL
            SELECT 'fsfi_weights', COUNT(*) FROM fsfi_weights
        """, c)
        sample = pd.read_sql("""
            SELECT o.date, ROUND(o.brent_close,2) brent,
                   f.product, ROUND(f.price_mid_median,2) fuel,
                   fp.commodity, ROUND(fp.price_median,2) food
            FROM oil_prices o
            JOIN fuel_prices f ON o.date=f.date AND f.product='Diesel'
            JOIN food_prices fp ON o.date=fp.date AND fp.commodity='canned sardines'
            ORDER BY o.date DESC LIMIT 5
        """, c)
    print("\n  Table counts:")
    for _, r in counts.iterrows(): print(f"    {r['t']:20s} {r['n']:>6d}")
    print("\n  Sample JOIN:"); print("  " + sample.to_string(index=False).replace("\n","\n  "))
    return counts