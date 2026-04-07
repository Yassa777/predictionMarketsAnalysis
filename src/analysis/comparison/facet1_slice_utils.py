"""Shared utilities for Phase 1 / Facet 1 slice analyses."""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[3]
DEFAULT_DATASET_PATH = BASE_DIR / "data" / "derived" / "facet1_enriched_market_dataset.parquet"

PLATFORM_COLORS = {
    "kalshi": "#1f77b4",
    "polymarket": "#ff7f0e",
}


def load_enriched_dataset(dataset_path: Path | str | None = None) -> pd.DataFrame:
    """Load the enriched market-level Facet 1 dataset."""
    dataset = Path(dataset_path or DEFAULT_DATASET_PATH)
    if not dataset.exists():
        raise FileNotFoundError(
            f"Enriched dataset not found: {dataset}. "
            "Run scripts/build_facet1_enriched_market_dataset.py first."
        )

    con = duckdb.connect()
    return con.execute(f"SELECT * FROM '{dataset}'").df()


def add_wilson_intervals(df: pd.DataFrame) -> pd.DataFrame:
    """Add 95% Wilson score intervals for empirical win rates."""
    z = 1.96
    n = df["market_count"].astype(float).to_numpy()
    p = df["empirical_win_rate"].astype(float).to_numpy()

    denominator = 1.0 + (z**2 / n)
    center = (p + (z**2 / (2.0 * n))) / denominator
    margin = (
        z
        * np.sqrt((p * (1.0 - p) / n) + (z**2 / (4.0 * n**2)))
        / denominator
    )

    df = df.copy()
    df["empirical_win_rate_ci_low"] = np.clip(center - margin, 0.0, 1.0)
    df["empirical_win_rate_ci_high"] = np.clip(center + margin, 0.0, 1.0)
    return df


def compute_bucket_calibration(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """Compute bucket-level calibration statistics for arbitrary slices."""
    bucket_cols = group_cols + [
        "price_bucket_5c_floor",
        "price_bucket_5c_mid",
        "price_bucket_5c_label",
    ]
    bucket_df = (
        df.groupby(bucket_cols, dropna=False)
        .agg(
            market_count=("reference_won", "size"),
            wins=("reference_won", "sum"),
            empirical_win_rate=("reference_won", "mean"),
            avg_implied_probability=("reference_price_cents", lambda s: s.mean() / 100.0),
        )
        .reset_index()
    )
    bucket_df["calibration_gap"] = (
        bucket_df["empirical_win_rate"] - bucket_df["avg_implied_probability"]
    )
    return add_wilson_intervals(bucket_df)


def compute_slice_summary(bucket_df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """Collapse bucket-level stats into slice-level summaries."""
    summary_rows: list[dict] = []

    for keys, group in bucket_df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)

        total_markets = int(group["market_count"].sum())
        weighted_abs_gap = (
            (group["calibration_gap"].abs() * group["market_count"]).sum() / total_markets
        )
        weighted_signed_gap = (
            (group["calibration_gap"] * group["market_count"]).sum() / total_markets
        )

        low_tail = group[group["price_bucket_5c_floor"] < 15]
        high_tail = group[group["price_bucket_5c_floor"] >= 85]

        row = dict(zip(group_cols, keys))
        row.update(
            {
                "market_count": total_markets,
                "bucket_count": int(len(group)),
                "expected_calibration_error": weighted_abs_gap,
                "signed_calibration_gap": weighted_signed_gap,
                "low_tail_gap": (
                    (low_tail["calibration_gap"] * low_tail["market_count"]).sum()
                    / low_tail["market_count"].sum()
                    if not low_tail.empty
                    else np.nan
                ),
                "high_tail_gap": (
                    (high_tail["calibration_gap"] * high_tail["market_count"]).sum()
                    / high_tail["market_count"].sum()
                    if not high_tail.empty
                    else np.nan
                ),
            }
        )
        summary_rows.append(row)

    return pd.DataFrame(summary_rows)
