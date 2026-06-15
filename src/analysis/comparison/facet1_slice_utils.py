"""Shared utilities for Phase 1 / Facet 1 slice analyses."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from src.analysis.kalshi.util.categories import get_hierarchy
from src.common.analysis import Analysis

BASE_DIR = Path(__file__).resolve().parents[3]
DEFAULT_DATASET_PATH = BASE_DIR / "data" / "derived" / "facet1_enriched_market_dataset.parquet"
DEFAULT_MARKET_DATASET_PATH = DEFAULT_DATASET_PATH
DEFAULT_HORIZON_DATASET_PATH = BASE_DIR / "data" / "derived" / "facet1_time_to_expiry_enriched_dataset.parquet"
FACET1_TABLE_OUTPUT_DIR = BASE_DIR / "research" / "tables" / "facet1"
FACET1_CHART_OUTPUT_DIR = BASE_DIR / "research" / "latex_charts" / "facet1"

PLATFORM_COLORS = {
    "kalshi": "#1f77b4",
    "polymarket": "#ff7f0e",
}
HORIZON_ORDER = ["30d", "7d", "3d", "1d", "6h", "1h"]
HEADLINE_HORIZON_ORDER = ["1d", "6h", "1h"]
FRESHNESS_FILTER_SPECS = [
    ("all", None),
    ("<=1h", 1.0),
    ("<=6h", 6.0),
    ("<=24h", 24.0),
]
FRESHNESS_FILTER_ORDER = [label for label, _ in FRESHNESS_FILTER_SPECS]
STALENESS_BUCKET_ORDER = ["<=1h", "1-6h", "6-24h", "1-7d", ">7d"]
LIQUIDITY_ORDER = [
    "Q1 Lowest",
    "Q2 Low-Mid",
    "Q3 Mid",
    "Q4 Mid-High",
    "Q5 Highest",
]


class Facet1Analysis(Analysis):
    """Facet 1 analysis base class with tracked research output publishing."""

    default_formats = ["png", "pdf", "csv"]
    disallowed_formats = {"gif"}

    def normalize_formats(self, formats: list[str] | None) -> list[str]:
        """Return the supported format list for this analysis."""
        if formats is None:
            return list(self.default_formats)
        return [fmt for fmt in formats if fmt not in self.disallowed_formats]

    def save_additional_outputs(
        self,
        output_dir: Path,
        formats: list[str],
    ) -> dict[str, Path]:
        """Hook for analyses that emit extra CSV files besides the main export."""
        return {}

    def save(
        self,
        output_dir: Path | str,
        formats: list[str] | None = None,
        dpi: int = 300,
    ) -> dict[str, Path]:
        """Save outputs and publish tracked Facet 1 copies."""
        normalized_formats = self.normalize_formats(formats)
        saved = super().save(output_dir, normalized_formats, dpi)
        saved.update(self.save_additional_outputs(Path(output_dir), normalized_formats))
        saved.update(publish_facet1_outputs(saved))
        return saved


def load_facet1_dataset(
    dataset_path: Path | str | None,
    default_path: Path,
    builder_hint: str,
) -> pd.DataFrame:
    """Load a parquet-backed Facet 1 dataset with a consistent error message."""
    dataset = Path(dataset_path or default_path)
    if not dataset.exists():
        raise FileNotFoundError(f"Facet 1 dataset not found: {dataset}. Run {builder_hint} first.")

    con = duckdb.connect()
    return con.execute(f"SELECT * FROM '{dataset}'").df()


def load_enriched_dataset(dataset_path: Path | str | None = None) -> pd.DataFrame:
    """Load the enriched market-level Facet 1 dataset."""
    return load_facet1_dataset(
        dataset_path,
        DEFAULT_MARKET_DATASET_PATH,
        "scripts/build_facet1_enriched_market_dataset.py",
    )


def load_enriched_horizon_dataset(dataset_path: Path | str | None = None) -> pd.DataFrame:
    """Load the enriched horizon-level Facet 1 dataset."""
    return load_facet1_dataset(
        dataset_path,
        DEFAULT_HORIZON_DATASET_PATH,
        "scripts/build_facet1_time_to_expiry_enriched_dataset.py",
    )


def publish_facet1_outputs(saved: dict[str, Path]) -> dict[str, Path]:
    """Copy Facet 1 tables/charts into the tracked research folders."""
    published: dict[str, Path] = {}

    for key, path in saved.items():
        if not path.exists():
            continue

        suffix = path.suffix.lower()
        if suffix == ".csv":
            destination_dir = FACET1_TABLE_OUTPUT_DIR
        elif suffix in {".png", ".pdf", ".svg"}:
            destination_dir = FACET1_CHART_OUTPUT_DIR
        else:
            continue

        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / path.name
        shutil.copy2(path, destination)
        published[f"published_{key}"] = destination

    return published


def materialize_kalshi_category_hierarchy(df: pd.DataFrame) -> pd.DataFrame:
    """Add consistent Kalshi category hierarchy columns to a dataframe."""
    df = df.copy()
    df["category_raw"] = df.get("category_raw", pd.Series(index=df.index, dtype="object"))
    df["category_group"] = pd.NA
    df["category_mid"] = pd.NA
    df["category_subcategory"] = pd.NA

    kalshi_mask = df["platform"] == "kalshi"
    if kalshi_mask.any():
        hierarchy = (
            df.loc[kalshi_mask, "category_raw"]
            .fillna("independent")
            .map(get_hierarchy)
        )
        hierarchy_df = pd.DataFrame(
            hierarchy.tolist(),
            columns=["category_group", "category_mid", "category_subcategory"],
            index=df.index[kalshi_mask],
        )
        df.loc[kalshi_mask, ["category_group", "category_mid", "category_subcategory"]] = hierarchy_df

    return df


def assign_horizon_order(df: pd.DataFrame, column: str = "horizon_label") -> pd.DataFrame:
    """Attach the shared categorical order for horizon labels."""
    df = df.copy()
    df[column] = pd.Categorical(df[column], categories=HORIZON_ORDER, ordered=True)
    return df


def assign_freshness_filter_order(df: pd.DataFrame, column: str = "freshness_filter") -> pd.DataFrame:
    """Attach the shared categorical order for freshness filters."""
    df = df.copy()
    df[column] = pd.Categorical(df[column], categories=FRESHNESS_FILTER_ORDER, ordered=True)
    return df


def iter_freshness_subsets(
    df: pd.DataFrame,
    staleness_col: str = "staleness_age_hours",
) -> list[tuple[str, pd.DataFrame]]:
    """Return the shared freshness-filtered slices."""
    subsets: list[tuple[str, pd.DataFrame]] = []
    for label, threshold in FRESHNESS_FILTER_SPECS:
        if threshold is None:
            subsets.append((label, df.copy()))
        else:
            subsets.append((label, df[df[staleness_col] <= threshold].copy()))
    return subsets


def add_retained_share(
    summary_df: pd.DataFrame,
    baseline_group_cols: list[str],
    *,
    filter_col: str = "freshness_filter",
    count_col: str = "market_count",
) -> pd.DataFrame:
    """Add retained-share versus the `all` baseline for a freshness summary table."""
    summary_df = summary_df.copy()
    baseline_df = (
        summary_df[summary_df[filter_col] == "all"][baseline_group_cols + [count_col]]
        .rename(columns={count_col: "baseline_market_count"})
        .drop_duplicates(subset=baseline_group_cols)
    )
    summary_df = summary_df.merge(baseline_df, on=baseline_group_cols, how="left")
    summary_df["retained_share"] = summary_df[count_col] / summary_df["baseline_market_count"]
    return summary_df


def weighted_categorical_regression(
    df: pd.DataFrame,
    *,
    outcome_col: str,
    weight_col: str,
    categorical_cols: list[str],
    baseline_levels: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Fit a weighted least-squares regression with categorical controls only."""
    baseline_levels = baseline_levels or {}
    required_cols = categorical_cols + [outcome_col, weight_col]
    regression_df = df[required_cols].dropna().copy()
    if regression_df.empty:
        return pd.DataFrame(
            columns=[
                "predictor",
                "level",
                "baseline_level",
                "coefficient",
                "std_error",
                "ci_low",
                "ci_high",
                "row_count",
                "weight_sum",
            ]
        )

    x_columns = [np.ones(len(regression_df))]
    coefficient_meta: list[tuple[str, str, str]] = []

    for col in categorical_cols:
        observed_levels = pd.Index(regression_df[col].astype(str)).unique().tolist()
        if not observed_levels:
            continue

        baseline = baseline_levels.get(col, observed_levels[0])
        ordered_levels = [baseline] + [level for level in observed_levels if level != baseline]
        regression_df[col] = pd.Categorical(
            regression_df[col].astype(str),
            categories=ordered_levels,
            ordered=True,
        )

        dummy_df = pd.get_dummies(regression_df[col], drop_first=True, dtype=float)
        for level in ordered_levels[1:]:
            x_columns.append(dummy_df[level].to_numpy())
            coefficient_meta.append((col, level, baseline))

    x = np.column_stack(x_columns)
    y = regression_df[outcome_col].astype(float).to_numpy()
    weights = regression_df[weight_col].astype(float).to_numpy()

    sqrt_weights = np.sqrt(weights)
    x_weighted = x * sqrt_weights[:, None]
    y_weighted = y * sqrt_weights

    beta, _, rank, _ = np.linalg.lstsq(x_weighted, y_weighted, rcond=None)
    residuals = y - (x @ beta)
    dof = max(len(y) - rank, 1)
    sigma2 = float(np.sum(weights * residuals**2) / dof)
    xtwx_inv = np.linalg.pinv(x_weighted.T @ x_weighted)
    std_errors = np.sqrt(np.clip(np.diag(sigma2 * xtwx_inv), 0.0, None))

    rows = []
    for idx, (predictor, level, baseline) in enumerate(coefficient_meta, start=1):
        coefficient = float(beta[idx])
        std_error = float(std_errors[idx])
        rows.append(
            {
                "predictor": predictor,
                "level": level,
                "baseline_level": baseline,
                "coefficient": coefficient,
                "std_error": std_error,
                "ci_low": coefficient - (1.96 * std_error),
                "ci_high": coefficient + (1.96 * std_error),
                "row_count": int(len(regression_df)),
                "weight_sum": float(weights.sum()),
            }
        )

    return pd.DataFrame(rows)


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
