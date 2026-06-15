"""Simple temporal-holdout modeling decision tables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analysis.comparison.facet1_slice_utils import (
    load_enriched_dataset,
    load_enriched_horizon_dataset,
)
from src.analysis.comparison.research_csv_utils import ResearchCsvAnalysis, brier_score, log_loss
from src.common.analysis import AnalysisOutput

SMOOTHING_WEIGHT = 50.0


class Facet2ModelingDecisionTableAnalysis(ResearchCsvAnalysis):
    """Compare market-price baseline with simple calibrated feature tables."""

    research_subdir = "modeling"

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="modeling_feature_benchmark",
            description="Temporal holdout benchmark for simple calibration feature sets",
        )
        self.dataset_path = Path(dataset_path) if dataset_path else None

    def run(self) -> AnalysisOutput:
        market_df = load_enriched_dataset(self.dataset_path)
        market_df["dataset"] = "last_trade"
        market_df["category_group"] = market_df["category_group"].fillna("Other")
        market_df["prediction_market_price"] = market_df["reference_price_cents"] / 100.0

        horizon_df = load_enriched_horizon_dataset()
        horizon_df["dataset"] = "horizon_snapshot"
        horizon_df["category_group"] = horizon_df["category_group"].fillna("Other")
        horizon_df["prediction_market_price"] = horizon_df["reference_price_cents"] / 100.0

        benchmark_rows = []
        slice_frames = []

        benchmark, predictions = evaluate_dataset(
            market_df,
            dataset_name="last_trade",
            variants={
                "market_price": None,
                "platform_price_bucket": ["platform", "price_bucket_5c_floor"],
                "platform_liquidity_price_bucket": ["platform", "liquidity_quintile", "price_bucket_5c_floor"],
                "platform_category_price_bucket": ["platform", "category_group", "price_bucket_5c_floor"],
            },
        )
        benchmark_rows.extend(benchmark)
        slice_frames.extend(
            [
                build_slice_metrics(predictions, "platform_category_price_bucket", ["platform"]),
                build_slice_metrics(predictions, "platform_category_price_bucket", ["platform", "category_group"]),
                build_slice_metrics(predictions, "platform_liquidity_price_bucket", ["platform", "liquidity_quintile"]),
            ]
        )

        benchmark, predictions = evaluate_dataset(
            horizon_df,
            dataset_name="horizon_snapshot",
            variants={
                "market_price": None,
                "platform_horizon_price_bucket": ["platform", "horizon_label", "price_bucket_5c_floor"],
                "platform_freshness_price_bucket": ["platform", "staleness_bucket", "price_bucket_5c_floor"],
                "platform_horizon_liquidity_price_bucket": [
                    "platform",
                    "horizon_label",
                    "liquidity_quintile",
                    "price_bucket_5c_floor",
                ],
            },
        )
        benchmark_rows.extend(benchmark)
        slice_frames.extend(
            [
                build_slice_metrics(predictions, "platform_horizon_liquidity_price_bucket", ["platform", "horizon_label"]),
                build_slice_metrics(predictions, "platform_freshness_price_bucket", ["platform", "staleness_bucket"]),
                build_slice_metrics(
                    predictions,
                    "platform_horizon_liquidity_price_bucket",
                    ["platform", "liquidity_quintile"],
                ),
            ]
        )

        benchmark_df = pd.DataFrame(benchmark_rows).sort_values(["dataset", "brier_score"])
        self.extra_tables = {
            "modeling_feature_slices": pd.concat(slice_frames, ignore_index=True),
        }
        return AnalysisOutput(data=benchmark_df)


def temporal_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    df = df.dropna(subset=["close_ts", "reference_won", "reference_price_cents"]).copy()
    df["close_ts"] = pd.to_datetime(df["close_ts"], utc=True)
    cutoff = df["close_ts"].quantile(0.8)
    train = df[df["close_ts"] <= cutoff].copy()
    test = df[df["close_ts"] > cutoff].copy()
    return train, test, cutoff


def evaluate_dataset(
    df: pd.DataFrame,
    *,
    dataset_name: str,
    variants: dict[str, list[str] | None],
) -> tuple[list[dict], pd.DataFrame]:
    train, test, cutoff = temporal_split(df)
    prior = float(train["reference_won"].mean())

    benchmark_rows = []
    predictions = test[
        [
            "platform",
            "market_id",
            "close_ts",
            "reference_won",
            "prediction_market_price",
            "category_group",
        ]
        + [col for col in ["horizon_label", "liquidity_quintile", "staleness_bucket"] if col in test.columns]
    ].copy()

    for variant_name, group_cols in variants.items():
        if group_cols is None:
            pred = test["prediction_market_price"].astype(float)
            train_count = len(train)
            group_count = 0
        else:
            group_cols = [col for col in group_cols if col in train.columns]
            rate_table = (
                train.groupby(group_cols, dropna=False)
                .agg(group_wins=("reference_won", "sum"), group_count=("reference_won", "size"))
                .reset_index()
            )
            rate_table["empirical_prediction"] = (
                rate_table["group_wins"] + SMOOTHING_WEIGHT * prior
            ) / (rate_table["group_count"] + SMOOTHING_WEIGHT)
            scored = test.merge(rate_table[group_cols + ["empirical_prediction"]], on=group_cols, how="left")
            pred = scored["empirical_prediction"].fillna(scored["prediction_market_price"]).astype(float)
            train_count = int(rate_table["group_count"].sum())
            group_count = int(len(rate_table))

        y = test["reference_won"].astype(float)
        baseline_brier = brier_score(y, test["prediction_market_price"])
        baseline_log_loss = log_loss(y, test["prediction_market_price"])
        variant_brier = brier_score(y, pred)
        variant_log_loss = log_loss(y, pred)
        predictions[f"pred_{variant_name}"] = pred.to_numpy()

        benchmark_rows.append(
            {
                "dataset": dataset_name,
                "variant": variant_name,
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
                "train_group_rows": train_count,
                "group_count": group_count,
                "temporal_cutoff": cutoff,
                "brier_score": variant_brier,
                "log_loss": variant_log_loss,
                "baseline_brier_score": baseline_brier,
                "baseline_log_loss": baseline_log_loss,
                "brier_delta_vs_market_price": variant_brier - baseline_brier,
                "log_loss_delta_vs_market_price": variant_log_loss - baseline_log_loss,
            }
        )

    return benchmark_rows, predictions


def build_slice_metrics(predictions: pd.DataFrame, variant_name: str, group_cols: list[str]) -> pd.DataFrame:
    pred_col = f"pred_{variant_name}"
    rows = []
    for keys, group in predictions.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        if len(group) < 100:
            continue
        y = group["reference_won"].astype(float)
        baseline = group["prediction_market_price"].astype(float)
        pred = group[pred_col].astype(float)
        row = dict(zip(group_cols, keys))
        row.update(
            {
                "variant": variant_name,
                "test_rows": int(len(group)),
                "brier_score": brier_score(y, pred),
                "baseline_brier_score": brier_score(y, baseline),
                "brier_delta_vs_market_price": brier_score(y, pred) - brier_score(y, baseline),
                "log_loss": log_loss(y, pred),
                "baseline_log_loss": log_loss(y, baseline),
                "log_loss_delta_vs_market_price": log_loss(y, pred) - log_loss(y, baseline),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["variant", "brier_delta_vs_market_price"]).reset_index(drop=True)
