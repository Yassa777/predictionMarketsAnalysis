"""Freshness-controlled horizon calibration analysis for Facet 1."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import PercentFormatter

from src.analysis.comparison.facet1_slice_utils import (
    HORIZON_ORDER,
    Facet1Analysis,
    add_retained_share,
    assign_freshness_filter_order,
    assign_horizon_order,
    compute_bucket_calibration,
    compute_slice_summary,
    iter_freshness_subsets,
    load_enriched_horizon_dataset,
)
from src.common.analysis import AnalysisOutput

CALIBRATION_CURVE_HORIZONS = ["1d", "6h", "1h"]
ECE_CHART_FILTERS = ["all", "<=6h", "<=24h"]
CURVE_FILTER_COLORS = {
    "all": "#111827",
    "<=6h": "#2563eb",
    "<=24h": "#ea580c",
}


class Facet1FreshnessControlledCalibrationAnalysis(Facet1Analysis):
    """Compare raw and freshness-filtered calibration across target horizons."""

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet1_freshness_controlled_calibration",
            description="Calibration by platform, horizon, and freshness filter",
        )
        self.dataset_path = Path(dataset_path) if dataset_path else None
        self.bucket_details: pd.DataFrame | None = None

    def save_additional_outputs(
        self,
        output_dir: Path,
        formats: list[str],
    ) -> dict[str, Path]:
        saved: dict[str, Path] = {}
        if self.bucket_details is not None and "csv" in formats:
            detail_path = output_dir / f"{self.name}_bucket_details.csv"
            self.bucket_details.to_csv(detail_path, index=False)
            saved["bucket_details_csv"] = detail_path
        return saved

    def run(self) -> AnalysisOutput:
        df = load_enriched_horizon_dataset(self.dataset_path)

        summary_frames: list[pd.DataFrame] = []
        bucket_frames: list[pd.DataFrame] = []
        for freshness_filter, subset in iter_freshness_subsets(df):
            if subset.empty:
                continue

            subset["freshness_filter"] = freshness_filter
            group_cols = ["platform", "horizon_label", "horizon_hours", "freshness_filter"]
            bucket_df = compute_bucket_calibration(subset, group_cols)
            summary_df = compute_slice_summary(bucket_df, group_cols)
            staleness_stats = (
                subset.groupby(group_cols, dropna=False)
                .agg(
                    median_staleness_age_hours=("staleness_age_hours", "median"),
                    mean_staleness_age_hours=("staleness_age_hours", "mean"),
                )
                .reset_index()
            )

            bucket_frames.append(bucket_df)
            summary_frames.append(summary_df.merge(staleness_stats, on=group_cols, how="left"))

        summary_df = pd.concat(summary_frames, ignore_index=True)
        summary_df = add_retained_share(
            summary_df,
            ["platform", "horizon_label", "horizon_hours"],
        )
        summary_df = assign_horizon_order(summary_df)
        summary_df = assign_freshness_filter_order(summary_df)
        summary_df = summary_df.sort_values(
            ["platform", "horizon_label", "freshness_filter"]
        ).reset_index(drop=True)

        bucket_df = pd.concat(bucket_frames, ignore_index=True)
        bucket_df = assign_horizon_order(bucket_df)
        bucket_df = assign_freshness_filter_order(bucket_df)
        self.bucket_details = bucket_df.sort_values(
            ["platform", "horizon_label", "freshness_filter", "price_bucket_5c_floor"]
        ).reset_index(drop=True)

        fig = self._create_figure(summary_df, self.bucket_details)
        return AnalysisOutput(figure=fig, data=summary_df)

    def _create_figure(self, summary_df: pd.DataFrame, bucket_df: pd.DataFrame) -> plt.Figure:
        fig = plt.figure(figsize=(18, 14))
        axes = fig.subplot_mosaic(
            [
                ["ece_kalshi", "ece_polymarket", "legend"],
                ["kalshi_1d", "kalshi_6h", "kalshi_1h"],
                ["polymarket_1d", "polymarket_6h", "polymarket_1h"],
            ],
            empty_sentinel=".",
        )

        x = list(range(len(HORIZON_ORDER)))
        for platform, axis_name in [("kalshi", "ece_kalshi"), ("polymarket", "ece_polymarket")]:
            ax = axes[axis_name]
            platform_df = summary_df[summary_df["platform"] == platform]

            for freshness_filter in ECE_CHART_FILTERS:
                filter_df = (
                    platform_df[platform_df["freshness_filter"] == freshness_filter]
                    .set_index("horizon_label")
                    .reindex(HORIZON_ORDER)
                )
                ax.plot(
                    x,
                    filter_df["expected_calibration_error"] * 100,
                    marker="o",
                    linewidth=2,
                    label=freshness_filter,
                    color=CURVE_FILTER_COLORS[freshness_filter],
                )

            ax.set_title(f"{platform.title()} ECE by Horizon")
            ax.set_xticks(x, HORIZON_ORDER)
            ax.yaxis.set_major_formatter(PercentFormatter())
            ax.grid(alpha=0.3)
            ax.set_xlabel("Target Horizon")

        axes["ece_kalshi"].set_ylabel("ECE (%)")

        legend_ax = axes["legend"]
        legend_ax.axis("off")
        handles, labels = axes["ece_polymarket"].get_legend_handles_labels()
        legend_ax.legend(handles, labels, loc="center", frameon=False, title="Freshness Filter")
        legend_ax.text(
            0.0,
            0.15,
            "Calibration curves below compare all rows against the <=6h subset at the headline horizons.",
            fontsize=10,
            ha="left",
            va="bottom",
        )

        for platform in ["kalshi", "polymarket"]:
            for horizon in CALIBRATION_CURVE_HORIZONS:
                ax = axes[f"{platform}_{horizon}"]
                curve_df = bucket_df[
                    (bucket_df["platform"] == platform)
                    & (bucket_df["horizon_label"] == horizon)
                    & (bucket_df["freshness_filter"].isin(["all", "<=6h"]))
                ].copy()

                ax.plot([0, 100], [0, 100], linestyle="--", color="#9CA3AF", linewidth=1.1)
                for freshness_filter in ["all", "<=6h"]:
                    filter_df = curve_df[curve_df["freshness_filter"] == freshness_filter].sort_values(
                        "avg_implied_probability"
                    )
                    if filter_df.empty:
                        continue

                    ax.plot(
                        filter_df["avg_implied_probability"] * 100,
                        filter_df["empirical_win_rate"] * 100,
                        marker="o",
                        markersize=3.5,
                        linewidth=1.8,
                        color=CURVE_FILTER_COLORS[freshness_filter],
                        label=freshness_filter,
                    )

                ax.set_title(f"{platform.title()} {horizon}")
                ax.set_xlim(0, 100)
                ax.set_ylim(0, 100)
                ax.set_xticks(range(0, 101, 20))
                ax.set_yticks(range(0, 101, 20))
                ax.grid(alpha=0.25)
                ax.set_xlabel("Average Implied Probability (%)")

        axes["kalshi_1d"].set_ylabel("Empirical Win Rate (%)")
        axes["polymarket_1d"].set_ylabel("Empirical Win Rate (%)")

        fig.suptitle("Facet 1 Freshness-Controlled Horizon Calibration", y=0.995)
        return fig
