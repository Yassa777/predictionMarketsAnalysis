"""Liquidity-sliced calibration analysis for Phase 1 / Facet 1."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.analysis.comparison.facet1_slice_utils import (
    PLATFORM_COLORS,
    compute_bucket_calibration,
    compute_slice_summary,
    load_enriched_dataset,
)
from src.common.analysis import Analysis, AnalysisOutput


LIQUIDITY_ORDER = [
    "Q1 Lowest",
    "Q2 Low-Mid",
    "Q3 Mid",
    "Q4 Mid-High",
    "Q5 Highest",
]


class Facet1LiquidityCalibrationAnalysis(Analysis):
    """Compare calibration across within-platform liquidity quintiles."""

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet1_liquidity_calibration",
            description="Calibration by platform-relative liquidity quintile",
        )
        self.dataset_path = Path(dataset_path) if dataset_path else None
        self.bucket_details: pd.DataFrame | None = None

    def save(
        self,
        output_dir: Path | str,
        formats: list[str] | None = None,
        dpi: int = 300,
    ) -> dict[str, Path]:
        if formats is None:
            formats = ["png", "pdf", "csv"]
        else:
            formats = [fmt for fmt in formats if fmt != "gif"]

        saved = super().save(output_dir, formats, dpi)
        if self.bucket_details is not None and "csv" in formats:
            output_dir = Path(output_dir)
            detail_path = output_dir / f"{self.name}_bucket_details.csv"
            self.bucket_details.to_csv(detail_path, index=False)
            saved["bucket_details_csv"] = detail_path
        return saved

    def run(self) -> AnalysisOutput:
        df = load_enriched_dataset(self.dataset_path)
        df = df[df["liquidity_quintile_label"].notna()].copy()

        bucket_df = compute_bucket_calibration(
            df,
            ["platform", "liquidity_quintile", "liquidity_quintile_label"],
        )
        summary_df = compute_slice_summary(
            bucket_df,
            ["platform", "liquidity_quintile", "liquidity_quintile_label"],
        )

        liquidity_stats = (
            df.groupby(["platform", "liquidity_quintile", "liquidity_quintile_label"], dropna=False)
            .agg(
                primary_liquidity_metric_name=("primary_liquidity_metric_name", "first"),
                median_primary_liquidity=("primary_liquidity_metric", "median"),
                min_primary_liquidity=("primary_liquidity_metric", "min"),
                max_primary_liquidity=("primary_liquidity_metric", "max"),
            )
            .reset_index()
        )
        summary_df = summary_df.merge(
            liquidity_stats,
            on=["platform", "liquidity_quintile", "liquidity_quintile_label"],
            how="left",
        )
        summary_df["liquidity_quintile_label"] = pd.Categorical(
            summary_df["liquidity_quintile_label"],
            categories=LIQUIDITY_ORDER,
            ordered=True,
        )
        summary_df = summary_df.sort_values(
            ["platform", "liquidity_quintile_label"]
        ).reset_index(drop=True)

        self.bucket_details = bucket_df.sort_values(
            ["platform", "liquidity_quintile", "price_bucket_5c_floor"]
        ).reset_index(drop=True)

        fig = self._create_figure(summary_df)
        return AnalysisOutput(figure=fig, data=summary_df)

    def _create_figure(self, summary_df: pd.DataFrame):
        fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)
        x = range(len(LIQUIDITY_ORDER))

        for platform, color in PLATFORM_COLORS.items():
            platform_df = (
                summary_df[summary_df["platform"] == platform]
                .set_index("liquidity_quintile_label")
                .reindex(LIQUIDITY_ORDER)
            )
            if platform_df.empty:
                continue

            axes[0].plot(
                x,
                platform_df["expected_calibration_error"] * 100,
                marker="o",
                linewidth=2,
                label=platform.title(),
                color=color,
            )
            axes[1].plot(
                x,
                platform_df["signed_calibration_gap"] * 100,
                marker="o",
                linewidth=2,
                label=platform.title(),
                color=color,
            )
            axes[2].plot(
                x,
                platform_df["market_count"],
                marker="o",
                linewidth=2,
                label=platform.title(),
                color=color,
            )

        axes[0].set_title("Facet 1 Liquidity Slice")
        axes[0].set_ylabel("ECE (%)")
        axes[0].grid(alpha=0.3)
        axes[0].legend(frameon=False)

        axes[1].axhline(0, color="black", linewidth=1, alpha=0.6)
        axes[1].set_ylabel("Signed Gap (%)")
        axes[1].grid(alpha=0.3)

        axes[2].set_ylabel("Markets")
        axes[2].set_xlabel("Liquidity Quintile")
        axes[2].grid(alpha=0.3)
        axes[2].set_xticks(list(x), LIQUIDITY_ORDER)

        fig.tight_layout()
        return fig
