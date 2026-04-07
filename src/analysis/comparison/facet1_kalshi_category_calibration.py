"""Kalshi category-sliced calibration analysis for Phase 1 / Facet 1."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.analysis.comparison.facet1_slice_utils import (
    compute_bucket_calibration,
    compute_slice_summary,
    load_enriched_dataset,
)
from src.analysis.kalshi.util.categories import GROUP_COLORS, get_group
from src.common.analysis import Analysis, AnalysisOutput


class Facet1KalshiCategoryCalibrationAnalysis(Analysis):
    """Compare Kalshi calibration across top-level category groups."""

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet1_kalshi_category_calibration",
            description="Kalshi-only calibration by high-level category group",
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
        df = df[df["platform"] == "kalshi"].copy()
        df["category_group"] = df["category_raw"].fillna("independent").map(get_group)

        bucket_df = compute_bucket_calibration(df, ["category_group"])
        summary_df = compute_slice_summary(bucket_df, ["category_group"])
        summary_df = summary_df.sort_values("market_count", ascending=False).reset_index(drop=True)

        self.bucket_details = bucket_df.sort_values(
            ["category_group", "price_bucket_5c_floor"]
        ).reset_index(drop=True)

        fig = self._create_figure(summary_df)
        return AnalysisOutput(figure=fig, data=summary_df)

    def _create_figure(self, summary_df: pd.DataFrame):
        fig, axes = plt.subplots(1, 3, figsize=(16, 7))
        categories = summary_df["category_group"]
        colors = [GROUP_COLORS.get(category, "#7f7f7f") for category in categories]

        axes[0].barh(categories, summary_df["expected_calibration_error"] * 100, color=colors)
        axes[0].set_title("ECE by Category")
        axes[0].set_xlabel("ECE (%)")
        axes[0].grid(axis="x", alpha=0.3)

        axes[1].barh(categories, summary_df["signed_calibration_gap"] * 100, color=colors)
        axes[1].axvline(0, color="black", linewidth=1, alpha=0.6)
        axes[1].set_title("Signed Gap by Category")
        axes[1].set_xlabel("Signed Gap (%)")
        axes[1].grid(axis="x", alpha=0.3)

        axes[2].barh(categories, summary_df["market_count"], color=colors)
        axes[2].set_title("Markets by Category")
        axes[2].set_xlabel("Market Count")
        axes[2].grid(axis="x", alpha=0.3)

        fig.suptitle("Facet 1 Kalshi Category Slice", fontsize=14)
        fig.tight_layout()
        return fig
