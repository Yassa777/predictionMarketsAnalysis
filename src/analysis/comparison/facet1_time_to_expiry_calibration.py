"""Time-to-expiry calibration analysis for Phase 1 / Facet 1."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.analysis.comparison.facet1_slice_utils import (
    PLATFORM_COLORS,
    compute_bucket_calibration,
    compute_slice_summary,
)
from src.common.analysis import Analysis, AnalysisOutput


HORIZON_ORDER = ["30d", "7d", "3d", "1d", "6h", "1h"]
HORIZON_COLORS = {
    "30d": "#0f766e",
    "7d": "#0891b2",
    "3d": "#2563eb",
    "1d": "#7c3aed",
    "6h": "#ea580c",
    "1h": "#b91c1c",
}


class Facet1TimeToExpiryCalibrationAnalysis(Analysis):
    """Compare calibration curves across fixed horizons before close."""

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet1_time_to_expiry_calibration",
            description="Calibration curves by fixed time-to-expiry horizon",
        )
        base_dir = Path(__file__).resolve().parents[3]
        self.dataset_path = Path(
            dataset_path or base_dir / "data" / "derived" / "facet1_time_to_expiry_dataset.parquet"
        )
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
        if not self.dataset_path.exists():
            raise FileNotFoundError(
                f"Time-to-expiry dataset not found: {self.dataset_path}. "
                "Run scripts/build_facet1_time_to_expiry_dataset.py first."
            )

        df = pd.read_parquet(self.dataset_path)
        bucket_df = compute_bucket_calibration(df, ["platform", "horizon_label", "horizon_hours"])
        summary_df = compute_slice_summary(bucket_df, ["platform", "horizon_label", "horizon_hours"])

        horizon_stats = (
            df.groupby(["platform", "horizon_label", "horizon_hours"], dropna=False)
            .agg(
                median_actual_hours_before_close=("actual_hours_before_close", "median"),
                mean_actual_hours_before_close=("actual_hours_before_close", "mean"),
                median_hours_before_close_gap=("hours_before_close_gap", "median"),
                mean_hours_before_close_gap=("hours_before_close_gap", "mean"),
            )
            .reset_index()
        )
        summary_df = summary_df.merge(
            horizon_stats,
            on=["platform", "horizon_label", "horizon_hours"],
            how="left",
        )
        summary_df["horizon_label"] = pd.Categorical(
            summary_df["horizon_label"],
            categories=HORIZON_ORDER,
            ordered=True,
        )
        summary_df = summary_df.sort_values(["platform", "horizon_label"]).reset_index(drop=True)

        bucket_df["horizon_label"] = pd.Categorical(
            bucket_df["horizon_label"],
            categories=HORIZON_ORDER,
            ordered=True,
        )
        self.bucket_details = bucket_df.sort_values(
            ["platform", "horizon_label", "price_bucket_5c_floor"]
        ).reset_index(drop=True)

        fig = self._create_figure(self.bucket_details, summary_df)
        return AnalysisOutput(figure=fig, data=summary_df)

    def _create_figure(self, bucket_df: pd.DataFrame, summary_df: pd.DataFrame) -> plt.Figure:
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))
        curve_axes = {
            "kalshi": axes[0, 0],
            "polymarket": axes[0, 1],
        }

        for platform, ax in curve_axes.items():
            platform_buckets = bucket_df[bucket_df["platform"] == platform]
            ax.plot([0, 100], [0, 100], linestyle="--", color="#9CA3AF", linewidth=1.2)

            for horizon in HORIZON_ORDER:
                horizon_df = platform_buckets[platform_buckets["horizon_label"] == horizon].copy()
                if horizon_df.empty:
                    continue

                horizon_df = horizon_df.sort_values("avg_implied_probability")
                ax.plot(
                    horizon_df["avg_implied_probability"] * 100,
                    horizon_df["empirical_win_rate"] * 100,
                    marker="o",
                    markersize=3.5,
                    linewidth=1.8,
                    label=horizon,
                    color=HORIZON_COLORS[horizon],
                )

            ax.set_title(platform.title())
            ax.set_xlim(0, 100)
            ax.set_ylim(0, 100)
            ax.set_xticks(range(0, 101, 10))
            ax.set_yticks(range(0, 101, 10))
            ax.grid(True, alpha=0.25)
            ax.set_xlabel("Average Implied Probability (%)")

        axes[0, 0].set_ylabel("Empirical Win Rate (%)")
        axes[0, 1].legend(title="Target Horizon", frameon=False, loc="lower right")

        x = list(range(len(HORIZON_ORDER)))
        for platform, color in PLATFORM_COLORS.items():
            platform_summary = (
                summary_df[summary_df["platform"] == platform]
                .set_index("horizon_label")
                .reindex(HORIZON_ORDER)
            )
            if platform_summary.empty:
                continue

            axes[1, 0].plot(
                x,
                platform_summary["expected_calibration_error"] * 100,
                marker="o",
                linewidth=2,
                label=platform.title(),
                color=color,
            )
            axes[1, 1].plot(
                x,
                platform_summary["market_count"],
                marker="o",
                linewidth=2,
                label=platform.title(),
                color=color,
            )

        axes[1, 0].set_title("ECE by Horizon")
        axes[1, 0].set_ylabel("ECE (%)")
        axes[1, 0].set_xticks(x, HORIZON_ORDER)
        axes[1, 0].grid(alpha=0.3)
        axes[1, 0].legend(frameon=False)

        axes[1, 1].set_title("Markets by Horizon")
        axes[1, 1].set_ylabel("Market Count")
        axes[1, 1].set_xticks(x, HORIZON_ORDER)
        axes[1, 1].grid(alpha=0.3)

        fig.suptitle("Facet 1 Time-to-Expiry Calibration", y=1.02)
        fig.tight_layout()
        return fig
