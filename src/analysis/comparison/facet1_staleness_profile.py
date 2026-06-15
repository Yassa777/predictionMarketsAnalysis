"""Staleness profile analysis for the enriched Facet 1 horizon dataset."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import PercentFormatter

from src.analysis.comparison.facet1_slice_utils import (
    HORIZON_ORDER,
    Facet1Analysis,
    assign_horizon_order,
    load_enriched_horizon_dataset,
)
from src.common.analysis import AnalysisOutput


class Facet1StalenessProfileAnalysis(Facet1Analysis):
    """Summarize freshness and staleness by platform and target horizon."""

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet1_staleness_profile",
            description="Staleness profile of the enriched Facet 1 horizon dataset",
        )
        self.dataset_path = Path(dataset_path) if dataset_path else None

    def run(self) -> AnalysisOutput:
        df = load_enriched_horizon_dataset(self.dataset_path)

        summary_df = (
            df.groupby(["platform", "horizon_label", "horizon_hours"], dropna=False)
            .agg(
                market_count=("reference_won", "size"),
                fresh_1h_market_count=("is_fresh_1h", "sum"),
                fresh_6h_market_count=("is_fresh_6h", "sum"),
                fresh_24h_market_count=("is_fresh_24h", "sum"),
                median_staleness_age_hours=("staleness_age_hours", "median"),
                mean_staleness_age_hours=("staleness_age_hours", "mean"),
                p90_staleness_age_hours=("staleness_age_hours", lambda s: s.quantile(0.90)),
                median_freshness_ratio=("freshness_ratio", "median"),
            )
            .reset_index()
        )
        summary_df["fresh_1h_share"] = summary_df["fresh_1h_market_count"] / summary_df["market_count"]
        summary_df["fresh_6h_share"] = summary_df["fresh_6h_market_count"] / summary_df["market_count"]
        summary_df["fresh_24h_share"] = summary_df["fresh_24h_market_count"] / summary_df["market_count"]
        summary_df = assign_horizon_order(summary_df)
        summary_df = summary_df.sort_values(["platform", "horizon_label"]).reset_index(drop=True)

        fig = self._create_figure(summary_df)
        return AnalysisOutput(figure=fig, data=summary_df)

    def _create_figure(self, summary_df: pd.DataFrame) -> plt.Figure:
        fig = plt.figure(figsize=(16, 10))
        grid = fig.add_gridspec(2, 2, height_ratios=[1.0, 1.15], hspace=0.35, wspace=0.2)

        heatmap_ax = fig.add_subplot(grid[0, :])
        retained_axes = {
            "kalshi": fig.add_subplot(grid[1, 0]),
            "polymarket": fig.add_subplot(grid[1, 1]),
        }

        heatmap_df = (
            summary_df.pivot(index="platform", columns="horizon_label", values="median_staleness_age_hours")
            .reindex(index=["kalshi", "polymarket"], columns=HORIZON_ORDER)
        )
        image = heatmap_ax.imshow(heatmap_df.to_numpy(), aspect="auto", cmap="YlOrRd")
        heatmap_ax.set_title("Median Staleness Age by Platform and Horizon")
        heatmap_ax.set_xticks(range(len(HORIZON_ORDER)), HORIZON_ORDER)
        heatmap_ax.set_yticks(range(len(heatmap_df.index)), [platform.title() for platform in heatmap_df.index])

        for row_idx, platform in enumerate(heatmap_df.index):
            for col_idx, horizon in enumerate(heatmap_df.columns):
                value = heatmap_df.loc[platform, horizon]
                heatmap_ax.text(
                    col_idx,
                    row_idx,
                    f"{value:.1f}h",
                    ha="center",
                    va="center",
                    color="#111827",
                    fontsize=10,
                )

        colorbar = fig.colorbar(image, ax=heatmap_ax, fraction=0.025, pad=0.02)
        colorbar.set_label("Median Staleness Age (hours)")

        freshness_lines = [
            ("fresh_1h_share", "<=1h", "#16a34a"),
            ("fresh_6h_share", "<=6h", "#2563eb"),
            ("fresh_24h_share", "<=24h", "#ea580c"),
        ]

        x = list(range(len(HORIZON_ORDER)))
        for platform, ax in retained_axes.items():
            platform_df = (
                summary_df[summary_df["platform"] == platform]
                .set_index("horizon_label")
                .reindex(HORIZON_ORDER)
            )
            for column, label, color in freshness_lines:
                ax.plot(
                    x,
                    platform_df[column],
                    marker="o",
                    linewidth=2,
                    label=label,
                    color=color,
                )

            ax.set_title(f"{platform.title()} Retained Share")
            ax.set_xticks(x, HORIZON_ORDER)
            ax.set_ylim(0, 1.05)
            ax.yaxis.set_major_formatter(PercentFormatter(1.0))
            ax.grid(alpha=0.3)
            ax.set_xlabel("Target Horizon")

        retained_axes["kalshi"].set_ylabel("Retained Share")
        retained_axes["polymarket"].legend(frameon=False, title="Freshness Threshold")

        fig.suptitle("Facet 1 Staleness Profile", y=0.98)
        return fig
