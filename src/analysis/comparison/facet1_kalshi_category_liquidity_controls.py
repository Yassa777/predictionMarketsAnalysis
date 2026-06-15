"""Kalshi category calibration controlled for liquidity."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import PercentFormatter

from src.analysis.comparison.facet1_slice_utils import (
    LIQUIDITY_ORDER,
    Facet1Analysis,
    compute_bucket_calibration,
    compute_slice_summary,
    load_enriched_dataset,
    materialize_kalshi_category_hierarchy,
    weighted_categorical_regression,
)
from src.analysis.kalshi.util.categories import GROUP_COLORS
from src.common.analysis import AnalysisOutput

DRILLDOWN_PARENT_THRESHOLD = 10_000
DRILLDOWN_SLICE_THRESHOLD = 1_000
TOP_LINE_CHART_GROUPS = 6


class Facet1KalshiCategoryLiquidityControlsAnalysis(Facet1Analysis):
    """Estimate Kalshi category calibration after conditioning on liquidity quintiles."""

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet1_kalshi_category_liquidity_controls",
            description="Kalshi category calibration controlled for liquidity quintile",
        )
        self.dataset_path = Path(dataset_path) if dataset_path else None
        self.bucket_details: pd.DataFrame | None = None
        self.regression_coefficients: pd.DataFrame | None = None
        self.category_mid_drilldown: pd.DataFrame | None = None

    def save_additional_outputs(
        self,
        output_dir: Path,
        formats: list[str],
    ) -> dict[str, Path]:
        saved: dict[str, Path] = {}
        if "csv" not in formats:
            return saved

        if self.bucket_details is not None:
            path = output_dir / f"{self.name}_bucket_details.csv"
            self.bucket_details.to_csv(path, index=False)
            saved["bucket_details_csv"] = path

        if self.regression_coefficients is not None:
            path = output_dir / f"{self.name}_regression_coefficients.csv"
            self.regression_coefficients.to_csv(path, index=False)
            saved["regression_coefficients_csv"] = path

        if self.category_mid_drilldown is not None and not self.category_mid_drilldown.empty:
            path = output_dir / f"{self.name}_category_mid_drilldown.csv"
            self.category_mid_drilldown.to_csv(path, index=False)
            saved["category_mid_drilldown_csv"] = path

        return saved

    def run(self) -> AnalysisOutput:
        df = load_enriched_dataset(self.dataset_path)
        df = materialize_kalshi_category_hierarchy(df)
        df = df[(df["platform"] == "kalshi") & df["liquidity_quintile_label"].notna()].copy()

        category_order = (
            df.groupby("category_group", dropna=False)
            .size()
            .sort_values(ascending=False)
            .index.tolist()
        )
        baseline_category = category_order[0]

        group_cols = ["category_group", "liquidity_quintile", "liquidity_quintile_label"]
        bucket_df = compute_bucket_calibration(df, group_cols)
        summary_df = compute_slice_summary(bucket_df, group_cols)
        liquidity_stats = (
            df.groupby(group_cols, dropna=False)
            .agg(
                median_volume_contracts=("primary_liquidity_metric", "median"),
                median_open_interest_contracts=("secondary_liquidity_metric", "median"),
            )
            .reset_index()
        )
        summary_df = summary_df.merge(liquidity_stats, on=group_cols, how="left")
        summary_df["category_group"] = pd.Categorical(
            summary_df["category_group"],
            categories=category_order,
            ordered=True,
        )
        summary_df["liquidity_quintile_label"] = pd.Categorical(
            summary_df["liquidity_quintile_label"],
            categories=LIQUIDITY_ORDER,
            ordered=True,
        )
        summary_df = summary_df.sort_values(
            ["category_group", "liquidity_quintile_label"]
        ).reset_index(drop=True)

        bucket_df["category_group"] = pd.Categorical(
            bucket_df["category_group"],
            categories=category_order,
            ordered=True,
        )
        bucket_df["liquidity_quintile_label"] = pd.Categorical(
            bucket_df["liquidity_quintile_label"],
            categories=LIQUIDITY_ORDER,
            ordered=True,
        )
        self.bucket_details = bucket_df.sort_values(
            ["category_group", "liquidity_quintile_label", "price_bucket_5c_floor"]
        ).reset_index(drop=True)

        regression_df = weighted_categorical_regression(
            self.bucket_details.copy(),
            outcome_col="calibration_gap",
            weight_col="market_count",
            categorical_cols=["category_group", "liquidity_quintile_label", "price_bucket_5c_label"],
            baseline_levels={
                "category_group": baseline_category,
                "liquidity_quintile_label": LIQUIDITY_ORDER[0],
                "price_bucket_5c_label": self.bucket_details["price_bucket_5c_label"].astype(str).min(),
            },
        )
        regression_df["control_family"] = "liquidity"
        self.regression_coefficients = regression_df

        self.category_mid_drilldown = self._build_category_mid_drilldown(df)

        fig = self._create_figure(summary_df, category_order, baseline_category)
        summary_df["category_group"] = summary_df["category_group"].astype(str)
        summary_df["liquidity_quintile_label"] = summary_df["liquidity_quintile_label"].astype(str)
        return AnalysisOutput(figure=fig, data=summary_df)

    def _build_category_mid_drilldown(self, df: pd.DataFrame) -> pd.DataFrame:
        parent_counts = df.groupby("category_group")["market_id"].nunique()
        qualifying_groups = parent_counts[parent_counts >= DRILLDOWN_PARENT_THRESHOLD].index.tolist()
        if not qualifying_groups:
            return pd.DataFrame()

        drilldown_df = df[df["category_group"].isin(qualifying_groups)].copy()
        group_cols = ["category_group", "category_mid", "liquidity_quintile", "liquidity_quintile_label"]
        bucket_df = compute_bucket_calibration(drilldown_df, group_cols)
        summary_df = compute_slice_summary(bucket_df, group_cols)
        stats_df = (
            drilldown_df.groupby(group_cols, dropna=False)
            .agg(
                median_volume_contracts=("primary_liquidity_metric", "median"),
                median_open_interest_contracts=("secondary_liquidity_metric", "median"),
            )
            .reset_index()
        )
        summary_df = summary_df.merge(stats_df, on=group_cols, how="left")
        summary_df = summary_df[summary_df["market_count"] >= DRILLDOWN_SLICE_THRESHOLD].copy()
        return summary_df.sort_values(["category_group", "category_mid", "liquidity_quintile"]).reset_index(drop=True)

    def _create_figure(
        self,
        summary_df: pd.DataFrame,
        category_order: list[str],
        baseline_category: str,
    ) -> plt.Figure:
        fig, axes = plt.subplots(1, 3, figsize=(22, 8), gridspec_kw={"width_ratios": [1.5, 1.2, 1.0]})

        heatmap_df = (
            summary_df.pivot(index="category_group", columns="liquidity_quintile_label", values="expected_calibration_error")
            .reindex(index=category_order, columns=LIQUIDITY_ORDER)
        )
        image = axes[0].imshow(heatmap_df.to_numpy() * 100, aspect="auto", cmap="YlOrRd")
        axes[0].set_title("ECE Heatmap by Category and Liquidity Quintile")
        axes[0].set_xticks(range(len(LIQUIDITY_ORDER)), LIQUIDITY_ORDER, rotation=30, ha="right")
        axes[0].set_yticks(range(len(category_order)), category_order)
        for row_idx, category in enumerate(category_order):
            for col_idx, quintile in enumerate(LIQUIDITY_ORDER):
                value = heatmap_df.loc[category, quintile]
                label = "" if pd.isna(value) else f"{value * 100:.2f}"
                axes[0].text(
                    col_idx,
                    row_idx,
                    label,
                    ha="center",
                    va="center",
                    fontsize=8,
                    color="#111827",
                )
        colorbar = fig.colorbar(image, ax=axes[0], fraction=0.046, pad=0.02)
        colorbar.set_label("ECE (%)")

        x = list(range(len(LIQUIDITY_ORDER)))
        top_groups = category_order[:TOP_LINE_CHART_GROUPS]
        for category in top_groups:
            category_df = (
                summary_df[summary_df["category_group"].astype(str) == category]
                .set_index("liquidity_quintile_label")
                .reindex(LIQUIDITY_ORDER)
            )
            axes[1].plot(
                x,
                category_df["expected_calibration_error"] * 100,
                marker="o",
                linewidth=2,
                label=category,
                color=GROUP_COLORS.get(category, "#6b7280"),
            )

        axes[1].set_title("Largest Groups Across Liquidity Quintiles")
        axes[1].set_xticks(x, LIQUIDITY_ORDER, rotation=20, ha="right")
        axes[1].yaxis.set_major_formatter(PercentFormatter())
        axes[1].grid(alpha=0.3)
        axes[1].set_ylabel("ECE (%)")
        axes[1].legend(frameon=False, fontsize=9)

        coefficient_df = self.regression_coefficients[self.regression_coefficients["predictor"] == "category_group"].copy()
        baseline_row = pd.DataFrame(
            [
                {
                    "predictor": "category_group",
                    "level": baseline_category,
                    "baseline_level": baseline_category,
                    "coefficient": 0.0,
                    "std_error": 0.0,
                    "ci_low": 0.0,
                    "ci_high": 0.0,
                }
            ]
        )
        coefficient_df = pd.concat([coefficient_df, baseline_row], ignore_index=True)
        coefficient_df = coefficient_df.sort_values("coefficient").reset_index(drop=True)

        axes[2].axvline(0, color="#111827", linewidth=1, alpha=0.6)
        axes[2].errorbar(
            coefficient_df["coefficient"] * 100,
            coefficient_df["level"],
            xerr=[
                (coefficient_df["coefficient"] - coefficient_df["ci_low"]) * 100,
                (coefficient_df["ci_high"] - coefficient_df["coefficient"]) * 100,
            ],
            fmt="o",
            color="#111827",
            ecolor="#6b7280",
            capsize=3,
        )
        axes[2].set_title("Category Effects After Liquidity Controls")
        axes[2].set_xlabel("Coefficient on Calibration Gap (pp)")
        axes[2].grid(axis="x", alpha=0.3)

        fig.suptitle("Facet 1 Kalshi Category Controls: Liquidity", y=0.98)
        fig.tight_layout()
        return fig
