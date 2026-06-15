"""Kalshi category calibration controlled for freshness and horizon."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import PercentFormatter

from src.analysis.comparison.facet1_slice_utils import (
    FRESHNESS_FILTER_ORDER,
    HEADLINE_HORIZON_ORDER,
    HORIZON_ORDER,
    Facet1Analysis,
    add_retained_share,
    assign_freshness_filter_order,
    assign_horizon_order,
    compute_bucket_calibration,
    compute_slice_summary,
    iter_freshness_subsets,
    load_enriched_horizon_dataset,
    materialize_kalshi_category_hierarchy,
    weighted_categorical_regression,
)
from src.common.analysis import AnalysisOutput

DRILLDOWN_PARENT_THRESHOLD = 10_000
DRILLDOWN_SLICE_THRESHOLD = 1_000


class Facet1KalshiCategoryFreshnessControlsAnalysis(Facet1Analysis):
    """Estimate Kalshi category calibration after conditioning on freshness."""

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet1_kalshi_category_freshness_controls",
            description="Kalshi category calibration controlled for freshness and horizon",
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
        df = load_enriched_horizon_dataset(self.dataset_path)
        df = materialize_kalshi_category_hierarchy(df)
        df = df[df["platform"] == "kalshi"].copy()

        category_order = (
            df.groupby("category_group", dropna=False)["market_id"]
            .nunique()
            .sort_values(ascending=False)
            .index.tolist()
        )
        baseline_category = category_order[0]

        summary_frames: list[pd.DataFrame] = []
        bucket_frames: list[pd.DataFrame] = []
        for freshness_filter, subset in iter_freshness_subsets(df):
            if subset.empty:
                continue

            subset["freshness_filter"] = freshness_filter
            group_cols = [
                "category_group",
                "horizon_label",
                "horizon_hours",
                "freshness_filter",
            ]
            bucket_df = compute_bucket_calibration(subset, group_cols)
            summary_df = compute_slice_summary(bucket_df, group_cols)
            freshness_stats = (
                subset.groupby(group_cols, dropna=False)
                .agg(median_staleness_age_hours=("staleness_age_hours", "median"))
                .reset_index()
            )

            bucket_frames.append(bucket_df)
            summary_frames.append(summary_df.merge(freshness_stats, on=group_cols, how="left"))

        summary_df = pd.concat(summary_frames, ignore_index=True)
        summary_df = self._expand_freshness_summary(df, summary_df)
        summary_df = add_retained_share(
            summary_df,
            ["category_group", "horizon_label", "horizon_hours"],
        )
        summary_df = assign_horizon_order(summary_df)
        summary_df = assign_freshness_filter_order(summary_df)
        summary_df["category_group"] = pd.Categorical(
            summary_df["category_group"],
            categories=category_order,
            ordered=True,
        )
        summary_df = summary_df.sort_values(
            ["category_group", "horizon_label", "freshness_filter"]
        ).reset_index(drop=True)

        bucket_df = pd.concat(bucket_frames, ignore_index=True)
        bucket_df = assign_horizon_order(bucket_df)
        bucket_df = assign_freshness_filter_order(bucket_df)
        bucket_df["category_group"] = pd.Categorical(
            bucket_df["category_group"],
            categories=category_order,
            ordered=True,
        )
        self.bucket_details = bucket_df.sort_values(
            ["category_group", "horizon_label", "freshness_filter", "price_bucket_5c_floor"]
        ).reset_index(drop=True)

        regression_df = weighted_categorical_regression(
            self.bucket_details.copy(),
            outcome_col="calibration_gap",
            weight_col="market_count",
            categorical_cols=["category_group", "freshness_filter", "horizon_label", "price_bucket_5c_label"],
            baseline_levels={
                "category_group": baseline_category,
                "freshness_filter": "all",
                "horizon_label": HORIZON_ORDER[0],
                "price_bucket_5c_label": self.bucket_details["price_bucket_5c_label"].astype(str).min(),
            },
        )
        regression_df["control_family"] = "freshness"
        self.regression_coefficients = regression_df

        self.category_mid_drilldown = self._build_category_mid_drilldown(df)

        fig = self._create_figure(summary_df, category_order, baseline_category)
        summary_df["category_group"] = summary_df["category_group"].astype(str)
        summary_df["horizon_label"] = summary_df["horizon_label"].astype(str)
        summary_df["freshness_filter"] = summary_df["freshness_filter"].astype(str)
        return AnalysisOutput(figure=fig, data=summary_df)

    def _build_category_mid_drilldown(self, df: pd.DataFrame) -> pd.DataFrame:
        parent_counts = df.groupby("category_group")["market_id"].nunique()
        qualifying_groups = parent_counts[parent_counts >= DRILLDOWN_PARENT_THRESHOLD].index.tolist()
        if not qualifying_groups:
            return pd.DataFrame()

        summary_frames: list[pd.DataFrame] = []
        for freshness_filter, subset in iter_freshness_subsets(df[df["category_group"].isin(qualifying_groups)]):
            if subset.empty:
                continue

            subset["freshness_filter"] = freshness_filter
            group_cols = [
                "category_group",
                "category_mid",
                "horizon_label",
                "horizon_hours",
                "freshness_filter",
            ]
            bucket_df = compute_bucket_calibration(subset, group_cols)
            summary_df = compute_slice_summary(bucket_df, group_cols)
            summary_frames.append(summary_df)

        if not summary_frames:
            return pd.DataFrame()

        drilldown_df = pd.concat(summary_frames, ignore_index=True)
        drilldown_df = add_retained_share(
            drilldown_df,
            ["category_group", "category_mid", "horizon_label", "horizon_hours"],
        )
        drilldown_df = drilldown_df[drilldown_df["market_count"] >= DRILLDOWN_SLICE_THRESHOLD].copy()
        drilldown_df = assign_horizon_order(drilldown_df)
        drilldown_df = assign_freshness_filter_order(drilldown_df)
        return drilldown_df.sort_values(
            ["category_group", "category_mid", "horizon_label", "freshness_filter"]
        ).reset_index(drop=True)

    def _expand_freshness_summary(self, df: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
        """Ensure every observed category-horizon pair carries all configured filters."""
        base_keys = (
            df[["category_group", "horizon_label", "horizon_hours"]]
            .drop_duplicates()
            .assign(_merge_key=1)
        )
        freshness_filters = pd.DataFrame({"freshness_filter": FRESHNESS_FILTER_ORDER, "_merge_key": 1})
        complete_index = (
            base_keys.merge(freshness_filters, on="_merge_key", how="inner")
            .drop(columns="_merge_key")
        )
        expanded_df = complete_index.merge(
            summary_df,
            on=["category_group", "horizon_label", "horizon_hours", "freshness_filter"],
            how="left",
        )
        expanded_df["market_count"] = expanded_df["market_count"].fillna(0).astype(int)
        expanded_df["bucket_count"] = expanded_df["bucket_count"].fillna(0).astype(int)
        return expanded_df

    def _create_figure(
        self,
        summary_df: pd.DataFrame,
        category_order: list[str],
        baseline_category: str,
    ) -> plt.Figure:
        fig = plt.figure(figsize=(22, 14))
        axes = fig.subplot_mosaic(
            [
                ["heatmap_1d", "heatmap_6h", "heatmap_1h"],
                ["comparison", "comparison", "coefficients"],
            ]
        )

        for horizon in HEADLINE_HORIZON_ORDER:
            heatmap_df = (
                summary_df[summary_df["horizon_label"].astype(str) == horizon]
                .pivot(index="category_group", columns="freshness_filter", values="expected_calibration_error")
                .reindex(index=category_order, columns=FRESHNESS_FILTER_ORDER)
            )
            ax = axes[f"heatmap_{horizon}"]
            image = ax.imshow(heatmap_df.to_numpy() * 100, aspect="auto", cmap="YlOrRd")
            ax.set_title(f"ECE Heatmap at {horizon}")
            ax.set_xticks(range(len(FRESHNESS_FILTER_ORDER)), FRESHNESS_FILTER_ORDER, rotation=30, ha="right")
            ax.set_yticks(range(len(category_order)), category_order)
            for row_idx, category in enumerate(category_order):
                for col_idx, freshness_filter in enumerate(FRESHNESS_FILTER_ORDER):
                    value = heatmap_df.loc[category, freshness_filter]
                    label = "" if pd.isna(value) else f"{value * 100:.2f}"
                    ax.text(
                        col_idx,
                        row_idx,
                        label,
                        ha="center",
                        va="center",
                        fontsize=8,
                        color="#111827",
                    )
            colorbar = fig.colorbar(image, ax=ax, fraction=0.046, pad=0.02)
            colorbar.set_label("ECE (%)")

        comparison_df = summary_df[
            (summary_df["horizon_label"].astype(str) == "1h")
            & (summary_df["freshness_filter"].astype(str).isin(["all", "<=6h"]))
        ].copy()
        comparison_pivot = comparison_df.pivot(
            index="category_group",
            columns="freshness_filter",
            values="expected_calibration_error",
        ).reindex(index=category_order, columns=["all", "<=6h"])

        y_positions = range(len(category_order))
        comparison_ax = axes["comparison"]
        comparison_ax.barh(
            [y - 0.18 for y in y_positions],
            comparison_pivot["all"] * 100,
            height=0.34,
            color="#111827",
            label="all",
        )
        comparison_ax.barh(
            [y + 0.18 for y in y_positions],
            comparison_pivot["<=6h"] * 100,
            height=0.34,
            color="#2563eb",
            label="<=6h",
        )
        comparison_ax.set_yticks(list(y_positions), category_order)
        comparison_ax.set_title("1h Horizon: All Rows vs <=6h")
        comparison_ax.set_xlabel("ECE (%)")
        comparison_ax.xaxis.set_major_formatter(PercentFormatter())
        comparison_ax.grid(axis="x", alpha=0.3)
        comparison_ax.legend(frameon=False)

        coefficient_df = self.regression_coefficients[
            self.regression_coefficients["predictor"] == "category_group"
        ].copy()
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

        coefficient_ax = axes["coefficients"]
        coefficient_ax.axvline(0, color="#111827", linewidth=1, alpha=0.6)
        coefficient_ax.errorbar(
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
        coefficient_ax.set_title("Category Effects After Freshness Controls")
        coefficient_ax.set_xlabel("Coefficient on Calibration Gap (pp)")
        coefficient_ax.grid(axis="x", alpha=0.3)

        fig.suptitle("Facet 1 Kalshi Category Controls: Freshness", y=0.98)
        fig.tight_layout()
        return fig
