"""Baseline Facet 1 calibration curves from the unified last-trade dataset."""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.common.analysis import Analysis, AnalysisOutput
from src.common.interfaces.chart import ChartConfig, ChartType, UnitType


class Facet1UnifiedCalibrationCurvesAnalysis(Analysis):
    """Plot Kalshi, Polymarket, and pooled calibration curves from the unified dataset."""

    def save(
        self,
        output_dir: Path | str,
        formats: list[str] | None = None,
        dpi: int = 300,
    ) -> dict[str, Path]:
        """Save only static formats for this non-animated analysis."""
        if formats is None:
            formats = ["png", "pdf", "csv", "json"]
        else:
            formats = [fmt for fmt in formats if fmt != "gif"]
        return super().save(output_dir, formats, dpi)

    def __init__(
        self,
        dataset_path: Path | str | None = None,
    ):
        super().__init__(
            name="facet1_unified_calibration_curves",
            description="Platform-specific and pooled 5c calibration curves from the unified last-trade dataset",
        )
        base_dir = Path(__file__).parent.parent.parent.parent
        self.dataset_path = Path(dataset_path or base_dir / "data" / "derived" / "facet1_unified_last_trade_dataset.parquet")

    def run(self) -> AnalysisOutput:
        """Execute the analysis and return outputs."""
        if not self.dataset_path.exists():
            raise FileNotFoundError(
                f"Derived dataset not found: {self.dataset_path}. "
                "Run scripts/build_facet1_unified_last_trade_5c_dataset.py first."
            )

        con = duckdb.connect()
        df = con.execute(
            f"""
            WITH bucketed AS (
                SELECT
                    platform,
                    price_bucket_5c_floor,
                    price_bucket_5c_mid,
                    price_bucket_5c_label,
                    COUNT(*) AS market_count,
                    SUM(reference_won) AS wins,
                    AVG(reference_won) AS empirical_win_rate,
                    AVG(reference_price_cents) / 100.0 AS avg_implied_probability
                FROM '{self.dataset_path}'
                GROUP BY 1, 2, 3, 4

                UNION ALL

                SELECT
                    'pooled' AS platform,
                    price_bucket_5c_floor,
                    price_bucket_5c_mid,
                    price_bucket_5c_label,
                    COUNT(*) AS market_count,
                    SUM(reference_won) AS wins,
                    AVG(reference_won) AS empirical_win_rate,
                    AVG(reference_price_cents) / 100.0 AS avg_implied_probability
                FROM '{self.dataset_path}'
                GROUP BY 2, 3, 4
            )
            SELECT
                platform,
                price_bucket_5c_floor,
                price_bucket_5c_mid,
                price_bucket_5c_label,
                market_count,
                wins,
                empirical_win_rate,
                avg_implied_probability,
                empirical_win_rate - avg_implied_probability AS calibration_gap
            FROM bucketed
            ORDER BY
                CASE platform
                    WHEN 'kalshi' THEN 1
                    WHEN 'polymarket' THEN 2
                    ELSE 3
                END,
                price_bucket_5c_floor
            """
        ).df()

        df = self._add_wilson_intervals(df)
        fig = self._create_figure(df)
        chart = self._create_chart(df)

        return AnalysisOutput(figure=fig, data=df, chart=chart)

    def _add_wilson_intervals(self, df: pd.DataFrame) -> pd.DataFrame:
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

        result = df.copy()
        result["ci_lower"] = np.clip(center - margin, 0.0, 1.0)
        result["ci_upper"] = np.clip(center + margin, 0.0, 1.0)
        return result

    def _create_figure(self, df: pd.DataFrame) -> plt.Figure:
        """Create the matplotlib figure."""
        fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharex=True, sharey=True)
        platforms = ["kalshi", "polymarket", "pooled"]
        titles = ["Kalshi", "Polymarket", "Pooled"]
        colors = {
            "kalshi": "#10B981",
            "polymarket": "#3B82F6",
            "pooled": "#111827",
        }

        for ax, platform, title in zip(axes, platforms, titles):
            platform_df = df[df["platform"] == platform].copy()
            x = platform_df["avg_implied_probability"].to_numpy() * 100
            y = platform_df["empirical_win_rate"].to_numpy() * 100
            lower = platform_df["ci_lower"].to_numpy() * 100
            upper = platform_df["ci_upper"].to_numpy() * 100
            counts = platform_df["market_count"].to_numpy()

            ax.plot([0, 100], [0, 100], linestyle="--", color="#9CA3AF", linewidth=1.2)
            ax.fill_between(x, lower, upper, alpha=0.18, color=colors[platform])
            ax.plot(x, y, color=colors[platform], linewidth=2, marker="o", markersize=4)
            ax.scatter(x, y, s=np.clip(np.sqrt(counts), 10, 80), color=colors[platform], alpha=0.75)

            ax.set_title(title)
            ax.set_xlim(0, 100)
            ax.set_ylim(0, 100)
            ax.set_xticks(range(0, 101, 10))
            ax.set_yticks(range(0, 101, 10))
            ax.grid(True, alpha=0.25)
            ax.set_xlabel("Average Implied Probability (%)")

        axes[0].set_ylabel("Empirical Win Rate (%)")
        fig.suptitle("Facet 1 Baseline Calibration Curves (5c buckets, last trade before close)", y=1.02)
        plt.tight_layout()
        return fig

    def _create_chart(self, df: pd.DataFrame) -> ChartConfig:
        """Create the chart configuration for web display."""
        chart_df = df.copy()
        chart_df["actual"] = (chart_df["empirical_win_rate"] * 100).round(2)
        chart_df["implied"] = (chart_df["avg_implied_probability"] * 100).round(2)
        chart_df["ci_lower_pct"] = (chart_df["ci_lower"] * 100).round(2)
        chart_df["ci_upper_pct"] = (chart_df["ci_upper"] * 100).round(2)

        chart_data = [
            {
                "platform_bucket": f"{row['platform']}:{row['price_bucket_5c_label']}",
                "platform": row["platform"],
                "bucket_mid": float(row["price_bucket_5c_mid"]),
                "actual": float(row["actual"]),
                "implied": float(row["implied"]),
                "ci_lower": float(row["ci_lower_pct"]),
                "ci_upper": float(row["ci_upper_pct"]),
                "market_count": int(row["market_count"]),
            }
            for _, row in chart_df.iterrows()
        ]

        return ChartConfig(
            type=ChartType.LINE,
            data=chart_data,
            xKey="bucket_mid",
            yKeys=["actual", "implied", "ci_lower", "ci_upper"],
            title="Facet 1 Baseline Calibration Curves",
            yUnit=UnitType.PERCENT,
            colors={
                "actual": "#111827",
                "implied": "#9CA3AF",
                "ci_lower": "#6B7280",
                "ci_upper": "#6B7280",
            },
            strokeDasharrays=[None, "5 5", "2 4", "2 4"],
            xLabel="5c Bucket Midpoint (%)",
            yLabel="Win Rate (%)",
            caption="Data includes Kalshi, Polymarket, and pooled rows; use the platform field to facet downstream.",
        )
