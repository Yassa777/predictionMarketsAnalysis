"""Market lifespan diagnostics for the remote research CSV packet."""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from src.analysis.comparison.facet1_polymarket_category_mapping import assign_polymarket_categories
from src.analysis.comparison.facet1_slice_utils import (
    BASE_DIR,
    compute_bucket_calibration,
    compute_slice_summary,
    load_enriched_dataset,
    load_enriched_horizon_dataset,
)
from src.analysis.comparison.research_csv_utils import ResearchCsvAnalysis
from src.common.analysis import AnalysisOutput

LIFESPAN_BINS_HOURS = [0, 1, 6, 24, 24 * 7, 24 * 30, 24 * 180, np.inf]
LIFESPAN_LABELS = ["<=1h", "1-6h", "6-24h", "1-7d", "7-30d", "30-180d", ">180d"]
LIFE_STAGE_LABELS = [
    "0-10%",
    "10-20%",
    "20-30%",
    "30-40%",
    "40-50%",
    "50-60%",
    "60-70%",
    "70-80%",
    "80-90%",
    "90-100%",
]


class Facet3LifespanAnalysis(ResearchCsvAnalysis):
    """Measure whether market lifespan explains calibration and activity timing."""

    research_subdir = "facet3"

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet3_lifespan_calibration",
            description="Calibration and activity diagnostics by market lifespan",
        )
        self.dataset_path = Path(dataset_path) if dataset_path else None

    def run(self) -> AnalysisOutput:
        df = load_enriched_dataset(self.dataset_path)
        df = add_lifespan_bucket(df)

        lifespan_bucket = compute_bucket_calibration(df.dropna(subset=["lifespan_bucket"]), ["platform", "lifespan_bucket"])
        lifespan_summary = compute_slice_summary(lifespan_bucket, ["platform", "lifespan_bucket"])
        lifespan_summary = sort_lifespan(lifespan_summary)

        category_df = self._add_polymarket_categories(df)
        category_bucket = compute_bucket_calibration(
            category_df.dropna(subset=["lifespan_bucket", "category_group"]),
            ["platform", "category_group", "lifespan_bucket"],
        )
        category_summary = compute_slice_summary(category_bucket, ["platform", "category_group", "lifespan_bucket"])
        category_summary = sort_lifespan(category_summary, ["platform", "category_group", "lifespan_bucket"])

        volume_concentration = self._build_kalshi_volume_concentration(df)
        late_stage_movement = self._build_late_stage_price_movement(df)
        stale_candidates = self._build_stale_market_candidates(category_df)

        self.extra_tables = {
            "facet3_lifespan_category_calibration": category_summary,
            "facet3_volume_concentration": volume_concentration,
            "facet3_late_stage_price_movement": late_stage_movement,
            "facet3_stale_market_candidates": stale_candidates,
        }
        return AnalysisOutput(data=lifespan_summary)

    def _add_polymarket_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        kalshi_df = df[df["platform"] == "kalshi"].copy()
        pm_df = df[df["platform"] == "polymarket"].copy()
        if pm_df.empty:
            return kalshi_df

        con = duckdb.connect()
        text_df = con.execute(
            f"""
            SELECT DISTINCT id AS market_id, question, slug
            FROM '{BASE_DIR / "data" / "polymarket" / "markets"}/*.parquet'
            """
        ).df()
        pm_df = pm_df.merge(text_df, on="market_id", how="left")
        pm_df = assign_polymarket_categories(pm_df)
        return pd.concat([kalshi_df, pm_df], ignore_index=True)

    def _build_kalshi_volume_concentration(self, df: pd.DataFrame) -> pd.DataFrame:
        kalshi_base = df[
            (df["platform"] == "kalshi")
            & df["market_start_ts"].notna()
            & df["close_ts"].notna()
            & (df["lifespan_hours"] > 0)
        ][["market_id", "market_start_ts", "close_ts", "lifespan_hours", "lifespan_bucket"]].copy()
        if kalshi_base.empty:
            return pd.DataFrame()

        con = duckdb.connect()
        con.register("kalshi_base", kalshi_base)
        concentration = con.execute(
            f"""
            WITH staged AS (
                SELECT
                    'kalshi' AS platform,
                    b.lifespan_bucket,
                    LEAST(
                        9,
                        GREATEST(
                            0,
                            CAST(FLOOR(
                                10.0 * EXTRACT(EPOCH FROM (t.created_time - b.market_start_ts))
                                / EXTRACT(EPOCH FROM (b.close_ts - b.market_start_ts))
                            ) AS INTEGER)
                    )
                    ) AS life_stage_index,
                    b.market_id,
                    COUNT(*) AS trade_count,
                    SUM(t.count)::DOUBLE AS contract_volume
                FROM '{BASE_DIR / "data" / "kalshi" / "trades"}/*.parquet' t
                JOIN kalshi_base b
                  ON t.ticker = b.market_id
                WHERE t.created_time >= b.market_start_ts
                  AND t.created_time <= b.close_ts
                GROUP BY 1, 2, 3, 4
            ),
            bucketed AS (
                SELECT
                    platform,
                    lifespan_bucket,
                    life_stage_index,
                    COUNT(DISTINCT market_id) AS market_count,
                    SUM(trade_count) AS trade_count,
                    SUM(contract_volume) AS contract_volume
                FROM staged
                GROUP BY 1, 2, 3
            ),
            totals AS (
                SELECT
                    platform,
                    lifespan_bucket,
                    SUM(trade_count) AS total_trade_count,
                    SUM(contract_volume) AS total_contract_volume
                FROM bucketed
                GROUP BY 1, 2
            )
            SELECT
                b.platform,
                b.lifespan_bucket,
                b.life_stage_index,
                b.market_count,
                b.trade_count,
                b.contract_volume,
                'kalshi_contract_count' AS volume_metric_name,
                b.trade_count::DOUBLE / t.total_trade_count AS trade_count_share,
                b.contract_volume / t.total_contract_volume AS contract_volume_share,
                SUM(b.contract_volume) OVER (
                    PARTITION BY b.platform, b.lifespan_bucket
                    ORDER BY b.life_stage_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) / t.total_contract_volume AS cumulative_contract_volume_share
            FROM bucketed b
            JOIN totals t USING (platform, lifespan_bucket)
            ORDER BY platform, lifespan_bucket, life_stage_index
            """
        ).df()
        concentration["life_stage_bucket"] = concentration["life_stage_index"].map(dict(enumerate(LIFE_STAGE_LABELS)))
        return concentration[
            [
                "platform",
                "lifespan_bucket",
                "life_stage_index",
                "life_stage_bucket",
                "volume_metric_name",
                "market_count",
                "trade_count",
                "contract_volume",
                "trade_count_share",
                "contract_volume_share",
                "cumulative_contract_volume_share",
            ]
        ]

    def _build_late_stage_price_movement(self, df: pd.DataFrame) -> pd.DataFrame:
        horizon_df = load_enriched_horizon_dataset()
        final_prices = df[["platform", "market_id", "reference_price_cents", "lifespan_bucket"]].rename(
            columns={"reference_price_cents": "final_reference_price_cents"}
        )
        movement_df = horizon_df.merge(final_prices, on=["platform", "market_id"], how="inner")
        movement_df = movement_df[
            movement_df["lifespan_hours"].gt(0)
            & movement_df["actual_hours_before_close"].notna()
            & movement_df["reference_price_cents"].notna()
            & movement_df["final_reference_price_cents"].notna()
        ].copy()
        movement_df["life_stage_fraction"] = (
            1.0 - (movement_df["actual_hours_before_close"] / movement_df["lifespan_hours"])
        ).clip(0.0, 1.0)
        movement_df["life_stage_index"] = np.floor(10.0 * movement_df["life_stage_fraction"]).clip(0, 9).astype(int)
        movement_df["life_stage_bucket"] = movement_df["life_stage_index"].map(dict(enumerate(LIFE_STAGE_LABELS)))
        movement_df["abs_move_to_final_cents"] = (
            movement_df["final_reference_price_cents"] - movement_df["reference_price_cents"]
        ).abs()
        movement_df["signed_move_to_final_cents"] = (
            movement_df["final_reference_price_cents"] - movement_df["reference_price_cents"]
        )
        grouped = (
            movement_df.groupby(
                [
                    "platform",
                    "lifespan_bucket",
                    "horizon_label",
                    "horizon_hours",
                    "life_stage_index",
                    "life_stage_bucket",
                ],
                dropna=False,
            )
            .agg(
                market_count=("market_id", "nunique"),
                mean_abs_move_to_final_cents=("abs_move_to_final_cents", "mean"),
                median_abs_move_to_final_cents=("abs_move_to_final_cents", "median"),
                p90_abs_move_to_final_cents=("abs_move_to_final_cents", lambda s: s.quantile(0.9)),
                mean_signed_move_to_final_cents=("signed_move_to_final_cents", "mean"),
                median_staleness_age_hours=("staleness_age_hours", "median"),
            )
            .reset_index()
            .sort_values(["platform", "lifespan_bucket", "horizon_hours", "life_stage_index"], ascending=[True, True, False, True])
        )
        return grouped

    def _build_stale_market_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        candidates = df.copy()
        candidates["final_staleness_hours"] = (
            pd.to_datetime(candidates["close_ts"], utc=True)
            - pd.to_datetime(candidates["last_trade_ts"], utc=True)
        ).dt.total_seconds() / 3600.0
        candidates["final_staleness_life_share"] = candidates["final_staleness_hours"] / candidates["lifespan_hours"]
        candidates["candidate_reasons"] = ""
        candidates.loc[candidates["final_staleness_hours"] >= 24, "candidate_reasons"] += "stale_24h_plus;"
        candidates.loc[
            candidates["final_staleness_life_share"] >= 0.25,
            "candidate_reasons",
        ] += "stale_25pct_life_plus;"
        candidates.loc[
            (candidates["liquidity_quintile"] == 1) & (candidates["final_staleness_hours"] >= 6),
            "candidate_reasons",
        ] += "quiet_low_liquidity_stale_6h_plus;"
        candidates["candidate_reasons"] = candidates["candidate_reasons"].str.rstrip(";")
        candidates = candidates[candidates["candidate_reasons"] != ""].copy()
        candidates["final_staleness_days"] = candidates["final_staleness_hours"] / 24.0
        candidates["lifespan_bucket"] = candidates["lifespan_bucket"].fillna("unknown")
        columns = [
            "platform",
            "market_id",
            "market_title",
            "category_group",
            "lifespan_bucket",
            "close_ts",
            "last_trade_ts",
            "final_staleness_hours",
            "final_staleness_days",
            "final_staleness_life_share",
            "reference_price_cents",
            "reference_won",
            "primary_liquidity_metric",
            "secondary_liquidity_metric",
            "liquidity_quintile_label",
            "candidate_reasons",
        ]
        return candidates[columns].sort_values("final_staleness_hours", ascending=False).head(5000)


def add_lifespan_bucket(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["lifespan_bucket"] = pd.cut(
        df["lifespan_hours"],
        bins=LIFESPAN_BINS_HOURS,
        labels=LIFESPAN_LABELS,
        include_lowest=True,
        right=False,
    ).astype("object")
    return df


def sort_lifespan(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    columns = columns or ["platform", "lifespan_bucket"]
    df = df.copy()
    df["lifespan_bucket"] = pd.Categorical(df["lifespan_bucket"], categories=LIFESPAN_LABELS, ordered=True)
    return df.sort_values(columns).reset_index(drop=True)
