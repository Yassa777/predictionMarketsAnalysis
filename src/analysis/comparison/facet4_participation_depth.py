"""Participation-depth diagnostics for market calibration.

The default path intentionally avoids raw Polymarket wallet aggregation because
the current trade table is large enough to exhaust the local spill budget. It
uses the Facet 1 filtered trade shards for Polymarket trade-count depth and
keeps wallet HHI columns present but null unless a bounded wallet table is added
later.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.analysis.comparison.facet1_slice_utils import (
    BASE_DIR,
    compute_bucket_calibration,
    compute_slice_summary,
    load_enriched_dataset,
)
from src.analysis.comparison.research_csv_utils import ResearchCsvAnalysis, add_quantile_bucket
from src.common.analysis import AnalysisOutput

DEPTH_LABELS = ["Q1 Thinnest", "Q2 Thin", "Q3 Mid", "Q4 Deep", "Q5 Deepest"]


class Facet4ParticipationDepthAnalysis(ResearchCsvAnalysis):
    """Measure whether participation depth explains calibration quality."""

    research_subdir = "facet4"

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet4_participation_calibration_by_depth",
            description="Calibration by participation depth and wallet concentration",
        )
        self.dataset_path = Path(dataset_path) if dataset_path else None

    def run(self) -> AnalysisOutput:
        df = load_enriched_dataset(self.dataset_path)
        features = self._build_market_features(df)
        features = add_participation_depth(features)

        depth_bucket = compute_bucket_calibration(
            features.dropna(subset=["participation_depth_bucket"]),
            ["platform", "participation_depth_bucket"],
        )
        depth_summary = compute_slice_summary(depth_bucket, ["platform", "participation_depth_bucket"])
        depth_stats = (
            features.dropna(subset=["participation_depth_bucket"])
            .groupby(["platform", "participation_depth_bucket"], dropna=False)
            .agg(
                participation_proxy_name=("participation_proxy_name", first_non_null),
                median_participation_proxy_value=("participation_proxy_value", "median"),
                min_participation_proxy_value=("participation_proxy_value", "min"),
                max_participation_proxy_value=("participation_proxy_value", "max"),
                median_trade_count=("trade_count", "median"),
                median_contract_or_usd_volume_proxy=("contract_or_usd_volume_proxy", "median"),
                median_polymarket_unique_wallet_count=("polymarket_unique_wallet_count", "median"),
                median_polymarket_wallet_hhi=("polymarket_wallet_hhi", "median"),
                median_kalshi_open_interest_contracts=("kalshi_open_interest_contracts", "median"),
            )
            .reset_index()
        )
        depth_summary = depth_summary.merge(
            depth_stats,
            on=["platform", "participation_depth_bucket"],
            how="left",
        )
        depth_summary = depth_summary.sort_values(["platform", "participation_depth_bucket"]).reset_index(drop=True)

        polymarket_concentration = self._build_polymarket_concentration_summary(features)
        kalshi_controls = self._build_kalshi_controls(features)

        self.extra_tables = {
            "facet4_participation_market_features": features,
            "facet4_polymarket_wallet_concentration": polymarket_concentration,
            "facet4_kalshi_trade_count_open_interest_controls": kalshi_controls,
        }
        return AnalysisOutput(data=depth_summary)

    def _build_market_features(self, df: pd.DataFrame) -> pd.DataFrame:
        con = duckdb.connect()
        con.execute("PRAGMA threads=2")

        kalshi_base = df.loc[df["platform"] == "kalshi", ["market_id", "close_ts"]].drop_duplicates()
        con.register("kalshi_base", kalshi_base)
        kalshi_trade_stats = con.execute(
            f"""
            SELECT
                t.ticker AS market_id,
                COUNT(*) AS kalshi_trade_count,
                SUM(t.count)::DOUBLE AS kalshi_contract_volume_from_trades
            FROM '{BASE_DIR / "data" / "kalshi" / "trades"}/*.parquet' t
            JOIN kalshi_base b
              ON t.ticker = b.market_id
            WHERE t.created_time <= b.close_ts
            GROUP BY 1
            """
        ).df()

        polymarket_trade_stats = self._build_polymarket_trade_stats(con, df)

        columns = [
            "platform",
            "market_id",
            "close_ts",
            "reference_price_cents",
            "reference_won",
            "price_bucket_5c_floor",
            "price_bucket_5c_mid",
            "price_bucket_5c_label",
            "primary_liquidity_metric_name",
            "primary_liquidity_metric",
            "secondary_liquidity_metric_name",
            "secondary_liquidity_metric",
            "kalshi_volume_contracts",
            "kalshi_open_interest_contracts",
            "polymarket_volume_usd",
            "polymarket_liquidity_usd",
        ]
        features = df[columns].copy()
        features = features.merge(kalshi_trade_stats, on="market_id", how="left")
        features = features.merge(polymarket_trade_stats, on="market_id", how="left")
        features["trade_count"] = features["kalshi_trade_count"].where(
            features["platform"] == "kalshi",
            features["polymarket_trade_count"],
        )
        features["contract_or_usd_volume_proxy"] = features[
            "kalshi_contract_volume_from_trades"
        ].where(
            features["platform"] == "kalshi",
            features["polymarket_volume_usd"],
        )
        features["contract_or_usd_volume_proxy_unit"] = features["platform"].map(
            {"kalshi": "contracts", "polymarket": "usd"}
        )
        features["polymarket_unique_wallet_count"] = pd.NA
        features["polymarket_wallet_hhi"] = pd.NA
        features["polymarket_effective_wallet_count"] = pd.NA
        features["participation_proxy_name"] = features["platform"].map(
            {
                "kalshi": "trade_count",
                "polymarket": "trade_count_filtered_facet1",
            }
        )
        features["participation_proxy_value"] = features["trade_count"]
        return features

    def _build_polymarket_trade_stats(
        self,
        con: duckdb.DuckDBPyConnection,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        ctf_path = BASE_DIR / "data" / "derived" / ".facet1_chunks" / "polymarket_ctf_relevant.parquet"
        legacy_path = BASE_DIR / "data" / "derived" / ".facet1_chunks" / "polymarket_legacy_relevant.parquet"
        columns = [
            "market_id",
            "polymarket_trade_count",
            "polymarket_ctf_trade_count",
            "polymarket_legacy_trade_count",
            "polymarket_participation_source",
        ]
        if not ctf_path.exists() or not legacy_path.exists():
            return pd.DataFrame(columns=columns)

        pm_base = df.loc[df["platform"] == "polymarket", ["market_id", "close_ts"]].drop_duplicates()
        con.register("pm_base", pm_base)
        return con.execute(
            f"""
            WITH resolved_markets AS (
                SELECT
                    p.market_id,
                    p.close_ts,
                    json_extract_string(m.clob_token_ids, '$[0]') AS token_0,
                    json_extract_string(m.clob_token_ids, '$[1]') AS token_1,
                    LOWER(m.market_maker_address) AS fpmm_address
                FROM pm_base p
                JOIN '{BASE_DIR / "data" / "polymarket" / "markets"}/*.parquet' m
                  ON m.id = p.market_id
            ),
            token_map AS (
                SELECT market_id, close_ts, token_0 AS token_id
                FROM resolved_markets
                WHERE token_0 IS NOT NULL AND token_0 <> ''

                UNION ALL

                SELECT market_id, close_ts, token_1 AS token_id
                FROM resolved_markets
                WHERE token_1 IS NOT NULL AND token_1 <> ''
            ),
            ctf_stats AS (
                SELECT
                    tm.market_id,
                    COUNT(*) AS polymarket_ctf_trade_count
                FROM '{ctf_path}' t
                JOIN token_map tm
                  ON t.token_id = tm.token_id
                WHERE t.trade_ts <= tm.close_ts
                GROUP BY 1
            ),
            legacy_stats AS (
                SELECT
                    rm.market_id,
                    COUNT(*) AS polymarket_legacy_trade_count
                FROM '{legacy_path}' t
                JOIN resolved_markets rm
                  ON t.fpmm_address = rm.fpmm_address
                WHERE t.trade_ts <= rm.close_ts
                GROUP BY 1
            )
            SELECT
                COALESCE(c.market_id, l.market_id) AS market_id,
                (
                    COALESCE(c.polymarket_ctf_trade_count, 0)
                    + COALESCE(l.polymarket_legacy_trade_count, 0)
                )::BIGINT AS polymarket_trade_count,
                COALESCE(c.polymarket_ctf_trade_count, 0)::BIGINT AS polymarket_ctf_trade_count,
                COALESCE(l.polymarket_legacy_trade_count, 0)::BIGINT AS polymarket_legacy_trade_count,
                'facet1_filtered_trade_shards' AS polymarket_participation_source
            FROM ctf_stats c
            FULL OUTER JOIN legacy_stats l USING (market_id)
            """
        ).df()

    def _build_polymarket_concentration_summary(self, features: pd.DataFrame) -> pd.DataFrame:
        pm = features[
            (features["platform"] == "polymarket")
            & features["polymarket_wallet_hhi"].notna()
            & features["reference_price_cents"].notna()
        ].copy()
        if pm.empty:
            return pd.DataFrame(
                [
                    {
                        "platform": "polymarket",
                        "wallet_hhi_bucket": "not_computed",
                        "market_count": 0,
                        "bucket_count": 0,
                        "expected_calibration_error": pd.NA,
                        "signed_calibration_gap": pd.NA,
                        "low_tail_gap": pd.NA,
                        "high_tail_gap": pd.NA,
                        "coverage_note": (
                            "wallet_hhi_not_computed_default_path_uses_facet1_filtered_trade_counts"
                        ),
                    }
                ]
            )

        pm["wallet_hhi_bucket"] = pd.cut(
            pm["polymarket_wallet_hhi"],
            bins=[0, 0.01, 0.05, 0.10, 0.25, 1.0],
            labels=["<=0.01", "0.01-0.05", "0.05-0.10", "0.10-0.25", ">0.25"],
            include_lowest=True,
        ).astype("object")
        bucket = compute_bucket_calibration(pm.dropna(subset=["wallet_hhi_bucket"]), ["wallet_hhi_bucket"])
        summary = compute_slice_summary(bucket, ["wallet_hhi_bucket"])
        summary["platform"] = "polymarket"
        return summary[
            [
                "platform",
                "wallet_hhi_bucket",
                "market_count",
                "bucket_count",
                "expected_calibration_error",
                "signed_calibration_gap",
                "low_tail_gap",
                "high_tail_gap",
            ]
        ].sort_values("wallet_hhi_bucket")

    def _build_kalshi_controls(self, features: pd.DataFrame) -> pd.DataFrame:
        kalshi = features[features["platform"] == "kalshi"].copy()
        kalshi = add_quantile_bucket(kalshi, "trade_count", "trade_count_bucket", DEPTH_LABELS)
        kalshi = add_quantile_bucket(
            kalshi,
            "kalshi_open_interest_contracts",
            "open_interest_bucket",
            DEPTH_LABELS,
        )
        bucket = compute_bucket_calibration(
            kalshi.dropna(subset=["trade_count_bucket", "open_interest_bucket"]),
            ["trade_count_bucket", "open_interest_bucket"],
        )
        summary = compute_slice_summary(bucket, ["trade_count_bucket", "open_interest_bucket"])
        stats = (
            kalshi.dropna(subset=["trade_count_bucket", "open_interest_bucket"])
            .groupby(["trade_count_bucket", "open_interest_bucket"], dropna=False)
            .agg(
                median_trade_count=("trade_count", "median"),
                min_trade_count=("trade_count", "min"),
                max_trade_count=("trade_count", "max"),
                median_open_interest_contracts=("kalshi_open_interest_contracts", "median"),
                min_open_interest_contracts=("kalshi_open_interest_contracts", "min"),
                max_open_interest_contracts=("kalshi_open_interest_contracts", "max"),
                median_volume_contracts=("contract_or_usd_volume_proxy", "median"),
            )
            .reset_index()
        )
        summary = summary.merge(stats, on=["trade_count_bucket", "open_interest_bucket"], how="left")
        summary["platform"] = "kalshi"
        return summary.sort_values(["trade_count_bucket", "open_interest_bucket"]).reset_index(drop=True)


def add_participation_depth(features: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for _, group in features.groupby("platform", dropna=False):
        frames.append(
            add_quantile_bucket(
                group,
                "participation_proxy_value",
                "participation_depth_bucket",
                DEPTH_LABELS,
            )
        )
    return pd.concat(frames, ignore_index=True)


def first_non_null(series: pd.Series):
    non_null = series.dropna()
    if non_null.empty:
        return pd.NA
    return non_null.iloc[0]
