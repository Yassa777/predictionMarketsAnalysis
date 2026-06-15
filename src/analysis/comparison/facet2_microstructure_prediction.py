"""Kalshi microstructure diagnostics for short-horizon predictability."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.analysis.comparison.facet1_slice_utils import BASE_DIR, load_enriched_dataset
from src.analysis.comparison.research_csv_utils import ResearchCsvAnalysis
from src.common.analysis import AnalysisOutput


class Facet2MicrostructurePredictionAnalysis(ResearchCsvAnalysis):
    """Summarize order flow, trade size, impact decay, and intraday effects."""

    research_subdir = "facet2"

    def __init__(self, dataset_path: Path | str | None = None, max_markets: int = 100):
        super().__init__(
            name="facet2_return_autocorrelation",
            description="Kalshi short-horizon microstructure diagnostics",
        )
        self.dataset_path = Path(dataset_path) if dataset_path else None
        self.max_markets = max_markets

    def run(self) -> AnalysisOutput:
        market_df = load_enriched_dataset(self.dataset_path)
        kalshi_ids = (
            market_df.loc[
                market_df["platform"] == "kalshi",
                ["market_id", "primary_liquidity_metric", "reference_won"],
            ]
            .drop_duplicates(subset=["market_id"])
            .sort_values(["primary_liquidity_metric", "market_id"], ascending=[False, True])
            .head(self.max_markets)
        )

        con = duckdb.connect()
        con.execute("PRAGMA threads=4")
        con.execute("SET preserve_insertion_order = false")
        con.register("kalshi_ids", kalshi_ids)

        hourly_view = self._build_hourly_view_sql()
        autocorrelation = self._return_autocorrelation(con, hourly_view)
        order_flow = self._order_flow_future_returns(con, hourly_view)
        trade_size = self._trade_size_impact(con)
        impact_decay = self._price_impact_decay(con)
        time_of_day = self._time_of_day_effects(con)

        source_trade_count = con.execute(
            f"""
            SELECT COUNT(*)
            FROM '{BASE_DIR / "data" / "kalshi" / "trades"}/*.parquet' t
            JOIN kalshi_ids ids
              ON t.ticker = ids.market_id
            WHERE t.yes_price IS NOT NULL
              AND t.created_time IS NOT NULL
              AND t.count > 0
            """
        ).fetchone()[0]
        for table in [autocorrelation, order_flow, trade_size, impact_decay, time_of_day]:
            table.insert(1, "sample_scope", f"top_{self.max_markets}_kalshi_markets_by_facet1_liquidity")
            table.insert(2, "selected_market_count", int(len(kalshi_ids)))
            table.insert(3, "source_trade_count", int(source_trade_count))

        self.extra_tables = {
            "facet2_order_flow_future_returns": order_flow,
            "facet2_trade_size_impact": trade_size,
            "facet2_price_impact_decay": impact_decay,
            "facet2_time_of_day_effects": time_of_day,
        }
        return AnalysisOutput(data=autocorrelation)

    def _build_hourly_view_sql(self) -> str:
        return f"""
        WITH hourly AS (
            SELECT
                t.ticker AS market_id,
                DATE_TRUNC('hour', t.created_time) AS hour_ts,
                SUM(t.count)::DOUBLE AS contract_volume,
                SUM(CASE WHEN t.taker_side = 'yes' THEN t.count ELSE -t.count END)::DOUBLE AS signed_contract_volume,
                SUM(t.yes_price * t.count)::DOUBLE / NULLIF(SUM(t.count), 0) AS vwap_yes_price_cents,
                COUNT(*) AS trade_count
            FROM '{BASE_DIR / "data" / "kalshi" / "trades"}/*.parquet' t
            JOIN kalshi_ids ids
              ON t.ticker = ids.market_id
            WHERE t.yes_price IS NOT NULL
              AND t.created_time IS NOT NULL
              AND t.count > 0
            GROUP BY 1, 2
        ),
        sequenced AS (
            SELECT
                *,
                signed_contract_volume / NULLIF(contract_volume, 0) AS order_flow_imbalance,
                vwap_yes_price_cents - LAG(vwap_yes_price_cents) OVER (
                    PARTITION BY market_id ORDER BY hour_ts
                ) AS return_cents,
                LEAD(vwap_yes_price_cents) OVER (
                    PARTITION BY market_id ORDER BY hour_ts
                ) - vwap_yes_price_cents AS next_hour_return_cents
            FROM hourly
        )
        SELECT * FROM sequenced
        """

    def _return_autocorrelation(self, con: duckdb.DuckDBPyConnection, hourly_view: str) -> pd.DataFrame:
        rows = []
        for lag in [1, 3, 6, 24]:
            df = con.execute(
                f"""
                WITH sequenced AS ({hourly_view}),
                lagged AS (
                    SELECT
                        return_cents,
                        LAG(return_cents, {lag}) OVER (PARTITION BY market_id ORDER BY hour_ts) AS lagged_return_cents
                    FROM sequenced
                )
                SELECT
                    'kalshi' AS platform,
                    {lag} AS lag_observations,
                    COUNT(*) AS observation_count,
                    CORR(return_cents, lagged_return_cents) AS return_autocorrelation,
                    AVG(return_cents) AS mean_return_cents,
                    STDDEV_SAMP(return_cents) AS stddev_return_cents
                FROM lagged
                WHERE return_cents IS NOT NULL
                  AND lagged_return_cents IS NOT NULL
                """
            ).df()
            rows.append(df)
        return pd.concat(rows, ignore_index=True)

    def _order_flow_future_returns(self, con: duckdb.DuckDBPyConnection, hourly_view: str) -> pd.DataFrame:
        return con.execute(
            f"""
            WITH sequenced AS ({hourly_view}),
            bucketed AS (
                SELECT
                    CASE
                        WHEN order_flow_imbalance <= -0.75 THEN 'strong_no_pressure'
                        WHEN order_flow_imbalance <= -0.25 THEN 'moderate_no_pressure'
                        WHEN order_flow_imbalance < 0.25 THEN 'balanced'
                        WHEN order_flow_imbalance < 0.75 THEN 'moderate_yes_pressure'
                        ELSE 'strong_yes_pressure'
                    END AS order_flow_bucket,
                    *
                FROM sequenced
                WHERE next_hour_return_cents IS NOT NULL
            )
            SELECT
                'kalshi' AS platform,
                order_flow_bucket,
                COUNT(*) AS market_hour_count,
                SUM(contract_volume) AS contract_volume,
                AVG(order_flow_imbalance) AS mean_order_flow_imbalance,
                AVG(next_hour_return_cents) AS mean_next_hour_return_cents,
                MEDIAN(next_hour_return_cents) AS median_next_hour_return_cents,
                AVG(CASE WHEN next_hour_return_cents > 0 THEN 1 ELSE 0 END) AS positive_next_hour_share
            FROM bucketed
            GROUP BY 1, 2
            ORDER BY
                CASE order_flow_bucket
                    WHEN 'strong_no_pressure' THEN 1
                    WHEN 'moderate_no_pressure' THEN 2
                    WHEN 'balanced' THEN 3
                    WHEN 'moderate_yes_pressure' THEN 4
                    ELSE 5
                END
            """
        ).df()

    def _trade_size_impact(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(
            f"""
            WITH sequenced AS (
                SELECT
                    t.ticker AS market_id,
                    t.count AS contract_count,
                    t.taker_side,
                    t.yes_price,
                    LEAD(t.yes_price) OVER (
                        PARTITION BY t.ticker ORDER BY t.created_time, t.trade_id
                    ) - t.yes_price AS next_trade_move_cents
                FROM '{BASE_DIR / "data" / "kalshi" / "trades"}/*.parquet' t
                JOIN kalshi_ids ids
                  ON t.ticker = ids.market_id
                WHERE t.yes_price IS NOT NULL
                  AND t.created_time IS NOT NULL
                  AND t.count > 0
            ),
            bucketed AS (
                SELECT
                    CASE
                        WHEN contract_count = 1 THEN '1'
                        WHEN contract_count <= 5 THEN '2-5'
                        WHEN contract_count <= 10 THEN '6-10'
                        WHEN contract_count <= 50 THEN '11-50'
                        WHEN contract_count <= 100 THEN '51-100'
                        ELSE '>100'
                    END AS trade_size_bucket,
                    *
                FROM sequenced
                WHERE next_trade_move_cents IS NOT NULL
            )
            SELECT
                'kalshi' AS platform,
                trade_size_bucket,
                taker_side,
                COUNT(*) AS trade_count,
                SUM(contract_count) AS contract_volume,
                AVG(next_trade_move_cents) AS mean_next_trade_move_cents,
                MEDIAN(next_trade_move_cents) AS median_next_trade_move_cents,
                AVG(ABS(next_trade_move_cents)) AS mean_abs_next_trade_move_cents
            FROM bucketed
            GROUP BY 1, 2, 3
            ORDER BY
                CASE trade_size_bucket
                    WHEN '1' THEN 1
                    WHEN '2-5' THEN 2
                    WHEN '6-10' THEN 3
                    WHEN '11-50' THEN 4
                    WHEN '51-100' THEN 5
                    ELSE 6
                END,
                taker_side
            """
        ).df()

    def _price_impact_decay(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(
            f"""
            WITH sequenced AS (
                SELECT
                    t.ticker AS market_id,
                    t.yes_price,
                    LAG(t.yes_price) OVER (
                        PARTITION BY t.ticker ORDER BY t.created_time, t.trade_id
                    ) AS previous_price,
                    LEAD(t.yes_price, 1) OVER (
                        PARTITION BY t.ticker ORDER BY t.created_time, t.trade_id
                    ) AS price_after_1_trade,
                    LEAD(t.yes_price, 5) OVER (
                        PARTITION BY t.ticker ORDER BY t.created_time, t.trade_id
                    ) AS price_after_5_trades,
                    LEAD(t.yes_price, 20) OVER (
                        PARTITION BY t.ticker ORDER BY t.created_time, t.trade_id
                    ) AS price_after_20_trades
                FROM '{BASE_DIR / "data" / "kalshi" / "trades"}/*.parquet' t
                JOIN kalshi_ids ids
                  ON t.ticker = ids.market_id
                WHERE t.yes_price IS NOT NULL
                  AND t.created_time IS NOT NULL
                  AND t.count > 0
            ),
            impacted AS (
                SELECT
                    *,
                    yes_price - previous_price AS impact_cents,
                    ABS(yes_price - previous_price) AS abs_impact_cents
                FROM sequenced
                WHERE previous_price IS NOT NULL
                  AND ABS(yes_price - previous_price) >= 1
            ),
            bucketed AS (
                SELECT
                    CASE
                        WHEN abs_impact_cents < 2 THEN '1c'
                        WHEN abs_impact_cents < 5 THEN '2-4c'
                        WHEN abs_impact_cents < 10 THEN '5-9c'
                        ELSE '>=10c'
                    END AS impact_bucket,
                    CASE WHEN impact_cents > 0 THEN 'up' ELSE 'down' END AS impact_direction,
                    *
                FROM impacted
            )
            SELECT
                'kalshi' AS platform,
                impact_bucket,
                impact_direction,
                COUNT(*) AS impacted_trade_count,
                AVG(impact_cents) AS mean_initial_impact_cents,
                AVG(price_after_1_trade - previous_price) AS mean_remaining_after_1_trade_cents,
                AVG(price_after_5_trades - previous_price) AS mean_remaining_after_5_trades_cents,
                AVG(price_after_20_trades - previous_price) AS mean_remaining_after_20_trades_cents,
                AVG((price_after_1_trade - previous_price) / NULLIF(impact_cents, 0)) AS mean_remaining_fraction_after_1_trade,
                AVG((price_after_5_trades - previous_price) / NULLIF(impact_cents, 0)) AS mean_remaining_fraction_after_5_trades,
                AVG((price_after_20_trades - previous_price) / NULLIF(impact_cents, 0)) AS mean_remaining_fraction_after_20_trades
            FROM bucketed
            GROUP BY 1, 2, 3
            ORDER BY
                CASE impact_bucket
                    WHEN '1c' THEN 1
                    WHEN '2-4c' THEN 2
                    WHEN '5-9c' THEN 3
                    ELSE 4
                END,
                impact_direction
            """
        ).df()

    def _time_of_day_effects(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(
            f"""
            WITH sequenced AS (
                SELECT
                    t.ticker AS market_id,
                    EXTRACT(hour FROM t.created_time) AS hour_utc,
                    CASE
                        WHEN EXTRACT(hour FROM t.created_time) BETWEEN 0 AND 5 THEN '00-05 overnight'
                        WHEN EXTRACT(hour FROM t.created_time) BETWEEN 6 AND 11 THEN '06-11 morning'
                        WHEN EXTRACT(hour FROM t.created_time) BETWEEN 12 AND 17 THEN '12-17 afternoon'
                        ELSE '18-23 evening'
                    END AS daypart_utc,
                    t.count AS contract_count,
                    t.taker_side,
                    t.yes_price,
                    ids.reference_won,
                    LEAD(t.yes_price) OVER (
                        PARTITION BY t.ticker ORDER BY t.created_time, t.trade_id
                    ) - t.yes_price AS next_trade_move_cents
                FROM '{BASE_DIR / "data" / "kalshi" / "trades"}/*.parquet' t
                JOIN kalshi_ids ids
                  ON t.ticker = ids.market_id
                WHERE t.yes_price IS NOT NULL
                  AND t.created_time IS NOT NULL
                  AND t.count > 0
            ),
            bucketed AS (
                SELECT
                    'hour_utc' AS bucket_type,
                    hour_utc::VARCHAR AS bucket,
                    hour_utc AS bucket_sort,
                    contract_count,
                    taker_side,
                    yes_price,
                    reference_won,
                    next_trade_move_cents
                FROM sequenced
                UNION ALL
                SELECT
                    'daypart_utc' AS bucket_type,
                    daypart_utc AS bucket,
                    CASE daypart_utc
                        WHEN '00-05 overnight' THEN 0
                        WHEN '06-11 morning' THEN 1
                        WHEN '12-17 afternoon' THEN 2
                        ELSE 3
                    END AS bucket_sort,
                    contract_count,
                    taker_side,
                    yes_price,
                    reference_won,
                    next_trade_move_cents
                FROM sequenced
            )
            SELECT
                'kalshi' AS platform,
                bucket_type,
                bucket,
                COUNT(*) AS trade_count,
                SUM(contract_count) AS contract_volume,
                AVG(CASE WHEN taker_side = 'yes' THEN 1 ELSE 0 END) AS yes_taker_share,
                AVG(next_trade_move_cents) AS mean_next_trade_move_cents,
                AVG(ABS(next_trade_move_cents)) AS mean_abs_next_trade_move_cents,
                AVG((yes_price / 100.0 - reference_won) * (yes_price / 100.0 - reference_won)) AS brier_score_yes_price,
                AVG(reference_won) AS resolved_yes_win_rate
            FROM bucketed
            WHERE next_trade_move_cents IS NOT NULL
            GROUP BY 1, 2, 3, bucket_sort
            ORDER BY bucket_type, bucket_sort
            """
        ).df()
