"""Build an enriched market-level dataset for Phase 1 / Facet 1 slice analyses.

This script layers market metadata onto the existing unified last-trade dataset
 so downstream slice analyses can run without rebuilding the trade
normalization logic.

Outputs default to `data/derived/`:

- facet1_enriched_market_dataset.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/derived"),
        help="Directory where derived datasets will be written.",
    )
    return parser.parse_args()


def assign_quintiles(group: pd.DataFrame) -> pd.DataFrame:
    """Assign stable within-platform liquidity quintiles."""
    group = group.sort_values("primary_liquidity_metric", kind="mergesort").copy()
    group["liquidity_quintile"] = pd.qcut(
        group["primary_liquidity_metric"].rank(method="first"),
        5,
        labels=[1, 2, 3, 4, 5],
    ).astype(int)
    group["liquidity_quintile_label"] = group["liquidity_quintile"].map(
        {
            1: "Q1 Lowest",
            2: "Q2 Low-Mid",
            3: "Q3 Mid",
            4: "Q4 Mid-High",
            5: "Q5 Highest",
        }
    )
    return group


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    unified_df = con.execute(
        "SELECT * FROM 'data/derived/facet1_unified_last_trade_dataset.parquet'"
    ).df()

    kalshi_ids_df = unified_df.loc[unified_df["platform"] == "kalshi", ["market_id"]].drop_duplicates()
    polymarket_ids_df = unified_df.loc[
        unified_df["platform"] == "polymarket", ["market_id"]
    ].drop_duplicates()
    con.register("kalshi_ids_df", kalshi_ids_df)
    con.register("polymarket_ids_df", polymarket_ids_df)

    kalshi_meta = con.execute(
        """
        SELECT
            ticker AS market_id,
            event_ticker,
            created_time,
            open_time,
            close_time,
            volume::DOUBLE AS kalshi_volume_contracts,
            open_interest::DOUBLE AS kalshi_open_interest_contracts
        FROM 'data/kalshi/markets/*.parquet'
        JOIN kalshi_ids_df
            ON ticker = kalshi_ids_df.market_id
        """
    ).df()
    polymarket_meta = con.execute(
        """
        SELECT
            id AS market_id,
            created_at,
            end_date,
            volume AS polymarket_volume_usd,
            liquidity AS polymarket_liquidity_usd
        FROM 'data/polymarket/markets/*.parquet'
        JOIN polymarket_ids_df
            ON id = polymarket_ids_df.market_id
        """
    ).df()

    df = unified_df.merge(kalshi_meta, on="market_id", how="left")
    df = df.merge(polymarket_meta, on="market_id", how="left")

    df["event_ticker"] = df["event_ticker"].where(df["platform"] == "kalshi")
    df["category_raw"] = (
        df["event_ticker"]
        .fillna("")
        .str.extract(r"^([A-Z0-9]+)", expand=False)
        .replace("", "independent")
        .where(df["platform"] == "kalshi")
    )
    df["close_ts"] = pd.to_datetime(df["close_ts"], utc=True)
    df["last_trade_ts"] = pd.to_datetime(df["last_trade_ts"], utc=True)
    kalshi_start = pd.to_datetime(df["open_time"].combine_first(df["created_time"]), utc=True)
    polymarket_start = pd.to_datetime(df["created_at"], utc=True)
    df["market_start_ts"] = kalshi_start.where(df["platform"] == "kalshi", polymarket_start)

    df["close_month"] = df["close_ts"].dt.strftime("%Y-%m")
    df["close_date"] = df["close_ts"].dt.strftime("%Y-%m-%d")
    df["market_start_date"] = df["market_start_ts"].dt.strftime("%Y-%m-%d")
    df["lifespan_hours"] = (df["close_ts"] - df["market_start_ts"]).dt.total_seconds() / 3600.0
    df["lifespan_days"] = df["lifespan_hours"] / 24.0

    df["primary_liquidity_metric_name"] = df["platform"].map(
        {
            "kalshi": "volume_contracts",
            "polymarket": "volume_usd",
        }
    )
    df["secondary_liquidity_metric_name"] = df["platform"].map(
        {
            "kalshi": "open_interest_contracts",
            "polymarket": "liquidity_usd",
        }
    )
    df["primary_liquidity_metric"] = df["kalshi_volume_contracts"].where(
        df["platform"] == "kalshi",
        df["polymarket_volume_usd"],
    )
    df["secondary_liquidity_metric"] = df["kalshi_open_interest_contracts"].where(
        df["platform"] == "kalshi",
        df["polymarket_liquidity_usd"],
    )

    df = pd.concat([assign_quintiles(group) for _, group in df.groupby("platform")], ignore_index=True)

    output_path = output_dir / "facet1_enriched_market_dataset.parquet"
    df.to_parquet(output_path, index=False)

    print(f"Wrote enriched market dataset: {output_path}")
    print(f"Rows: {len(df):,}")
    print(df.groupby("platform").size().to_string())


if __name__ == "__main__":
    main()
