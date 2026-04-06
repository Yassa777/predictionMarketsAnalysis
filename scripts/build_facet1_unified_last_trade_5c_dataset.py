"""Build the unified Phase 1 / Facet 1 last-trade calibration dataset.

This script creates two derived datasets:

1. A market-level unified dataset with one row per market using the last trade
   observed before market close.
2. A 5-cent bucket summary for calibration analysis.

Outputs default to `data/derived/`.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/derived"),
        help="Directory where derived datasets will be written.",
    )
    return parser.parse_args()


def register_usdc_fpmm_addresses(con: duckdb.DuckDBPyConnection, lookup_path: Path) -> int:
    """Register legacy FPMM addresses that use USDC collateral."""
    con.execute("CREATE OR REPLACE TEMP TABLE usdc_fpmm_addresses (fpmm_address VARCHAR)")

    if not lookup_path.exists():
        return 0

    with lookup_path.open() as f:
        collateral_lookup = json.load(f)

    addresses = [
        (address.lower(),)
        for address, info in collateral_lookup.items()
        if info.get("collateral_symbol") == "USDC"
    ]

    if addresses:
        con.executemany("INSERT INTO usdc_fpmm_addresses VALUES (?)", addresses)

    return len(addresses)


def prepare_polymarket_inputs(
    con: duckdb.DuckDBPyConnection,
    base_dir: Path,
    ctf_filtered_path: Path,
    legacy_filtered_path: Path,
    chunk_dir: Path,
) -> None:
    """Prepare reusable Polymarket market metadata and filtered trade shards."""
    polymarket_markets = base_dir / "data" / "polymarket" / "markets"
    polymarket_trades = base_dir / "data" / "polymarket" / "trades"
    polymarket_legacy_trades = base_dir / "data" / "polymarket" / "legacy_trades"
    polymarket_blocks = base_dir / "data" / "polymarket" / "blocks"

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE polymarket_resolved_markets AS
        SELECT
            'polymarket' AS platform,
            id AS market_id,
            question AS market_title,
            'outcome_0' AS reference_side,
            json_extract_string(outcomes, '$[0]') AS reference_label,
            CASE
                WHEN CAST(json_extract_string(outcome_prices, '$[0]') AS DOUBLE)
                   > CAST(json_extract_string(outcome_prices, '$[1]') AS DOUBLE)
                THEN json_extract_string(outcomes, '$[0]')
                ELSE json_extract_string(outcomes, '$[1]')
            END AS winning_label,
            end_date AS close_ts,
            json_extract_string(clob_token_ids, '$[0]') AS token_0,
            json_extract_string(clob_token_ids, '$[1]') AS token_1,
            LOWER(market_maker_address) AS fpmm_address,
            CASE
                WHEN CAST(json_extract_string(outcome_prices, '$[0]') AS DOUBLE)
                   > CAST(json_extract_string(outcome_prices, '$[1]') AS DOUBLE)
                THEN 1 ELSE 0
            END AS reference_won
        FROM '{polymarket_markets}/*.parquet'
        WHERE closed = true
          AND end_date IS NOT NULL
          AND json_array_length(outcomes) = 2
          AND json_array_length(outcome_prices) = 2
          AND json_array_length(clob_token_ids) = 2
          AND greatest(
                CAST(json_extract_string(outcome_prices, '$[0]') AS DOUBLE),
                CAST(json_extract_string(outcome_prices, '$[1]') AS DOUBLE)
              ) > 0.99
          AND least(
                CAST(json_extract_string(outcome_prices, '$[0]') AS DOUBLE),
                CAST(json_extract_string(outcome_prices, '$[1]') AS DOUBLE)
              ) < 0.01
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE polymarket_token_map AS
        SELECT
            market_id,
            market_title,
            reference_side,
            reference_label,
            winning_label,
            close_ts,
            reference_won,
            token_0 AS token_id,
            TRUE AS is_reference_token,
            fpmm_address
        FROM polymarket_resolved_markets

        UNION ALL

        SELECT
            market_id,
            market_title,
            reference_side,
            reference_label,
            winning_label,
            close_ts,
            reference_won,
            token_1 AS token_id,
            FALSE AS is_reference_token,
            fpmm_address
        FROM polymarket_resolved_markets
        """
    )

    if ctf_filtered_path.exists():
        ctf_filtered_path.unlink()
    if legacy_filtered_path.exists():
        legacy_filtered_path.unlink()

    ctf_staging_path = chunk_dir / "polymarket_ctf_prefilter_raw.parquet"
    if ctf_staging_path.exists():
        ctf_staging_path.unlink()

    print("Filtering current Polymarket trades to relevant tokens...")
    con.execute(
        f"""
        COPY (
            SELECT
                t.taker_asset_id AS token_id,
                t.block_number,
                t.transaction_hash,
                t.log_index,
                t.timestamp,
                100.0 * t.maker_amount::DOUBLE / t.taker_amount::DOUBLE AS traded_price_cents
            FROM '{polymarket_trades}/*.parquet' t
            JOIN (SELECT DISTINCT token_id FROM polymarket_token_map) tokens
              ON t.maker_asset_id = '0' AND t.taker_asset_id = tokens.token_id
            WHERE t.maker_amount > 0
              AND t.taker_amount > 0

            UNION ALL

            SELECT
                t.maker_asset_id AS token_id,
                t.block_number,
                t.transaction_hash,
                t.log_index,
                t.timestamp,
                100.0 * t.taker_amount::DOUBLE / t.maker_amount::DOUBLE AS traded_price_cents
            FROM '{polymarket_trades}/*.parquet' t
            JOIN (SELECT DISTINCT token_id FROM polymarket_token_map) tokens
              ON t.taker_asset_id = '0' AND t.maker_asset_id = tokens.token_id
            WHERE t.maker_amount > 0
              AND t.taker_amount > 0
        ) TO '{ctf_staging_path}' (FORMAT PARQUET)
        """
    )

    print("Resolving timestamps for filtered current-trade shards...")
    con.execute(
        f"""
        COPY (
            WITH relevant_blocks AS (
                SELECT DISTINCT block_number
                FROM '{ctf_staging_path}'
            ),
            block_lookup AS (
                SELECT
                    rb.block_number,
                    TRY_CAST(b.timestamp AS TIMESTAMPTZ) AS trade_ts
                FROM relevant_blocks rb
                JOIN '{polymarket_blocks}/*.parquet' b USING (block_number)
            )
            SELECT
                s.token_id,
                COALESCE(to_timestamp(s.timestamp), bl.trade_ts) AS trade_ts,
                s.transaction_hash,
                s.log_index,
                s.traded_price_cents
            FROM '{ctf_staging_path}' s
            LEFT JOIN block_lookup bl USING (block_number)
            WHERE COALESCE(to_timestamp(s.timestamp), bl.trade_ts) IS NOT NULL
        ) TO '{ctf_filtered_path}' (FORMAT PARQUET)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT
                LOWER(t.fpmm_address) AS fpmm_address,
                COALESCE(to_timestamp(t.timestamp), TRY_CAST(b.timestamp AS TIMESTAMPTZ)) AS trade_ts,
                t.transaction_hash,
                t.log_index,
                t.outcome_index,
                100.0 * t.amount::DOUBLE / t.outcome_tokens::DOUBLE AS traded_price_cents
            FROM '{polymarket_legacy_trades}/*.parquet' t
            JOIN usdc_fpmm_addresses u
              ON LOWER(t.fpmm_address) = u.fpmm_address
            LEFT JOIN '{polymarket_blocks}/*.parquet' b USING (block_number)
            WHERE t.amount::DOUBLE > 0
              AND t.outcome_tokens::DOUBLE > 0
              AND COALESCE(to_timestamp(t.timestamp), TRY_CAST(b.timestamp AS TIMESTAMPTZ)) IS NOT NULL
        ) TO '{legacy_filtered_path}' (FORMAT PARQUET)
        """
    )


def wrap_with_bucket_columns(base_query: str) -> str:
    """Attach 5-cent bucket columns to a base market-level query."""
    return f"""
SELECT
    platform,
    market_id,
    market_title,
    reference_side,
    reference_label,
    winning_label,
    close_ts,
    last_trade_ts,
    last_trade_source,
    reference_price_cents,
    reference_won,
    CAST(FLOOR(LEAST(reference_price_cents, 99.999999) / 5.0) * 5 AS INTEGER) AS price_bucket_5c_floor,
    CAST(FLOOR(LEAST(reference_price_cents, 99.999999) / 5.0) * 5 + 2.5 AS DOUBLE) AS price_bucket_5c_mid,
    CAST(FLOOR(LEAST(reference_price_cents, 99.999999) / 5.0) * 5 AS INTEGER)::VARCHAR
        || '-'
        || CAST(FLOOR(LEAST(reference_price_cents, 99.999999) / 5.0) * 5 + 5 AS INTEGER)::VARCHAR
        AS price_bucket_5c_label
FROM (
    {base_query}
) base
WHERE reference_price_cents IS NOT NULL
  AND reference_price_cents >= 0
  AND reference_price_cents <= 100
"""


def build_kalshi_chunk_query(base_dir: Path, chunk_count: int, chunk_index: int) -> str:
    """Return a chunked Kalshi query with one last-trade row per market."""
    kalshi_markets = base_dir / "data" / "kalshi" / "markets"
    kalshi_trades = base_dir / "data" / "kalshi" / "trades"

    base_query = f"""
WITH kalshi_resolved_markets AS (
    SELECT
        'kalshi' AS platform,
        ticker AS market_id,
        title AS market_title,
        'yes' AS reference_side,
        'Yes' AS reference_label,
        CASE WHEN result = 'yes' THEN 'Yes' ELSE 'No' END AS winning_label,
        close_time AS close_ts,
        CASE WHEN result = 'yes' THEN 1 ELSE 0 END AS reference_won
    FROM '{kalshi_markets}/*.parquet'
    WHERE result IN ('yes', 'no')
      AND close_time IS NOT NULL
      AND ABS(HASH(ticker)) % {chunk_count} = {chunk_index}
),
kalshi_last_trades AS (
    SELECT
        m.platform,
        m.market_id,
        m.market_title,
        m.reference_side,
        m.reference_label,
        m.winning_label,
        m.close_ts,
        t.created_time AS last_trade_ts,
        'kalshi_trade' AS last_trade_source,
        CAST(t.yes_price AS DOUBLE) AS reference_price_cents,
        m.reference_won,
        ROW_NUMBER() OVER (
            PARTITION BY m.market_id
            ORDER BY t.created_time DESC, t.trade_id DESC
        ) AS rn
    FROM kalshi_resolved_markets m
    JOIN '{kalshi_trades}/*.parquet' t
      ON t.ticker = m.market_id
    WHERE t.created_time <= m.close_ts
      AND t.yes_price IS NOT NULL
),
ranked_kalshi AS (
    SELECT
        market_id,
        market_title,
        reference_side,
        reference_label,
        winning_label,
        close_ts,
        platform,
        last_trade_ts,
        last_trade_source,
        reference_price_cents,
        reference_won,
        ROW_NUMBER() OVER (
            PARTITION BY market_id
            ORDER BY last_trade_ts DESC, last_trade_source DESC
        ) AS rn
    FROM kalshi_last_trades
)
SELECT
    platform,
    market_id,
    market_title,
    reference_side,
    reference_label,
    winning_label,
    close_ts,
    last_trade_ts,
    last_trade_source,
    reference_price_cents,
    reference_won
FROM ranked_kalshi
WHERE rn = 1
"""

    return wrap_with_bucket_columns(base_query)


def build_polymarket_chunk_query(
    ctf_filtered_path: Path,
    legacy_filtered_path: Path,
    chunk_count: int,
    chunk_index: int,
) -> str:
    """Return a chunked Polymarket query with one last-trade row per market."""

    base_query = f"""
WITH chunk_markets AS (
    SELECT
        *
    FROM polymarket_resolved_markets
    WHERE ABS(HASH(market_id)) % {chunk_count} = {chunk_index}
),
chunk_token_map AS (
    SELECT
        tmap.market_id,
        tmap.market_title,
        tmap.reference_side,
        tmap.reference_label,
        tmap.winning_label,
        tmap.close_ts,
        tmap.reference_won,
        tmap.token_id,
        tmap.is_reference_token
    FROM polymarket_token_map tmap
    JOIN chunk_markets m USING (market_id)
),
polymarket_ctf_last_trades AS (
    SELECT
        platform,
        market_id,
        market_title,
        reference_side,
        reference_label,
        winning_label,
        close_ts,
        last_trade_ts,
        last_trade_source,
        reference_price_cents,
        reference_won
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY market_id
                ORDER BY
                    last_trade_ts DESC,
                    trade_hash DESC,
                    trade_log_index DESC
            ) AS rn
        FROM (
            SELECT
                'polymarket' AS platform,
                m.market_id,
                m.market_title,
                m.reference_side,
                m.reference_label,
                m.winning_label,
                m.close_ts,
                t.trade_ts AS last_trade_ts,
                'polymarket_ctf' AS last_trade_source,
                CASE
                    WHEN m.is_reference_token THEN t.traded_price_cents
                    ELSE 100.0 - t.traded_price_cents
                END AS reference_price_cents,
                m.reference_won,
                t.transaction_hash AS trade_hash,
                t.log_index AS trade_log_index
            FROM chunk_token_map m
            JOIN '{ctf_filtered_path}' t
              ON t.token_id = m.token_id
            WHERE t.trade_ts <= m.close_ts
        )
    )
    WHERE rn = 1
),
polymarket_legacy_last_trades AS (
    SELECT
        platform,
        market_id,
        market_title,
        reference_side,
        reference_label,
        winning_label,
        close_ts,
        last_trade_ts,
        last_trade_source,
        reference_price_cents,
        reference_won
    FROM (
        SELECT
            'polymarket' AS platform,
            m.market_id,
            m.market_title,
            m.reference_side,
            m.reference_label,
            m.winning_label,
            m.close_ts,
            t.trade_ts AS last_trade_ts,
            'polymarket_legacy_fpmm' AS last_trade_source,
            CASE
                WHEN t.outcome_index = 0 THEN t.traded_price_cents
                WHEN t.outcome_index = 1 THEN 100.0 - t.traded_price_cents
                ELSE NULL
            END AS reference_price_cents,
            m.reference_won,
            ROW_NUMBER() OVER (
                PARTITION BY m.market_id
                ORDER BY
                    t.trade_ts DESC,
                    t.transaction_hash DESC,
                    t.log_index DESC
            ) AS rn
        FROM chunk_markets m
        JOIN usdc_fpmm_addresses u
          ON m.fpmm_address = u.fpmm_address
        JOIN '{legacy_filtered_path}' t
          ON t.fpmm_address = m.fpmm_address
        WHERE t.trade_ts <= m.close_ts
    )
    WHERE rn = 1
),
all_last_trade_candidates AS (
    SELECT * FROM polymarket_ctf_last_trades

    UNION ALL

    SELECT * FROM polymarket_legacy_last_trades
),
ranked_unified AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY platform, market_id
            ORDER BY last_trade_ts DESC, last_trade_source DESC
        ) AS rn
    FROM all_last_trade_candidates
    WHERE reference_price_cents IS NOT NULL
      AND reference_price_cents >= 0
      AND reference_price_cents <= 100
)
SELECT
    platform,
    market_id,
    market_title,
    reference_side,
    reference_label,
    winning_label,
    close_ts,
    last_trade_ts,
    last_trade_source,
    reference_price_cents,
    reference_won
FROM ranked_unified
WHERE rn = 1
"""

    return wrap_with_bucket_columns(base_query)


def build_bucket_summary_query() -> str:
    """Return the SQL query that aggregates the 5-cent bucket summary."""
    return """
SELECT
    platform,
    reference_side,
    price_bucket_5c_floor,
    price_bucket_5c_mid,
    price_bucket_5c_label,
    COUNT(*) AS market_count,
    SUM(reference_won) AS wins,
    AVG(reference_won) AS empirical_win_rate,
    AVG(reference_price_cents) / 100.0 AS avg_implied_probability,
    AVG(reference_won) - (AVG(reference_price_cents) / 100.0) AS calibration_gap
FROM unified_last_trade_dataset
GROUP BY 1, 2, 3, 4, 5
ORDER BY platform, price_bucket_5c_floor
"""


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent.parent
    output_dir = (base_dir / args.output_dir).resolve() if not args.output_dir.is_absolute() else args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    spill_dir = output_dir / ".duckdb_spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    chunk_dir = output_dir / ".facet1_chunks"
    kalshi_chunk_dir = chunk_dir / "kalshi"
    polymarket_chunk_dir = chunk_dir / "polymarket"
    ctf_filtered_path = chunk_dir / "polymarket_ctf_relevant.parquet"
    legacy_filtered_path = chunk_dir / "polymarket_legacy_relevant.parquet"
    kalshi_chunk_dir.mkdir(parents=True, exist_ok=True)
    polymarket_chunk_dir.mkdir(parents=True, exist_ok=True)

    row_output_path = output_dir / "facet1_unified_last_trade_dataset.parquet"
    bucket_output_path = output_dir / "facet1_unified_last_trade_5c_buckets.parquet"
    bucket_csv_output_path = output_dir / "facet1_unified_last_trade_5c_buckets.csv"

    for path in (row_output_path, bucket_output_path, bucket_csv_output_path):
        if path.exists():
            path.unlink()
    for chunk_path in (
        list(kalshi_chunk_dir.glob("*.parquet"))
        + list(polymarket_chunk_dir.glob("*.parquet"))
        + [ctf_filtered_path, legacy_filtered_path]
    ):
        if chunk_path.exists():
            chunk_path.unlink()

    con = duckdb.connect()
    con.execute("PRAGMA threads=2")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{spill_dir}'")
    con.execute("SET memory_limit = '8GB'")
    con.execute("SET max_temp_directory_size = '45GiB'")

    lookup_path = base_dir / "data" / "polymarket" / "fpmm_collateral_lookup.json"
    usdc_count = register_usdc_fpmm_addresses(con, lookup_path)
    print(f"Registered {usdc_count:,} USDC-collateralized legacy FPMM addresses")
    print("Preparing reusable Polymarket inputs...")
    prepare_polymarket_inputs(con, base_dir, ctf_filtered_path, legacy_filtered_path, chunk_dir)

    kalshi_chunks = 8
    polymarket_chunks = 32

    for chunk_index in range(kalshi_chunks):
        chunk_path = kalshi_chunk_dir / f"kalshi_chunk_{chunk_index:02d}.parquet"
        chunk_query = build_kalshi_chunk_query(base_dir, kalshi_chunks, chunk_index)
        print(f"Writing Kalshi chunk {chunk_index + 1}/{kalshi_chunks} ...")
        con.execute(f"COPY ({chunk_query}) TO '{chunk_path}' (FORMAT PARQUET)")

    for chunk_index in range(polymarket_chunks):
        chunk_path = polymarket_chunk_dir / f"polymarket_chunk_{chunk_index:02d}.parquet"
        chunk_query = build_polymarket_chunk_query(ctf_filtered_path, legacy_filtered_path, polymarket_chunks, chunk_index)
        print(f"Writing Polymarket chunk {chunk_index + 1}/{polymarket_chunks} ...")
        con.execute(f"COPY ({chunk_query}) TO '{chunk_path}' (FORMAT PARQUET)")

    print("Merging chunked outputs into unified dataset...")
    con.execute(
        f"""
        COPY (
            SELECT * FROM '{kalshi_chunk_dir}/*.parquet'
            UNION ALL
            SELECT * FROM '{polymarket_chunk_dir}/*.parquet'
        ) TO '{row_output_path}' (FORMAT PARQUET)
        """
    )

    dataset_stats = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            SUM(CASE WHEN platform = 'kalshi' THEN 1 ELSE 0 END) AS kalshi_rows,
            SUM(CASE WHEN platform = 'polymarket' THEN 1 ELSE 0 END) AS polymarket_rows,
            MIN(reference_price_cents) AS min_price_cents,
            MAX(reference_price_cents) AS max_price_cents
        FROM '{row_output_path}'
        """
    ).fetchone()
    print(
        "Unified dataset rows: "
        f"{dataset_stats[0]:,} total "
        f"({dataset_stats[1]:,} Kalshi, {dataset_stats[2]:,} Polymarket); "
        f"price range {dataset_stats[3]:.4f}c to {dataset_stats[4]:.4f}c"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW unified_last_trade_dataset AS
        SELECT *
        FROM '{row_output_path}'
        """
    )

    bucket_summary_query = build_bucket_summary_query()
    print("Writing 5-cent bucket summary...")
    con.execute(f"COPY ({bucket_summary_query}) TO '{bucket_output_path}' (FORMAT PARQUET)")

    print(f"Writing {bucket_csv_output_path} ...")
    con.execute(f"COPY ({bucket_summary_query}) TO '{bucket_csv_output_path}' (HEADER, DELIMITER ',')")

    bucket_count = con.execute(f"SELECT COUNT(*) FROM '{bucket_output_path}'").fetchone()[0]
    print(f"Bucket summary rows: {bucket_count:,}")
    print("Done.")


if __name__ == "__main__":
    main()
