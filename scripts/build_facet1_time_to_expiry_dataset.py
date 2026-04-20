"""Build the Phase 1 / Facet 1 time-to-expiry calibration dataset.

This script creates a horizon-snapshot dataset where each row represents one
resolved market observed at a fixed target horizon before close, using the last
trade seen before the horizon cutoff.

Outputs default to `data/derived/`:

- facet1_time_to_expiry_dataset.parquet
- facet1_time_to_expiry_5c_buckets.parquet
- facet1_time_to_expiry_5c_buckets.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_facet1_unified_last_trade_5c_dataset import (
    prepare_polymarket_inputs,
    register_usdc_fpmm_addresses,
)


HORIZONS = [
    ("30d", 720.0, "INTERVAL 30 DAY"),
    ("7d", 168.0, "INTERVAL 7 DAY"),
    ("3d", 72.0, "INTERVAL 3 DAY"),
    ("1d", 24.0, "INTERVAL 1 DAY"),
    ("6h", 6.0, "INTERVAL 6 HOUR"),
    ("1h", 1.0, "INTERVAL 1 HOUR"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/derived"),
        help="Directory where derived datasets will be written.",
    )
    return parser.parse_args()


def prepare_polymarket_market_metadata(con: duckdb.DuckDBPyConnection, base_dir: Path) -> None:
    """Prepare reusable resolved-market metadata without rewriting filtered trades."""
    polymarket_markets = base_dir / "data" / "polymarket" / "markets"

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


def horizon_values_sql(horizons: list[tuple[str, float, str]] | None = None) -> str:
    """Return the SQL VALUES block for the configured horizons."""
    selected_horizons = horizons or HORIZONS
    values = ",\n        ".join(
        f"('{label}', {hours}, {interval_sql})"
        for label, hours, interval_sql in selected_horizons
    )
    return f"""
    SELECT *
    FROM (
        VALUES
        {values}
    ) AS horizons(horizon_label, horizon_hours, horizon_interval)
    """


def wrap_with_horizon_bucket_columns(base_query: str) -> str:
    """Attach time-to-expiry metadata and 5-cent bucket columns."""
    return f"""
SELECT
    platform,
    market_id,
    market_title,
    reference_side,
    reference_label,
    winning_label,
    close_ts,
    horizon_label,
    horizon_hours,
    cutoff_ts,
    snapshot_trade_ts,
    snapshot_trade_source,
    date_diff('second', snapshot_trade_ts, close_ts) / 3600.0 AS actual_hours_before_close,
    (date_diff('second', snapshot_trade_ts, close_ts) / 3600.0) - horizon_hours AS hours_before_close_gap,
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
  AND snapshot_trade_ts <= cutoff_ts
"""


def build_kalshi_chunk_query(
    base_dir: Path,
    chunk_count: int,
    chunk_index: int,
    horizons: list[tuple[str, float, str]] | None = None,
) -> str:
    """Return a chunked Kalshi query with one pre-cutoff row per market and horizon."""
    kalshi_markets = base_dir / "data" / "kalshi" / "markets"
    kalshi_trades = base_dir / "data" / "kalshi" / "trades"

    base_query = f"""
WITH horizons AS (
    {horizon_values_sql(horizons)}
),
kalshi_resolved_markets AS (
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
market_horizons AS (
    SELECT
        m.platform,
        m.market_id,
        m.market_title,
        m.reference_side,
        m.reference_label,
        m.winning_label,
        m.close_ts,
        h.horizon_label,
        h.horizon_hours,
        m.close_ts - h.horizon_interval AS cutoff_ts,
        m.reference_won
    FROM kalshi_resolved_markets m
    CROSS JOIN horizons h
),
ranked_snapshots AS (
    SELECT
        m.platform,
        m.market_id,
        m.market_title,
        m.reference_side,
        m.reference_label,
        m.winning_label,
        m.close_ts,
        m.horizon_label,
        m.horizon_hours,
        m.cutoff_ts,
        t.created_time AS snapshot_trade_ts,
        'kalshi_trade' AS snapshot_trade_source,
        CAST(t.yes_price AS DOUBLE) AS reference_price_cents,
        m.reference_won,
        ROW_NUMBER() OVER (
            PARTITION BY m.market_id, m.horizon_label
            ORDER BY t.created_time DESC, t.trade_id DESC
        ) AS rn
    FROM market_horizons m
    JOIN '{kalshi_trades}/*.parquet' t
      ON t.ticker = m.market_id
    WHERE t.created_time <= m.cutoff_ts
      AND t.yes_price IS NOT NULL
)
SELECT
    platform,
    market_id,
    market_title,
    reference_side,
    reference_label,
    winning_label,
    close_ts,
    horizon_label,
    horizon_hours,
    cutoff_ts,
    snapshot_trade_ts,
    snapshot_trade_source,
    reference_price_cents,
    reference_won
FROM ranked_snapshots
WHERE rn = 1
"""

    return wrap_with_horizon_bucket_columns(base_query)


def build_polymarket_chunk_query(
    ctf_filtered_path: Path,
    legacy_filtered_path: Path,
    chunk_count: int,
    chunk_index: int,
    horizons: list[tuple[str, float, str]] | None = None,
) -> str:
    """Return a chunked Polymarket query with one pre-cutoff row per market and horizon."""
    base_query = f"""
WITH horizons AS (
    {horizon_values_sql(horizons)}
),
chunk_markets AS (
    SELECT
        *
    FROM polymarket_resolved_markets
    WHERE ABS(HASH(market_id)) % {chunk_count} = {chunk_index}
),
chunk_market_horizons AS (
    SELECT
        m.platform,
        m.market_id,
        m.market_title,
        m.reference_side,
        m.reference_label,
        m.winning_label,
        m.close_ts,
        m.fpmm_address,
        h.horizon_label,
        h.horizon_hours,
        m.close_ts - h.horizon_interval AS cutoff_ts,
        m.reference_won
    FROM chunk_markets m
    CROSS JOIN horizons h
),
chunk_token_horizons AS (
    SELECT
        mh.platform,
        mh.market_id,
        mh.market_title,
        mh.reference_side,
        mh.reference_label,
        mh.winning_label,
        mh.close_ts,
        mh.horizon_label,
        mh.horizon_hours,
        mh.cutoff_ts,
        mh.reference_won,
        tm.token_id,
        tm.is_reference_token
    FROM chunk_market_horizons mh
    JOIN polymarket_token_map tm
      ON mh.market_id = tm.market_id
),
polymarket_ctf_candidates AS (
    SELECT
        cth.platform,
        cth.market_id,
        cth.market_title,
        cth.reference_side,
        cth.reference_label,
        cth.winning_label,
        cth.close_ts,
        cth.horizon_label,
        cth.horizon_hours,
        cth.cutoff_ts,
        t.trade_ts AS snapshot_trade_ts,
        'polymarket_ctf' AS snapshot_trade_source,
        CASE
            WHEN cth.is_reference_token THEN t.traded_price_cents
            ELSE 100.0 - t.traded_price_cents
        END AS reference_price_cents,
        cth.reference_won,
        strftime(t.trade_ts, '%Y-%m-%dT%H:%M:%S.%f')
            || '|'
            || COALESCE(t.transaction_hash, '')
            || '|'
            || lpad(CAST(t.log_index AS VARCHAR), 12, '0') AS snapshot_order_key
    FROM chunk_token_horizons cth
    JOIN '{ctf_filtered_path}' t
      ON t.token_id = cth.token_id
    WHERE t.trade_ts <= cth.cutoff_ts
),
polymarket_ctf_snapshots AS (
    SELECT
        platform,
        market_id,
        market_title,
        reference_side,
        reference_label,
        winning_label,
        close_ts,
        horizon_label,
        horizon_hours,
        cutoff_ts,
        arg_max(snapshot_trade_ts, snapshot_order_key) AS snapshot_trade_ts,
        arg_max(snapshot_trade_source, snapshot_order_key) AS snapshot_trade_source,
        arg_max(reference_price_cents, snapshot_order_key) AS reference_price_cents,
        reference_won,
        max(snapshot_order_key) AS snapshot_order_key
    FROM polymarket_ctf_candidates
    GROUP BY
        platform,
        market_id,
        market_title,
        reference_side,
        reference_label,
        winning_label,
        close_ts,
        horizon_label,
        horizon_hours,
        cutoff_ts,
        reference_won
),
polymarket_legacy_candidates AS (
    SELECT
        mh.platform,
        mh.market_id,
        mh.market_title,
        mh.reference_side,
        mh.reference_label,
        mh.winning_label,
        mh.close_ts,
        mh.horizon_label,
        mh.horizon_hours,
        mh.cutoff_ts,
        t.trade_ts AS snapshot_trade_ts,
        'polymarket_legacy_fpmm' AS snapshot_trade_source,
        CASE
            WHEN t.outcome_index = 0 THEN t.traded_price_cents
            WHEN t.outcome_index = 1 THEN 100.0 - t.traded_price_cents
            ELSE NULL
        END AS reference_price_cents,
        mh.reference_won,
        strftime(t.trade_ts, '%Y-%m-%dT%H:%M:%S.%f')
            || '|'
            || COALESCE(t.transaction_hash, '')
            || '|'
            || lpad(CAST(t.log_index AS VARCHAR), 12, '0') AS snapshot_order_key
    FROM chunk_market_horizons mh
    JOIN usdc_fpmm_addresses u
      ON mh.fpmm_address = u.fpmm_address
    JOIN '{legacy_filtered_path}' t
      ON t.fpmm_address = mh.fpmm_address
    WHERE t.trade_ts <= mh.cutoff_ts
),
polymarket_legacy_snapshots AS (
    SELECT
        platform,
        market_id,
        market_title,
        reference_side,
        reference_label,
        winning_label,
        close_ts,
        horizon_label,
        horizon_hours,
        cutoff_ts,
        arg_max(snapshot_trade_ts, snapshot_order_key) AS snapshot_trade_ts,
        arg_max(snapshot_trade_source, snapshot_order_key) AS snapshot_trade_source,
        arg_max(reference_price_cents, snapshot_order_key) AS reference_price_cents,
        reference_won,
        max(snapshot_order_key) AS snapshot_order_key
    FROM polymarket_legacy_candidates
    WHERE reference_price_cents IS NOT NULL
    GROUP BY
        platform,
        market_id,
        market_title,
        reference_side,
        reference_label,
        winning_label,
        close_ts,
        horizon_label,
        horizon_hours,
        cutoff_ts,
        reference_won
),
all_snapshot_candidates AS (
    SELECT * FROM polymarket_ctf_snapshots

    UNION ALL

    SELECT * FROM polymarket_legacy_snapshots
),
final_snapshots AS (
    SELECT
        platform,
        market_id,
        market_title,
        reference_side,
        reference_label,
        winning_label,
        close_ts,
        horizon_label,
        horizon_hours,
        cutoff_ts,
        arg_max(snapshot_trade_ts, snapshot_order_key) AS snapshot_trade_ts,
        arg_max(snapshot_trade_source, snapshot_order_key) AS snapshot_trade_source,
        arg_max(reference_price_cents, snapshot_order_key) AS reference_price_cents,
        reference_won
    FROM all_snapshot_candidates
    WHERE reference_price_cents IS NOT NULL
      AND reference_price_cents >= 0
      AND reference_price_cents <= 100
    GROUP BY
        platform,
        market_id,
        market_title,
        reference_side,
        reference_label,
        winning_label,
        close_ts,
        horizon_label,
        horizon_hours,
        cutoff_ts,
        reference_won
)
SELECT
    platform,
    market_id,
    market_title,
    reference_side,
    reference_label,
    winning_label,
    close_ts,
    horizon_label,
    horizon_hours,
    cutoff_ts,
    snapshot_trade_ts,
    snapshot_trade_source,
    reference_price_cents,
    reference_won
FROM final_snapshots
"""

    return wrap_with_horizon_bucket_columns(base_query)


def build_bucket_summary_query() -> str:
    """Return the SQL query that aggregates the 5-cent bucket summary."""
    return """
SELECT
    platform,
    horizon_label,
    horizon_hours,
    price_bucket_5c_floor,
    price_bucket_5c_mid,
    price_bucket_5c_label,
    COUNT(*) AS market_count,
    SUM(reference_won) AS wins,
    AVG(reference_won) AS empirical_win_rate,
    AVG(reference_price_cents) / 100.0 AS avg_implied_probability,
    AVG(reference_won) - (AVG(reference_price_cents) / 100.0) AS calibration_gap,
    AVG(actual_hours_before_close) AS avg_actual_hours_before_close
FROM horizon_snapshot_dataset
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY platform, horizon_hours DESC, price_bucket_5c_floor
"""


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent.parent
    output_dir = (base_dir / args.output_dir).resolve() if not args.output_dir.is_absolute() else args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    spill_dir = output_dir / ".duckdb_spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    chunk_dir = output_dir / ".facet1_time_to_expiry_chunks"
    kalshi_chunk_dir = chunk_dir / "kalshi"
    polymarket_chunk_dir = chunk_dir / "polymarket"
    ctf_filtered_path = chunk_dir / "polymarket_ctf_relevant.parquet"
    legacy_filtered_path = chunk_dir / "polymarket_legacy_relevant.parquet"
    shared_chunk_dir = output_dir / ".facet1_chunks"
    shared_ctf_filtered_path = shared_chunk_dir / "polymarket_ctf_relevant.parquet"
    shared_legacy_filtered_path = shared_chunk_dir / "polymarket_legacy_relevant.parquet"
    kalshi_chunk_dir.mkdir(parents=True, exist_ok=True)
    polymarket_chunk_dir.mkdir(parents=True, exist_ok=True)

    row_output_path = output_dir / "facet1_time_to_expiry_dataset.parquet"
    bucket_output_path = output_dir / "facet1_time_to_expiry_5c_buckets.parquet"
    bucket_csv_output_path = output_dir / "facet1_time_to_expiry_5c_buckets.csv"

    for path in (row_output_path, bucket_output_path, bucket_csv_output_path):
        if path.exists():
            path.unlink()
    for stray_path in chunk_dir.glob("*.parquet"):
        if stray_path.exists():
            stray_path.unlink()
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
    if shared_ctf_filtered_path.exists() and shared_legacy_filtered_path.exists():
        print("Reusing existing filtered Polymarket inputs from data/derived/.facet1_chunks ...")
        prepare_polymarket_market_metadata(con, base_dir)
        ctf_filtered_path = shared_ctf_filtered_path
        legacy_filtered_path = shared_legacy_filtered_path
    else:
        print("Preparing reusable Polymarket inputs...")
        prepare_polymarket_inputs(con, base_dir, ctf_filtered_path, legacy_filtered_path, chunk_dir)

    kalshi_chunks = 8
    polymarket_chunks = 32

    for chunk_index in range(kalshi_chunks):
        chunk_path = kalshi_chunk_dir / f"kalshi_chunk_{chunk_index:02d}.parquet"
        chunk_query = build_kalshi_chunk_query(base_dir, kalshi_chunks, chunk_index)
        print(f"Writing Kalshi horizon chunk {chunk_index + 1}/{kalshi_chunks} ...")
        con.execute(f"COPY ({chunk_query}) TO '{chunk_path}' (FORMAT PARQUET)")

    for chunk_index in range(polymarket_chunks):
        chunk_path = polymarket_chunk_dir / f"polymarket_chunk_{chunk_index:02d}.parquet"
        chunk_query = build_polymarket_chunk_query(
            ctf_filtered_path,
            legacy_filtered_path,
            polymarket_chunks,
            chunk_index,
        )
        print(f"Writing Polymarket horizon chunk {chunk_index + 1}/{polymarket_chunks} ...")
        con.execute(f"COPY ({chunk_query}) TO '{chunk_path}' (FORMAT PARQUET)")

    print("Merging chunked outputs into horizon dataset...")
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
            COUNT(DISTINCT market_id) AS distinct_markets,
            SUM(CASE WHEN platform = 'kalshi' THEN 1 ELSE 0 END) AS kalshi_rows,
            SUM(CASE WHEN platform = 'polymarket' THEN 1 ELSE 0 END) AS polymarket_rows,
            MIN(horizon_hours) AS min_horizon_hours,
            MAX(horizon_hours) AS max_horizon_hours
        FROM '{row_output_path}'
        """
    ).fetchone()
    print(
        "Time-to-expiry dataset rows: "
        f"{dataset_stats[0]:,} total across {dataset_stats[1]:,} distinct markets "
        f"({dataset_stats[2]:,} Kalshi, {dataset_stats[3]:,} Polymarket); "
        f"horizons {dataset_stats[4]:.1f}h to {dataset_stats[5]:.1f}h"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW horizon_snapshot_dataset AS
        SELECT *
        FROM '{row_output_path}'
        """
    )
    bucket_summary_query = build_bucket_summary_query()
    con.execute(f"COPY ({bucket_summary_query}) TO '{bucket_output_path}' (FORMAT PARQUET)")
    con.execute(f"COPY ({bucket_summary_query}) TO '{bucket_csv_output_path}' (HEADER, DELIMITER ',')")

    bucket_stats = con.execute(
        f"""
        SELECT COUNT(*) AS rows
        FROM '{bucket_output_path}'
        """
    ).fetchone()
    print(f"Time-to-expiry bucket rows: {bucket_stats[0]:,}")


if __name__ == "__main__":
    main()
