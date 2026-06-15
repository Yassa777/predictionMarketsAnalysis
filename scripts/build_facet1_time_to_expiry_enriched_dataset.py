"""Build the enriched horizon-level Facet 1 dataset with staleness controls.

This script joins the existing time-to-expiry snapshot dataset to the market-
level enrichment table so downstream freshness and category-control analyses can
share one consistent horizon table.

Outputs default to `data/derived/`:

- facet1_time_to_expiry_enriched_dataset.parquet
"""

from __future__ import annotations

import argparse
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


def build_metadata_table(con: duckdb.DuckDBPyConnection, market_dataset_path: Path) -> None:
    """Prepare a one-row-per-market metadata table for the horizon join."""
    duplicate_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT platform, market_id
            FROM '{market_dataset_path}'
            GROUP BY 1, 2
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_count:
        raise AssertionError(
            f"Enriched market dataset has {duplicate_count} duplicated (platform, market_id) rows."
        )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE facet1_market_metadata AS
        SELECT
            platform,
            market_id,
            ANY_VALUE(event_ticker) AS event_ticker,
            ANY_VALUE(category_raw) AS category_raw,
            ANY_VALUE(category_group) AS category_group,
            ANY_VALUE(category_mid) AS category_mid,
            ANY_VALUE(category_subcategory) AS category_subcategory,
            ANY_VALUE(close_month) AS close_month,
            ANY_VALUE(lifespan_hours) AS lifespan_hours,
            ANY_VALUE(primary_liquidity_metric_name) AS primary_liquidity_metric_name,
            ANY_VALUE(secondary_liquidity_metric_name) AS secondary_liquidity_metric_name,
            ANY_VALUE(primary_liquidity_metric) AS primary_liquidity_metric,
            ANY_VALUE(secondary_liquidity_metric) AS secondary_liquidity_metric,
            ANY_VALUE(liquidity_quintile) AS liquidity_quintile,
            ANY_VALUE(liquidity_quintile_label) AS liquidity_quintile_label
        FROM '{market_dataset_path}'
        GROUP BY 1, 2
        """
    )


def build_enriched_dataset(
    con: duckdb.DuckDBPyConnection,
    time_dataset_path: Path,
    market_dataset_path: Path,
    output_path: Path,
) -> None:
    """Join horizon rows to market metadata and materialize freshness fields."""
    build_metadata_table(con, market_dataset_path)

    con.execute(
        f"""
        COPY (
            SELECT
                horizons.*,
                horizons.hours_before_close_gap AS staleness_age_hours,
                horizons.hours_before_close_gap / 24.0 AS staleness_age_days,
                CASE
                    WHEN horizons.hours_before_close_gap <= 1.0 THEN '<=1h'
                    WHEN horizons.hours_before_close_gap <= 6.0 THEN '1-6h'
                    WHEN horizons.hours_before_close_gap <= 24.0 THEN '6-24h'
                    WHEN horizons.hours_before_close_gap <= 168.0 THEN '1-7d'
                    ELSE '>7d'
                END AS staleness_bucket,
                horizons.hours_before_close_gap <= 1.0 AS is_fresh_1h,
                horizons.hours_before_close_gap <= 6.0 AS is_fresh_6h,
                horizons.hours_before_close_gap <= 24.0 AS is_fresh_24h,
                horizons.actual_hours_before_close / horizons.horizon_hours AS freshness_ratio,
                metadata.event_ticker,
                metadata.category_raw,
                metadata.category_group,
                metadata.category_mid,
                metadata.category_subcategory,
                metadata.close_month,
                metadata.lifespan_hours,
                metadata.primary_liquidity_metric_name,
                metadata.secondary_liquidity_metric_name,
                metadata.primary_liquidity_metric,
                metadata.secondary_liquidity_metric,
                metadata.liquidity_quintile,
                metadata.liquidity_quintile_label
            FROM '{time_dataset_path}' AS horizons
            LEFT JOIN facet1_market_metadata AS metadata
                USING (platform, market_id)
        ) TO '{output_path}' (FORMAT PARQUET)
        """
    )


def validate_dataset(
    con: duckdb.DuckDBPyConnection,
    time_dataset_path: Path,
    output_path: Path,
) -> None:
    """Validate the enriched horizon dataset against the requested acceptance checks."""
    source_rows = con.execute(f"SELECT COUNT(*) FROM '{time_dataset_path}'").fetchone()[0]
    output_rows = con.execute(f"SELECT COUNT(*) FROM '{output_path}'").fetchone()[0]
    if output_rows != source_rows:
        raise AssertionError(f"Row count mismatch: source={source_rows:,}, output={output_rows:,}")

    negative_staleness_rows = con.execute(
        f"SELECT COUNT(*) FROM '{output_path}' WHERE staleness_age_hours < 0"
    ).fetchone()[0]
    if negative_staleness_rows:
        raise AssertionError(f"Found {negative_staleness_rows:,} rows with negative staleness_age_hours.")

    unequal_gap_rows = con.execute(
        f"""
        SELECT COUNT(*)
        FROM '{output_path}'
        WHERE staleness_age_hours != hours_before_close_gap
        """
    ).fetchone()[0]
    if unequal_gap_rows:
        raise AssertionError(
            f"Found {unequal_gap_rows:,} rows where staleness_age_hours != hours_before_close_gap."
        )

    freshness_flag_violations = con.execute(
        f"""
        SELECT COUNT(*)
        FROM '{output_path}'
        WHERE (is_fresh_1h AND NOT is_fresh_6h)
           OR (is_fresh_6h AND NOT is_fresh_24h)
        """
    ).fetchone()[0]
    if freshness_flag_violations:
        raise AssertionError(f"Found {freshness_flag_violations:,} non-monotone freshness flag rows.")

    horizon_count_mismatches = con.execute(
        f"""
        WITH source_counts AS (
            SELECT platform, horizon_label, COUNT(*) AS row_count
            FROM '{time_dataset_path}'
            GROUP BY 1, 2
        ),
        output_counts AS (
            SELECT platform, horizon_label, COUNT(*) AS row_count
            FROM '{output_path}'
            GROUP BY 1, 2
        )
        SELECT COUNT(*)
        FROM (
            SELECT * FROM source_counts
            EXCEPT
            SELECT * FROM output_counts

            UNION ALL

            SELECT * FROM output_counts
            EXCEPT
            SELECT * FROM source_counts
        )
        """
    ).fetchone()[0]
    if horizon_count_mismatches:
        raise AssertionError(
            f"Found {horizon_count_mismatches:,} platform-by-horizon count mismatches after enrichment."
        )

    missing_metadata_rows = con.execute(
        f"""
        SELECT COUNT(*)
        FROM '{output_path}'
        WHERE close_month IS NULL
           OR liquidity_quintile IS NULL
        """
    ).fetchone()[0]
    if missing_metadata_rows:
        raise AssertionError(f"Found {missing_metadata_rows:,} rows missing joined market metadata.")

    missing_kalshi_category_rows = con.execute(
        f"""
        SELECT COUNT(*)
        FROM '{output_path}'
        WHERE platform = 'kalshi'
          AND category_group IS NULL
        """
    ).fetchone()[0]
    if missing_kalshi_category_rows:
        raise AssertionError(
            f"Found {missing_kalshi_category_rows:,} Kalshi rows without category_group."
        )


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    time_dataset_path = output_dir / "facet1_time_to_expiry_dataset.parquet"
    market_dataset_path = output_dir / "facet1_enriched_market_dataset.parquet"
    output_path = output_dir / "facet1_time_to_expiry_enriched_dataset.parquet"

    if not time_dataset_path.exists():
        raise FileNotFoundError(
            f"Time-to-expiry dataset not found: {time_dataset_path}. "
            "Run scripts/build_facet1_time_to_expiry_dataset.py first."
        )
    if not market_dataset_path.exists():
        raise FileNotFoundError(
            f"Enriched market dataset not found: {market_dataset_path}. "
            "Run scripts/build_facet1_enriched_market_dataset.py first."
        )

    con = duckdb.connect()
    build_enriched_dataset(con, time_dataset_path, market_dataset_path, output_path)
    validate_dataset(con, time_dataset_path, output_path)

    row_count = con.execute(f"SELECT COUNT(*) FROM '{output_path}'").fetchone()[0]
    print(f"Wrote enriched horizon dataset: {output_path}")
    print(f"Rows: {row_count:,}")
    print(
        con.execute(
            f"""
            SELECT platform, horizon_label, COUNT(*) AS row_count
            FROM '{output_path}'
            GROUP BY 1, 2
            ORDER BY
                platform,
                CASE horizon_label
                    WHEN '30d' THEN 1
                    WHEN '7d' THEN 2
                    WHEN '3d' THEN 3
                    WHEN '1d' THEN 4
                    WHEN '6h' THEN 5
                    WHEN '1h' THEN 6
                    ELSE 999
                END
            """
        ).df().to_string(index=False)
    )


if __name__ == "__main__":
    main()
