# Facet 3 Market Lifespan

Last updated: 2026-06-15

## Purpose

Facet 3 asks whether a market's lifespan helps explain where and when pricing
errors appear.

The analysis turns the large raw trade database into compact CSVs that can be
used remotely for writing and research without downloading the full dataset.

## Analysis

Analysis:
[facet3_lifespan_analysis.py](../src/analysis/comparison/facet3_lifespan_analysis.py)

Run:

```bash
./.venv/bin/python main.py analyze facet3_lifespan_calibration
```

The analysis starts from:

- `data/derived/facet1_enriched_market_dataset.parquet`
- `data/derived/facet1_time_to_expiry_enriched_dataset.parquet`
- `data/kalshi/trades/*.parquet` for Kalshi lifecycle volume concentration
- `data/polymarket/markets/*.parquet` for Polymarket category labels

## Output Files

Generated under `output/` and copied to `research/tables/facet3/`:

- `facet3_lifespan_calibration.csv`
- `facet3_lifespan_category_calibration.csv`
- `facet3_volume_concentration.csv`
- `facet3_late_stage_price_movement.csv`
- `facet3_stale_market_candidates.csv`

Current tracked row counts:

- lifespan calibration rows: `14`
- lifespan x category calibration rows: `141`
- volume concentration rows: `70`
- late-stage movement rows: `456`
- stale candidate rows: `5,000`

## Lifespan Buckets

Markets are bucketed by total open-to-close lifespan:

- `<=1h`
- `1-6h`
- `6-24h`
- `1-7d`
- `7-30d`
- `30-180d`
- `>180d`

## Table Notes

`facet3_lifespan_calibration.csv` recomputes 5-cent bucket calibration inside
each platform and lifespan bucket.

`facet3_lifespan_category_calibration.csv` adds category groups. Kalshi uses
the existing event-ticker taxonomy; Polymarket uses the deterministic category
mapper documented in [FACET1_POLYMARKET_CATEGORIES.md](./FACET1_POLYMARKET_CATEGORIES.md).

`facet3_volume_concentration.csv` is currently Kalshi-only. It maps contract
volume into normalized market-life deciles and reports cumulative volume share.
Kalshi is used first because its trade table has direct prices, timestamps, and
contract counts.

`facet3_late_stage_price_movement.csv` measures how much snapshot prices at
fixed horizons still move before final close.

`facet3_stale_market_candidates.csv` is a compact audit list of markets with
large final staleness, large stale share of total lifespan, or low-liquidity
staleness flags.

## Interpretation Notes

This facet is mainly explanatory. It tests whether errors are concentrated in
very short markets, long-lived quiet markets, or late-stage market states. It
also creates candidate lists for later qualitative inspection.
