# Facet 1 Slice Analyses

Last updated: 2026-04-06

## Purpose

This document records the methodology and current results for the next Phase 1 / Facet 1 slices built on top of the unified last-trade market dataset:

- monthly drift
- liquidity
- Kalshi category groups

It also documents the enriched market-level dataset that makes these slices reusable.

## Enriched Market Dataset

Builder:
[build_facet1_enriched_market_dataset.py](../scripts/build_facet1_enriched_market_dataset.py)

Run:

```bash
./.venv/bin/python -u scripts/build_facet1_enriched_market_dataset.py
```

Output:

- `data/derived/facet1_enriched_market_dataset.parquet`

The enriched dataset starts from:
[facet1_unified_last_trade_dataset.parquet](../data/derived/facet1_unified_last_trade_dataset.parquet)

and attaches:

- `event_ticker` and `category_raw` for Kalshi
- `market_start_ts`
- `close_month`
- `lifespan_hours`
- platform-native liquidity fields
- within-platform liquidity quintiles

Current materialized result:

- rows: `811,010`
- Kalshi rows: `554,020`
- Polymarket rows: `256,990`

Coverage notes:

- Kalshi `market_start_ts`: `100%`
- Polymarket `market_start_ts`: `231,481 / 256,990` rows (`90.1%`)
- category mapping is intentionally only structural at this stage:
  - Kalshi: `category_raw` from `event_ticker`
  - Polymarket: deferred

## Methodology

All three slices keep the same base observation unit:

- one resolved market
- one last pre-close reference-side trade
- one realized outcome for the fixed reference side

### Monthly Drift

Analysis:
[facet1_monthly_drift.py](../src/analysis/comparison/facet1_monthly_drift.py)

Method:

- group markets by `platform, close_month`
- recompute 5-cent bucket calibration inside each month
- summarize each month with:
  - expected calibration error (ECE): weighted mean absolute bucket gap
  - signed calibration gap: weighted mean empirical minus implied
  - low-tail and high-tail gaps

Interpretation:

- positive signed gap: the reference side won more often than implied
- negative signed gap: the reference side was overpriced on average

### Liquidity

Analysis:
[facet1_liquidity_calibration.py](../src/analysis/comparison/facet1_liquidity_calibration.py)

Method:

- bucket each platform into within-platform liquidity quintiles
- primary liquidity metric:
  - Kalshi: `volume_contracts`
  - Polymarket: `volume_usd`
- secondary liquidity metric kept for reference:
  - Kalshi: `open_interest_contracts`
  - Polymarket: `liquidity_usd`
- recompute bucket calibration inside each `platform x liquidity_quintile`

The quintiles are platform-relative by design. Raw Kalshi contract counts and raw Polymarket USD volume are not directly comparable on one shared scale.

### Kalshi Category Groups

Analysis:
[facet1_kalshi_category_calibration.py](../src/analysis/comparison/facet1_kalshi_category_calibration.py)

Method:

- use `category_raw` from the leading `event_ticker` prefix
- map that raw key through the existing Kalshi taxonomy in
  [categories.py](../src/analysis/kalshi/util/categories.py)
- aggregate to the top-level group (`Sports`, `Crypto`, `Finance`, etc.)
- recompute bucket calibration inside each category group

This slice is Kalshi-only for now because the Polymarket market parquet does not expose a native category field.

## Current Results

### Monthly Drift

Weighted over all months:

- Kalshi ECE: `2.96%`
- Kalshi signed gap: `-2.05%`
- Polymarket ECE: `1.19%`
- Polymarket signed gap: `-0.57%`

Restricting to months with at least `1,000` resolved markets:

- Kalshi ECE: `2.96%`
- Polymarket ECE: `1.01%`

Notable periods:

- Kalshi’s worst high-volume drift appears in `2025-03` through `2025-05`, with monthly ECE around `4.2%` to `4.7%`
- Polymarket’s largest high-volume drift appears in `2024-08` through `2024-10`, with monthly ECE around `2.5%` to `3.5%`
- Across time, both platforms remain negatively signed on average, meaning the reference side tends to be slightly overpriced rather than underpriced

### Liquidity

Main pattern:

- Kalshi improves monotonically with liquidity
- Polymarket is already fairly well calibrated across all quintiles, with the cleanest fit in the middle and upper-middle quintiles

Kalshi summary by quintile:

- `Q1 Lowest`: ECE `3.44%`, signed gap `-3.02%`
- `Q5 Highest`: ECE `1.85%`, signed gap `-0.82%`

Polymarket summary by quintile:

- `Q1 Lowest`: ECE `1.74%`, signed gap `-1.59%`
- `Q3 Mid`: ECE `0.55%`, signed gap `-0.25%`
- `Q5 Highest`: ECE `0.94%`, signed gap `0.10%`

Tail behavior:

- Kalshi favorite-longshot bias weakens in the highest-liquidity quintile but does not disappear
- Polymarket’s high-tail overpricing is concentrated in the lowest-liquidity quintile and is close to zero in the top quintiles

### Kalshi Category Groups

Best-calibrated large groups:

- `Weather`: ECE `1.54%`
- `Politics`: ECE `1.83%`
- `Sports`: ECE `2.24%` on `301,455` markets

Worst-calibrated groups:

- `Esports`: ECE `5.17%`
- `Crypto`: ECE `4.83%`
- `Finance`: ECE `3.87%`

Interpretation:

- the strongest Kalshi miscalibration is not universal
- it is concentrated in a few groups, especially `Esports` and `Crypto`
- high-volume, broad-participation groups like `Sports` are still imperfect but materially closer to calibrated than the worst groups

## Output Files

Main analysis outputs:

- `output/facet1_monthly_drift.{png,pdf,csv}`
- `output/facet1_monthly_drift_bucket_details.csv`
- `output/facet1_liquidity_calibration.{png,pdf,csv}`
- `output/facet1_liquidity_calibration_bucket_details.csv`
- `output/facet1_kalshi_category_calibration.{png,pdf,csv}`
- `output/facet1_kalshi_category_calibration_bucket_details.csv`

Tracked research copies:

- [research/tables/facet1](../research/tables/facet1/README.md)
- [research/latex_charts/facet1](../research/latex_charts/facet1/README.md)

## Deferred Follow-Ups

### Polymarket Category Mapping

Still needed:

- define a stable coarse taxonomy for Polymarket from `question` / `slug`
- version the mapping logic so category results remain reproducible
- only then run a cross-platform category comparison

### Time-To-Expiry Slice

Still needed:

- build a separate horizon snapshot dataset, not another slice of the last-trade table
- choose cutoffs like `30d`, `7d`, `1d`, `6h`, `1h`
- for each horizon, keep the last trade observed before `close_ts - horizon`
- then rerun the same 5-cent calibration logic at each horizon

This is separate because the current Facet 1 dataset only captures the final pre-close trade, so it cannot answer “are prices better 1 day out than 30 days out?” on its own.
