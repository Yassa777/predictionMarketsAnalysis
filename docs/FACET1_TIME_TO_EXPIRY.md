# Facet 1 Time-To-Expiry

Last updated: 2026-04-20

## Purpose

This document describes the time-to-expiry extension of Phase 1 / Facet 1.

The goal is to measure calibration not just at the final pre-close trade, but at
fixed horizons before close:

- `30d`
- `7d`
- `3d`
- `1d`
- `6h`
- `1h`

This answers a different question from the baseline Facet 1 dataset:

- baseline Facet 1: "how calibrated is the last trade before close?"
- time-to-expiry: "how calibrated are markets at a fixed distance from close?"

## Builder

Builder:
[build_facet1_time_to_expiry_dataset.py](../scripts/build_facet1_time_to_expiry_dataset.py)

Run:

```bash
./.venv/bin/python -u scripts/build_facet1_time_to_expiry_dataset.py
```

Outputs:

- `data/derived/facet1_time_to_expiry_dataset.parquet`
- `data/derived/facet1_time_to_expiry_5c_buckets.parquet`
- `data/derived/facet1_time_to_expiry_5c_buckets.csv`

## Method

The dataset keeps the same fixed reference-side convention as the baseline Facet 1 work:

- Kalshi: reference side is always `yes`
- Polymarket: reference side is always `outcome_0`

For each resolved market and each horizon:

1. compute `cutoff_ts = close_ts - horizon`
2. find the last trade with `trade_ts <= cutoff_ts`
3. convert that trade into `reference_price_cents`
4. keep the realized outcome as `reference_won`
5. assign the same 5-cent price bucket scheme used elsewhere in Facet 1

The key output row is therefore one `(market, horizon)` observation.

## Important Interpretation Note

The snapshot trade is the last trade before the target cutoff, not necessarily a
trade exactly at the cutoff.

That means:

- `actual_hours_before_close` can be larger than the target `horizon_hours`
- the difference is stored as `hours_before_close_gap`

This is not an error. It exposes market staleness.

In practice, many markets do not trade continuously, so the time-to-expiry slice
captures both:

- how calibration changes with time remaining
- how stale markets are at that horizon

This matters especially for Polymarket, where the mean snapshot can be much
earlier than the target cutoff because some markets go quiet long before close.

## Materialized Result

Materialized on `2026-04-07`:

- dataset rows: `1,733,381`
- distinct markets represented: `660,882`
- Kalshi rows: `1,005,563`
- Polymarket rows: `727,818`
- bucket rows: `240`

## Analysis

Analysis:
[facet1_time_to_expiry_calibration.py](../src/analysis/comparison/facet1_time_to_expiry_calibration.py)

Run:

```bash
./.venv/bin/python main.py analyze facet1_time_to_expiry_calibration
```

Outputs:

- `output/facet1_time_to_expiry_calibration.png`
- `output/facet1_time_to_expiry_calibration.pdf`
- `output/facet1_time_to_expiry_calibration.csv`
- `output/facet1_time_to_expiry_calibration_bucket_details.csv`

## Current Results

### Kalshi

ECE by horizon:

- `30d`: `1.73%`
- `7d`: `1.66%`
- `3d`: `0.96%`
- `1d`: `1.31%`
- `6h`: `1.33%`
- `1h`: `2.03%`

Main takeaway:

- Kalshi is not worst at long horizons
- the cleanest calibration in this first pass is around `3d`
- calibration worsens again very near close, especially at `1h`

That suggests the final-hour market is not simply “most efficient.” Late trading
may be noisier or more selective than a naive convergence story would imply.

### Polymarket

ECE by horizon:

- `30d`: `1.01%`
- `7d`: `0.96%`
- `3d`: `0.93%`
- `1d`: `0.81%`
- `6h`: `0.83%`
- `1h`: `0.65%`

Main takeaway:

- Polymarket improves more smoothly as close approaches
- the best calibration in this first pass is at `1h`
- even long-horizon Polymarket calibration is already tighter than most Kalshi horizons

### Freshness / Staleness

Median actual hours before close:

- Kalshi `30d` target: `770.45h` actual
- Kalshi `1h` target: `5.43h` actual
- Polymarket `30d` target: `749.30h` actual
- Polymarket `1h` target: `1.97h` actual

Mean actual hours before close are much larger, especially on Polymarket:

- Polymarket `30d`: `1584.76h`
- Polymarket `1h`: `129.32h`

Interpretation:

- many markets are stale before the target cutoff
- the time-to-expiry slice is also surfacing a stale-market effect
- later refinements should likely add a freshness filter or analyze `hours_before_close_gap` directly

## What This Changes

This result makes the remaining proposal more concrete:

- time-to-expiry should stay as a separate dataset, not a slice of the last-trade table
- stale-market analysis now looks more important than before
- later predictive work should compare:
  - raw time-to-expiry calibration
  - time-to-expiry with freshness constraints

## Tracked Research Outputs

- [research/tables/facet1](../research/tables/facet1/README.md)
- [research/latex_charts/facet1](../research/latex_charts/facet1/README.md)
- [FACET1_RESULTS_SUMMARY.md](./FACET1_RESULTS_SUMMARY.md)
