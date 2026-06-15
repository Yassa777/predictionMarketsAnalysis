# Facet 1 Staleness Controls

Last updated: 2026-04-20

## Purpose

This document records the staleness-control extension to the Facet 1
time-to-expiry work.

The question is no longer just:

- how calibration changes by target horizon

It is now:

- how much of the horizon pattern is a true time-to-close effect
- how much is a stale-price artifact from quiet markets

## Shared Enriched Horizon Dataset

Builder:
[build_facet1_time_to_expiry_enriched_dataset.py](../scripts/build_facet1_time_to_expiry_enriched_dataset.py)

Run:

```bash
./.venv/bin/python -u scripts/build_facet1_enriched_market_dataset.py
./.venv/bin/python -u scripts/build_facet1_time_to_expiry_enriched_dataset.py
```

Output:

- `data/derived/facet1_time_to_expiry_enriched_dataset.parquet`

The enriched horizon table starts from:

- `data/derived/facet1_time_to_expiry_dataset.parquet`
- `data/derived/facet1_enriched_market_dataset.parquet`

and adds:

- `staleness_age_hours`
- `staleness_age_days`
- `staleness_bucket`
- `is_fresh_1h`
- `is_fresh_6h`
- `is_fresh_24h`
- `freshness_ratio`
- `close_month`
- `lifespan_hours`
- liquidity fields and quintiles
- Kalshi category hierarchy fields

Validation checks enforced in the builder:

- row count preserved exactly: `1,733,381`
- `staleness_age_hours == hours_before_close_gap` on every row
- no negative staleness rows
- freshness flags are monotone
- `platform x horizon` counts match the base time-to-expiry dataset

## Analyses

### Staleness Profile

Analysis:
[facet1_staleness_profile.py](../src/analysis/comparison/facet1_staleness_profile.py)

Run:

```bash
./.venv/bin/python main.py analyze facet1_staleness_profile
```

Outputs:

- `output/facet1_staleness_profile.{png,pdf,csv}`

### Freshness-Controlled Calibration

Analysis:
[facet1_freshness_controlled_calibration.py](../src/analysis/comparison/facet1_freshness_controlled_calibration.py)

Run:

```bash
./.venv/bin/python main.py analyze facet1_freshness_controlled_calibration
```

Outputs:

- `output/facet1_freshness_controlled_calibration.{png,pdf,csv}`
- `output/facet1_freshness_controlled_calibration_bucket_details.csv`

## Current Results

### Staleness Profile

Median staleness age at the target cutoff:

- Kalshi `30d`: `50.4h`
- Kalshi `1d`: `5.6h`
- Kalshi `1h`: `4.4h`
- Polymarket `30d`: `29.3h`
- Polymarket `1d`: `7.8h`
- Polymarket `1h`: `1.0h`

Retained share under the `<=6h` filter:

- Kalshi `30d`: `16.7%`
- Kalshi `1d`: `51.1%`
- Kalshi `1h`: `59.1%`
- Polymarket `30d`: `31.8%`
- Polymarket `1d`: `47.0%`
- Polymarket `1h`: `71.7%`

Interpretation:

- long-horizon rows are heavily contaminated by stale prices on both platforms
- Kalshi still has substantial staleness even at `1h`
- Polymarket becomes materially fresher near close, but long-horizon coverage is still thin under strict freshness cuts

### All Rows vs Freshness-Filtered Rows

Kalshi ECE, `all` vs `<=6h`:

- `30d`: `1.73%` -> `3.70%`
- `7d`: `1.66%` -> `1.98%`
- `3d`: `0.96%` -> `1.18%`
- `1d`: `1.31%` -> `1.58%`
- `6h`: `1.33%` -> `1.35%`
- `1h`: `2.03%` -> `2.30%`

Polymarket ECE, `all` vs `<=6h`:

- `30d`: `1.01%` -> `1.10%`
- `7d`: `0.96%` -> `1.10%`
- `3d`: `0.93%` -> `0.93%`
- `1d`: `0.81%` -> `0.76%`
- `6h`: `0.83%` -> `0.97%`
- `1h`: `0.65%` -> `0.67%`

Decision answer:

- the horizon effect persists after freshness controls
- it is not mostly a stale-market artifact
- staleness matters most for long-horizon coverage and for interpreting magnitude, not for reversing the main pattern

Platform-specific read:

- Kalshi still looks worst very near close even after freshness filters
- Polymarket still looks best near close even after freshness filters

## Takeaway

The right read is:

- keep the enriched horizon dataset as the shared base for any Phase 2 horizon work
- do not treat raw time-to-expiry rows as equally fresh observations
- use freshness either as a filter for conservative headline tables or as an explicit control in later modeling

## Next Choice

The next unresolved choice is still:

- should Phase 2 use only freshness-filtered rows
- or keep all rows and model freshness directly as a feature

At the moment the evidence favors keeping all rows for power, while always
reporting a freshness-controlled robustness check.

## Tracked Research Outputs

- [research/tables/facet1](../research/tables/facet1/README.md)
- [research/latex_charts/facet1](../research/latex_charts/facet1/README.md)
