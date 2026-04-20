# Facet 1 Results Summary

Last updated: 2026-04-20

## Purpose

This document consolidates the current Phase 1 / Facet 1 findings across the
derived datasets and slice analyses that are now implemented.

Supporting method documents:

- [FACET1_UNIFIED_DATASET.md](./FACET1_UNIFIED_DATASET.md)
- [FACET1_SLICE_ANALYSES.md](./FACET1_SLICE_ANALYSES.md)
- [FACET1_TIME_TO_EXPIRY.md](./FACET1_TIME_TO_EXPIRY.md)

Tracked tables and chart assets:

- [research/tables/facet1](../research/tables/facet1/README.md)
- [research/latex_charts/facet1](../research/latex_charts/facet1/README.md)

## Implemented Assets

The current Facet 1 stack consists of:

- one unified market-level last-trade dataset across Kalshi and Polymarket
- one enriched market-level dataset with month, lifespan, liquidity, and Kalshi category metadata
- one separate time-to-expiry dataset with fixed horizon snapshots before close
- tracked CSV tables and PDF/PNG figures for baseline calibration, monthly drift, liquidity, category, and time-to-expiry outputs

## Headline Findings

### Baseline Calibration

Baseline calibration is materially different across platforms.

- Kalshi shows a strong favorite-longshot pattern: low-priced contracts are too expensive on average and high-priced contracts are too cheap.
- Polymarket is much closer to calibrated across the full price range.
- The strongest static miscalibration is therefore platform-specific rather than universal.

Headline tail summaries:

- Kalshi low tail (`<15c`): implied `3.43%`, empirical `1.57%`
- Kalshi high tail (`>=85c`): implied `97.17%`, empirical `98.63%`
- Polymarket low tail (`<15c`): implied `1.32%`, empirical `1.07%`
- Polymarket high tail (`>=85c`): implied `98.73%`, empirical `98.38%`

Primary output tables:

- `facet1_unified_calibration_curves.csv`
- `facet1_tail_summary.csv`

### Monthly Drift

Calibration is not stable through calendar time.

- Kalshi has larger month-to-month drift and more pronounced bad periods.
- Polymarket also drifts, but the variation is smaller overall.
- The monthly slice supports a time-aware evaluation approach for later modeling rather than random train/test splits.

Weighted over all months:

- Kalshi ECE: `2.96%`
- Kalshi signed gap: `-2.05%`
- Polymarket ECE: `1.19%`
- Polymarket signed gap: `-0.57%`

Interpretation note:

- the raw dataset spans `2020-11` through `2027-01`, but the cleaner inference comes from the higher-count months rather than sparse edge months

Primary output table:

- `facet1_monthly_drift.csv`

### Liquidity

Liquidity is a strong explanatory slice, especially on Kalshi.

- Kalshi calibration improves steadily as within-platform liquidity rises.
- Polymarket is already fairly well calibrated across all quintiles, with the weakest fit concentrated in the lowest-liquidity markets.
- This suggests thin participation is one of the main conditions under which mispricing survives.

Representative results:

- Kalshi `Q1 Lowest` ECE: `3.44%`
- Kalshi `Q5 Highest` ECE: `1.85%`
- Polymarket `Q1 Lowest` ECE: `1.74%`
- Polymarket `Q3 Mid` ECE: `0.55%`

Primary output table:

- `facet1_liquidity_calibration.csv`

### Kalshi Category Groups

Kalshi miscalibration is concentrated in a subset of category groups.

- `Weather`, `Politics`, and `Sports` are relatively efficient.
- `Esports`, `Crypto`, and `Finance` are materially less efficient.
- This pattern suggests semantics and market structure matter, not just platform identity.

Headline group results:

- `Weather` ECE: `1.54%`
- `Politics` ECE: `1.83%`
- `Sports` ECE: `2.24%`
- `Esports` ECE: `5.17%`
- `Crypto` ECE: `4.83%`
- `Finance` ECE: `3.87%`

Primary output table:

- `facet1_kalshi_category_calibration.csv`

### Time-To-Expiry

Time-to-expiry is not just a longer or shorter version of the baseline last-trade view, so it is implemented as a separate horizon dataset.

- Polymarket becomes more calibrated as close approaches.
- Kalshi is best around intermediate horizons and then worsens again in the final hour.
- The horizon result therefore does not support a naive “closest to close is always most efficient” story.

Headline horizon results:

- Kalshi is cleanest at `3d` and materially worse again at `1h`
- Polymarket improves fairly smoothly from `30d` through `1h`

Primary output table:

- `facet1_time_to_expiry_calibration.csv`

## Freshness / Staleness Caveat

The time-to-expiry dataset exposed an important caveat that was not visible in
the baseline last-trade dataset.

- each horizon row uses the last trade before a target cutoff, not a trade exactly at the cutoff
- many markets, especially quieter ones, have snapshot prices that are older than the target horizon
- some of the apparent horizon effect is therefore a freshness effect

This means the next refinement should explicitly test whether the horizon result
survives once stale prices are filtered out.

## Current Interpretation

The implemented Facet 1 work supports the following reading of the market:

- prediction market inefficiency is real, but it is concentrated rather than universal
- Kalshi is the clearer source of static calibration error
- liquidity, category, and market freshness all appear to matter
- Polymarket looks closer to efficient on static calibration and likely needs more emphasis on microstructure, participation, and freshness than on broad outcome mispricing

## Next Analyses

The most important follow-on work is now better defined.

1. Staleness / freshness analysis on top of the time-to-expiry dataset.
2. Kalshi category analysis controlled for liquidity and freshness.
3. Polymarket category mapping if a stable reproducible taxonomy is needed.
4. Separate freshness-aware modeling baselines before any more complex predictive work.
