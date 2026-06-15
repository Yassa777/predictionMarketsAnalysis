# Facet 1 Kalshi Category Controls

Last updated: 2026-04-20

## Purpose

This document records the controlled Kalshi category follow-up to the original
Facet 1 slice analysis.

The goal is to separate category effects from two obvious composition effects:

- liquidity
- freshness / stale-price exposure

## Shared Inputs

Market-level builder:
[build_facet1_enriched_market_dataset.py](../scripts/build_facet1_enriched_market_dataset.py)

Horizon-level builder:
[build_facet1_time_to_expiry_enriched_dataset.py](../scripts/build_facet1_time_to_expiry_enriched_dataset.py)

The market enrichment path now materializes:

- `category_group`
- `category_mid`
- `category_subcategory`

Kalshi category coverage:

- market-level Kalshi rows with null `category_group`: `0`
- horizon-level Kalshi rows with null `category_group`: `0`

Unmapped Kalshi rows are labeled `Other`; they are never silently dropped.

## Analyses

### Liquidity Controls

Analysis:
[facet1_kalshi_category_liquidity_controls.py](../src/analysis/comparison/facet1_kalshi_category_liquidity_controls.py)

Run:

```bash
./.venv/bin/python main.py analyze facet1_kalshi_category_liquidity_controls
```

Outputs:

- `output/facet1_kalshi_category_liquidity_controls.{png,pdf,csv}`
- `output/facet1_kalshi_category_liquidity_controls_bucket_details.csv`
- `output/facet1_kalshi_category_liquidity_controls_regression_coefficients.csv`
- `output/facet1_kalshi_category_liquidity_controls_category_mid_drilldown.csv`

### Freshness Controls

Analysis:
[facet1_kalshi_category_freshness_controls.py](../src/analysis/comparison/facet1_kalshi_category_freshness_controls.py)

Run:

```bash
./.venv/bin/python main.py analyze facet1_kalshi_category_freshness_controls
```

Outputs:

- `output/facet1_kalshi_category_freshness_controls.{png,pdf,csv}`
- `output/facet1_kalshi_category_freshness_controls_bucket_details.csv`
- `output/facet1_kalshi_category_freshness_controls_regression_coefficients.csv`
- `output/facet1_kalshi_category_freshness_controls_category_mid_drilldown.csv`

`category_mid` drilldowns are only emitted for parent groups with at least
`10,000` distinct markets overall and reported slices with at least `1,000`
markets.

## Current Results

### After Liquidity Controls

Largest Kalshi groups in the controlled table:

- `Sports`: `301,455` markets
- `Crypto`: `116,214`
- `Finance`: `63,229`
- `Weather`: `30,560`

Q1 vs Q5 ECE:

- `Sports`: `2.93%` -> `1.65%`
- `Crypto`: `5.73%` -> `2.74%`
- `Finance`: `6.04%` -> `1.96%`
- `Weather`: `2.66%` -> `1.20%`

Decision answer for liquidity control:

- `Crypto` still looks like the clearest large-category inefficiency after liquidity control
- `Finance` still looks elevated, but less extreme than `Crypto`
- `Sports`, `Politics`, and `Weather` move much closer to calibration in the top quintile
- `Esports` remains very poor where observed, but high-liquidity `Esports` cells are small and should be treated cautiously

The confirmatory weighted bucket-gap regression is consistent with category
effects surviving controls: several category coefficients remain materially away
from the `Sports` baseline after controlling for liquidity quintile and price
bucket.

### After Freshness Controls

Headline `1h` ECE using the `<=6h` filter:

- `Esports`: `5.78%` on `4,499` rows
- `Crypto`: `4.59%` on `17,467`
- `Sports`: `2.71%` on `169,795`
- `Finance`: `2.48%` on `29,291`
- `Weather`: `1.26%` on `15,914`
- `Entertainment`: `1.41%` on `4,949`

All category-horizon slices now carry all configured freshness filters, and the
retained share declines monotonically as filters tighten.

Decision answer for freshness control:

- `Crypto` and `Esports` still look inefficient after freshness control
- `Finance` and `Sports` remain meaningfully worse than the cleanest groups
- `Weather` and `Entertainment` remain relatively well calibrated even after freshness conditioning
- freshness filtering changes magnitudes, but it does not erase category dispersion

The weighted bucket-gap regression again indicates that category effects survive
controls, especially for `Media` and `Esports` relative to the `Sports`
baseline.

## Interpretation

The safest read is:

- category differences are not purely semantic, but they are also not explained away by liquidity or freshness alone
- the strongest persistent large-category signal is `Crypto`
- `Finance` still matters
- `Weather` stays comparatively clean

Small groups such as `Science/Tech` and `World Events` can show large point
estimates, but the cell counts are thin enough that they should stay secondary.

## Tracked Research Outputs

- [research/tables/facet1](../research/tables/facet1/README.md)
- [research/latex_charts/facet1](../research/latex_charts/facet1/README.md)
