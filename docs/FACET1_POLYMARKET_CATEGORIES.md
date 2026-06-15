# Facet 1 Polymarket Categories

Last updated: 2026-06-15

## Purpose

This document records the deterministic Polymarket category mapping added after
the original Facet 1 Kalshi category work.

The original category analysis was Kalshi-only because Kalshi exposes a usable
event ticker taxonomy. Polymarket does not expose the same native category field
in the local market parquet, so this extension maps Polymarket markets from
question and slug text into the same coarse research categories used elsewhere.

## Analysis

Analysis:
[facet1_polymarket_category_mapping.py](../src/analysis/comparison/facet1_polymarket_category_mapping.py)

Run:

```bash
./.venv/bin/python main.py analyze facet1_polymarket_category_mapping
```

The analysis starts from:

- `data/derived/facet1_enriched_market_dataset.parquet`
- `data/polymarket/markets/*.parquet`

## Method

The mapper applies deterministic regular-expression rules in priority order.
Each matched market receives:

- `category_group`
- `category_rule`

The current rule families cover:

- Crypto
- Esports
- Sports
- Weather
- Politics
- Finance
- Entertainment
- Media
- Science/Tech
- World Events
- Other

The `Other` category is retained rather than dropped. This makes coverage
auditable and avoids silently excluding markets that do not match the current
rule set.

## Output Files

Generated under `output/` and copied to
`research/tables/facet1_polymarket_categories/`:

- `facet1_polymarket_category_mapping.csv`
- `facet1_polymarket_category_calibration.csv`
- `facet1_cross_platform_category_calibration.csv`

Current tracked row counts:

- mapping rows: `256,990`
- Polymarket category calibration rows: `11`
- cross-platform comparison rows: `11`

## Interpretation Notes

The mapping is intentionally simple and reproducible. It is suitable for
research slicing, but not a claim that every market has a perfect semantic
label. The `category_rule` column should be inspected when auditing individual
rows or when refining the taxonomy.

The cross-platform table compares only categories present on both platforms
after mapping. It reports Kalshi and Polymarket ECE side by side, plus the
Polymarket-minus-Kalshi difference.
