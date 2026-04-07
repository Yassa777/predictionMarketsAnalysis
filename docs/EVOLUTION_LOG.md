# Evolution Log

This file records dated implementation milestones so the codebase evolution is easier to follow.

## 2026-04-06

### Facet 1 Dataset Builder Added

Added:
[build_facet1_unified_last_trade_5c_dataset.py](../scripts/build_facet1_unified_last_trade_5c_dataset.py)

Intent:

- materialize a unified market-level dataset for Phase 1, Facet 1
- use the last trade before close instead of trade-level calibration points
- normalize Kalshi and Polymarket into a shared schema
- produce a 5-cent bucket summary directly from the derived market-level table

### Derived Output Convention Documented

Added:
[FACET1_UNIFIED_DATASET.md](./FACET1_UNIFIED_DATASET.md)

Intent:

- document the builder entrypoint
- pin down reference-side semantics
- record the output schema
- make the generated row counts easy to recover later

### Git Hygiene Tightened

Updated:
[.gitignore](../.gitignore)

Intent:

- keep `data/derived/` and its temporary DuckDB artifacts out of Git
- make the repository safe to push after generating large derived tables

### Performance Optimization Applied

The first end-to-end implementation hit memory and temp-spill bottlenecks on the current Polymarket trade corpus.

The builder was then reworked to:

- precompute resolved Polymarket market metadata
- prefilter relevant CTF trades before timestamp resolution
- prefilter relevant legacy FPMM trades separately
- build final market-level outputs in chunks and merge them at the end

Result:

- end-to-end build completed successfully on 2026-04-06
- final dataset size: `811,010` rows
- final bucket table size: `40` rows

### Baseline Facet 1 Calibration Analysis Added

Added:
[facet1_unified_calibration_curves.py](../src/analysis/comparison/facet1_unified_calibration_curves.py)

Outputs:

- `output/facet1_unified_calibration_curves.png`
- `output/facet1_unified_calibration_curves.pdf`
- `output/facet1_unified_calibration_curves.csv`
- `output/facet1_unified_calibration_curves.json`

Intent:

- plot the first baseline Facet 1 calibration curves from the derived market-level dataset
- compare Kalshi, Polymarket, and pooled calibration side by side
- expose bucket counts and 95% Wilson confidence intervals for each 5-cent bucket

### Tracked Research Output Folders Added

Added:

- [research/tables/facet1](../research/tables/facet1/README.md)
- [research/latex_charts/facet1](../research/latex_charts/facet1/README.md)

Intent:

- keep publication-facing CSV tables under version control
- keep LaTeX-ready chart assets under version control
- separate tracked research artifacts from ignored generated datasets in `data/derived/`

### Facet 1 Slice Analyses Added

Added:

- [build_facet1_enriched_market_dataset.py](../scripts/build_facet1_enriched_market_dataset.py)
- [facet1_monthly_drift.py](../src/analysis/comparison/facet1_monthly_drift.py)
- [facet1_liquidity_calibration.py](../src/analysis/comparison/facet1_liquidity_calibration.py)
- [facet1_kalshi_category_calibration.py](../src/analysis/comparison/facet1_kalshi_category_calibration.py)
- [FACET1_SLICE_ANALYSES.md](./FACET1_SLICE_ANALYSES.md)

Intent:

- enrich the market-level Facet 1 dataset with month, lifespan, and liquidity metadata
- measure calibration drift over time
- measure how calibration changes with within-platform liquidity
- isolate Kalshi category groups before attempting Polymarket category mapping
- document the deferred follow-up work needed for Polymarket categories and time-to-expiry horizons
