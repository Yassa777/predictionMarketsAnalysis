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
