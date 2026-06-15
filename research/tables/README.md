# Research Tables

Last updated: 2026-06-15

This folder stores tracked CSV outputs that can be used for writing and remote
research without access to the full local parquet database.

Current folders:

- `facet1/`: baseline calibration, freshness, liquidity, time-to-expiry, and
  Kalshi category controls.
- `facet1_polymarket_categories/`: deterministic Polymarket category mapping
  and cross-platform category calibration.
- `facet2/`: Kalshi-first microstructure diagnostics.
- `facet3/`: market lifespan, volume concentration, late-stage movement, and
  stale candidate tables.
- `facet4/`: participation depth, Kalshi participation proxies, and Polymarket
  wallet concentration features.
- `modeling/`: temporal-holdout modeling decision tables.

The largest CSVs are split where needed to stay below GitHub's hard per-file
limit. GitHub may still warn for files larger than 50 MB.
