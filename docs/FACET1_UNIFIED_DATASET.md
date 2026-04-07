# Facet 1 Unified Dataset

Last updated: 2026-04-06

## Purpose

This document describes the derived dataset used for Phase 1, Facet 1 of the research proposal in [RESEARCH_PROPOSAL.md](./RESEARCH_PROPOSAL.md).

The goal is to build one market-level table across Kalshi and Polymarket where each row represents:

- one resolved binary market
- the last observed trade before market close
- the outcome of a fixed reference side
- the 5-cent bucket used for calibration analysis

## Builder Script

Script:
[build_facet1_unified_last_trade_5c_dataset.py](../scripts/build_facet1_unified_last_trade_5c_dataset.py)

Run:

```bash
./.venv/bin/python -u scripts/build_facet1_unified_last_trade_5c_dataset.py
```

## Output Files

Generated under `data/derived/`:

- `facet1_unified_last_trade_dataset.parquet`
- `facet1_unified_last_trade_5c_buckets.parquet`
- `facet1_unified_last_trade_5c_buckets.csv`
- `facet1_enriched_market_dataset.parquet` via the follow-on builder in
  [build_facet1_enriched_market_dataset.py](../scripts/build_facet1_enriched_market_dataset.py)

These files are intentionally Git-ignored because they are generated artifacts.

## Reference Side Convention

The dataset uses a fixed reference side per platform.

- Kalshi: reference side is always `yes`
- Polymarket: reference side is always `outcome_0`

This keeps the semantics stable for:

- `reference_price_cents`
- `reference_won`

`reference_won = 1` means the fixed reference side won.
`reference_won = 0` means the fixed reference side lost.

## Unified Dataset Columns

- `platform`
- `market_id`
- `market_title`
- `reference_side`
- `reference_label`
- `winning_label`
- `close_ts`
- `last_trade_ts`
- `last_trade_source`
- `reference_price_cents`
- `reference_won`
- `price_bucket_5c_floor`
- `price_bucket_5c_mid`
- `price_bucket_5c_label`

## Build Logic

### Kalshi

- Filter to resolved markets with non-null `close_time`
- Join trades on `ticker`
- Keep trades with `created_time <= close_time`
- Rank descending by `created_time, trade_id`
- Keep `rn = 1`
- Use `yes_price` as `reference_price_cents`

### Polymarket CTF

- Filter to resolved binary markets with clean end-state prices
- Use `clob_token_ids[0]` as the reference token
- Filter current trades to only relevant outcome tokens
- Resolve timestamps from trade timestamps or block timestamps
- Convert any trade into the implied price of `outcome_0`
- Rank descending by `trade_ts, transaction_hash, log_index`
- Keep `rn = 1`

### Polymarket Legacy FPMM

- Filter to USDC-collateralized FPMM markets
- Join legacy trades on `fpmm_address`
- Resolve timestamps from blocks
- Convert `outcome_index` into the implied price of `outcome_0`
- Rank descending by `trade_ts, transaction_hash, log_index`
- Keep `rn = 1`

## 5-Cent Buckets

The bucket label is built from:

```text
0-5, 5-10, 10-15, ..., 95-100
```

using:

- `price_bucket_5c_floor`
- `price_bucket_5c_mid`
- `price_bucket_5c_label`

## Current Materialized Result

Materialized on 2026-04-06:

- unified rows: `811,010`
- Kalshi rows: `554,020`
- Polymarket rows: `256,990`
- 5-cent bucket rows: `40`

## Notes

- The builder is optimized to avoid a single giant Polymarket join.
- It prepares filtered Polymarket inputs first, then computes chunked market-level outputs.
- Temporary DuckDB spill and chunk files are written under `data/derived/` during execution and then merged into final outputs.
- Slice analyses built on top of this dataset are documented in
  [FACET1_SLICE_ANALYSES.md](./FACET1_SLICE_ANALYSES.md).
