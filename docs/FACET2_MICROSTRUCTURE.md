# Facet 2 Microstructure Outputs

This analysis creates compact CSV tables for remote inspection without requiring
full raw-trade database access.

## Command

```bash
./.venv/bin/python main.py analyze facet2_return_autocorrelation
```

## Outputs

The command writes the same CSV files to `output/` and
`research/tables/facet2/`:

- `facet2_return_autocorrelation.csv`: event-time return autocorrelation at 1,
  5, 20, and 100 trade lags.
- `facet2_order_flow_future_returns.csv`: last-20-trade order-flow imbalance
  buckets versus future YES-price movement.
- `facet2_trade_size_impact.csv`: fixed trade-size buckets versus immediate
  and future YES-price movement.
- `facet2_price_impact_decay.csv`: side-aligned event-time price-impact
  retention after 1, 5, 20, and 100 trades.
- `facet2_time_of_day_effects.csv`: UTC hour and daypart summaries covering
  trading volume, movement, order flow, and resolved-market YES-price Brier
  score.

## Current Scope

The current implementation is Kalshi-first and uses a deterministic bounded
sample by default: the top 100 Kalshi markets in the enriched Facet 1 dataset
by the primary liquidity metric. This keeps the event-window analysis small
enough to run on a local machine while avoiding Python-side raw-trade loading.
The CSVs include
`sample_scope`, `selected_market_count`, and `source_trade_count` columns so
the sample is explicit in downstream use.

Kalshi trades already expose a binary YES price, taker side, contract count,
and timestamp, which makes event-time normalization direct.

Polymarket current-trade files are much larger and require extra normalization
before they are comparable:

- map token IDs to binary outcome sides per market;
- infer YES-price direction consistently across CTF Exchange and NegRisk rows;
- handle block/timestamp joins at 400M-row scale;
- decide whether to include legacy FPMM trades with a separate price convention.

Until that normalization is implemented, these outputs include `platform =
kalshi` rows only. The CSV schema keeps a `platform` column so Polymarket rows
can be appended later without changing downstream consumers.

## Interpretation Notes

All movement columns are in Kalshi YES-price cents. A positive future return
means the YES price increased after the reference trade. In price-impact decay,
returns are side-aligned by taker direction, so positive post-trade movement
means continuation in the taker's direction.
