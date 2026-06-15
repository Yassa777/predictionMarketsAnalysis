# Modeling Decision Tables

This analysis benchmarks whether simple feature slices improve out-of-sample
resolved-market prediction over the raw market-price baseline.

## Command

```bash
./.venv/bin/python main.py analyze modeling_feature_benchmark
```

## Input

The analysis reads:

- `data/derived/facet1_time_to_expiry_enriched_dataset.parquet`

Build or refresh it with:

```bash
./.venv/bin/python -u scripts/build_facet1_time_to_expiry_enriched_dataset.py
```

## Outputs

The command writes the same CSV files to `output/` and
`research/tables/modeling/`:

- `modeling_feature_benchmark.csv`: Brier score and log loss on a temporal
  holdout split for the raw market price and simple shrunken residual variants.
- `modeling_feature_slices.csv`: model-versus-market deltas by platform,
  horizon, category, freshness, staleness, and liquidity slices where the test
  slice has enough rows.

## Method

Rows are sorted by market close timestamp. The first 80% of close timestamps
form the training partition and the remaining rows form the test partition.
The baseline prediction is `reference_price_cents / 100`.

The feature variants are deliberately simple residual tables. On the training
partition, each variant estimates the average market-price residual within a
feature grouping, shrinks that residual toward zero, and adds it to the market
price on the test partition. This keeps the table useful as a modeling decision
aid without introducing a heavier model dependency.

Negative deltas in the output mean the feature variant improved over the raw
market-price baseline for that metric.
