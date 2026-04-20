# Research Proposal: Prediction Market Forecasting

## Objective

Determine whether prediction market prices contain systematic, exploitable biases — and if so, build models that capture them.

This is structured in two phases: **Foundation** (statistical analysis to map where edges exist) and **Modeling** (neural nets to amplify confirmed edges). The foundation phase must complete first — it determines whether the modeling phase is worth pursuing and where to focus.

---

## Phase 1: Foundation (Statistical Analysis)

### Facet 1 — Calibration & Systematic Bias Detection

**Core question**: Are prediction market prices actually correct probabilities, or are they systematically wrong in predictable ways?

**Method**:
- For every resolved Kalshi market (`result` = yes/no), record the last traded price before close and the actual outcome.
- Compute empirical resolution rates by price bucket and compare to the price itself.
- A perfectly calibrated market priced at 70c resolves Yes exactly 70% of the time.

**Slices**:

| Dimension | Why |
|---|---|
| Price range (5c buckets) | The calibration curve itself — where is it off? |
| Category group | Sports vs Politics vs Finance — different participant pools, different efficiency |
| Time-to-expiry | Are prices more reliable 1 day out vs 30 days out? |
| Volume / open interest | Do illiquid markets have worse calibration? |
| Time period (monthly) | Has market efficiency improved over the dataset's lifespan? |

**Key outputs**:
- Calibration curve (predicted probability vs actual resolution rate)
- Calibration error by category group (heatmap)
- Favorite-longshot bias measurement: are tails (< 15c, > 85c) systematically mispriced?
- Calibration drift over time (are markets getting more efficient?)

**What it decides**: If calibration is near-perfect everywhere, the modeling phase pivots to pure microstructure. If specific categories or price ranges are miscalibrated, those become the target zones for all downstream work.

Implementation note as of 2026-04-07:

- baseline calibration, monthly drift, liquidity, and Kalshi category slices are implemented
- time-to-expiry is now implemented as a separate horizon-snapshot dataset in
  [FACET1_TIME_TO_EXPIRY.md](./FACET1_TIME_TO_EXPIRY.md)

---

### Facet 2 — Microstructure & Short-Term Price Prediction

**Core question**: Does the pattern of how trades arrive predict where the price is going next?

**Method**:
- Compute rolling trade flow features: buy/sell imbalance, trade size distribution, inter-trade timing.
- Measure whether these features predict price movement over the next N minutes/hours.
- Measure price impact decay: when a trade moves the price, how much sticks vs reverts?

**Analyses**:

| Analysis | What it measures |
|---|---|
| Order flow imbalance → future returns | Does net buying pressure predict price increases? |
| Trade size impact | Do large trades predict further movement (momentum) or reversal (mean reversion)? |
| Price impact half-life | How quickly does a trade's price impact decay? This defines the exploitable window. |
| Time-of-day effects | Are overnight/off-hours markets thinner and more predictable? |
| Maker vs taker profitability | Who's informed — the limit order placer or the market taker? |

**Platform considerations**:
- **Kalshi**: `taker_side` directly tells us who initiated. `yes_price`/`no_price` + `count` give clean trade-level data. No trader ID though — can't track individuals.
- **Polymarket**: `maker`/`taker` addresses let us track wallet-level flow. `maker_amount`/`taker_amount` give notional sizes. Need to derive price from amount ratios.

**Key outputs**:
- Autocorrelation of returns at various lags (1 min, 5 min, 1 hour, 1 day)
- Price impact curve (trade size vs immediate price change)
- Price impact decay curve (time vs remaining price impact)
- Maker vs taker PnL comparison

**What it decides**: The timescale of any tradeable signal. If price impact decays in seconds, only bots can exploit it. If it takes hours, a slower strategy works. If there's no autocorrelation at any lag, microstructure-based prediction is a dead end.

---

### Facet 3 — Market Lifespan Analysis

**Core question**: Does a market's lifespan predict where and when edges appear within it?

**Method**:
- Compute lifespan for each Kalshi market (`open_time` → `close_time`).
- For Polymarket, use `created_at` → `end_date` (90.1% coverage).
- Map trading activity distribution across each market's normalized lifespan (0% = open, 100% = close).

**Analyses**:

| Analysis | What it measures |
|---|---|
| Lifespan distribution by category | Do categories fully explain duration, or is there within-category variance? |
| Lifespan × calibration | Are short-lived markets (24h) worse calibrated than long-lived ones (months)? |
| Volume concentration curve | What % of a market's total volume occurs in the last 10% of its life? |
| Stale market detection | Markets with extended periods of zero/near-zero trading activity at a fixed price |
| Price volatility by life stage | Is most price movement early (discovery) or late (resolution info)? |
| Remaining time vs price movement magnitude | How much can prices still move as a function of time left? |

**Key outputs**:
- Lifespan distribution histograms by category group
- Heatmap: calibration error × lifespan bucket × category
- Volume concentration curve (cumulative % of volume vs % of lifespan elapsed)
- List of "stale" markets: ticker, price, days since last trade, final result

**What it decides**: When to enter positions. If edges concentrate early in a market's life (poor initial pricing), strategies should target new markets. If edges concentrate late (thin liquidity near expiry), strategies should target markets approaching close. Stale markets may be the simplest opportunity of all.

---

### Facet 4 — Participation Depth

**Core question**: Does the crowd structure of a market predict its accuracy and behavior?

**Data constraints**:
- **Kalshi**: No trader ID. Proxy participation via trade count, contract volume (`count`), and `open_interest` per ticker.
- **Polymarket**: `maker`/`taker` wallet addresses allow counting unique participants and measuring concentration.

**Analyses**:

| Analysis | What it measures |
|---|---|
| Participation proxy distribution (Kalshi) | Trade count and contract volume per market — what's the shape? |
| Unique wallet count (Polymarket) | How many distinct addresses trade each market? |
| Concentration index (Polymarket) | Herfindahl index per market — few whales or many small traders? |
| Participation depth × calibration | Do markets with more participants have better calibration? (Wisdom of crowds test) |
| Participation × category | Do category participation profiles correlate with calibration quality? |
| Participation growth curve | How does trader count evolve over a market's lifespan? Front-loaded vs back-loaded? |
| Repeat vs one-time wallets (Polymarket) | Are markets dominated by experienced wallets better calibrated? |
| Volume vs participant count decomposition | High volume + few participants = whales. High participants + low volume = retail. Which produces better prices? |

**Key outputs**:
- Distribution of trade count / unique wallets per market
- Concentration index distribution by category
- Scatter plot: participation metric vs calibration error
- Participation growth curves (normalized lifespan vs cumulative unique traders)
- Repeat trader analysis: wallet frequency distribution, accuracy by experience level

**What it decides**: Why edges exist. If miscalibrated markets are consistently the ones with thin participation, the thesis is simple — low crowd wisdom = opportunity. If concentrated (whale-dominated) markets are better calibrated, it suggests informed traders correct prices and the edge is in markets they haven't touched yet. This also directly informs whether wallet embeddings (Phase 2) are worth building.

---

## Phase 1 → Phase 2 Decision Gate

Before starting Phase 2, Phase 1 must answer:

| Question | If yes → | If no → |
|---|---|---|
| Are markets systematically miscalibrated? | Build outcome prediction models targeting miscalibrated zones | Pivot to pure microstructure / cross-platform arb |
| Does miscalibration vary by category? | Category-specific models | Single universal model |
| Is there short-term return autocorrelation? | Trade flow models (Transformer on trade stream) | Skip microstructure modeling |
| Do stale/illiquid markets exist at scale? | Simple mean-reversion strategy on stale markets | Focus on liquid markets only |
| Does participation depth predict accuracy? | Use participation features in models; pursue wallet embeddings | Drop participation as a feature |

---

## Phase 2: Modeling (Conditional on Phase 1 Findings)

These are ordered by complexity. Each builds on Phase 1 findings.

### Model A — Price Trajectory (LSTM / TCN)

**Input**: Sliding window of price snapshots for a single market over time.
**Target**: Final resolution (0/1).
**Thesis**: The shape of the price trajectory (momentum, mean-reversion, volatility clustering) contains information beyond the current price level. Phase 1 calibration analysis tells us which price ranges and categories to focus on.

### Model B — Trade Stream Transformer

**Input**: Sequence of recent trades (price, size, side, time-delta between trades).
**Target**: Next-N-minute price direction.
**Thesis**: Order flow patterns encode information arrival. Phase 2 microstructure analysis tells us the relevant timescale and whether the signal exists at all.

### Model C — Wallet Embeddings (Polymarket only)

**Input**: Learn a dense embedding per wallet address from their trading history.
**Target**: Trade profitability.
**Thesis**: Some wallets are consistently informed. Their presence/absence in a market is a signal. Phase 1 participation analysis tells us whether repeat traders actually matter and whether concentration predicts accuracy.

### Model D — LLM Embeddings + Tabular

**Input**: Pre-trained language model embedding of the market question text + numerical features (current price, volume, time-to-expiry, category, participation metrics).
**Target**: Resolution probability (calibrated).
**Thesis**: Certain types of questions are systematically mispriced. The text encodes semantic information that price alone doesn't capture. Phase 1 category analysis tells us whether semantic grouping matters.

---

## Data Requirements Summary

| Analysis | Kalshi Markets | Kalshi Trades | Polymarket Markets | Polymarket Trades | Blocks |
|---|---|---|---|---|---|
| Facet 1: Calibration | Required (result, prices) | Required (trade prices) | Optional | Optional | — |
| Facet 2: Microstructure | — | Required | — | Required (maker/taker flow) | Required (timestamps) |
| Facet 3: Lifespan | Required (open/close times) | Required (volume over time) | Partial (created_at 90%) | Required (volume over time) | Required (timestamps) |
| Facet 4: Participation | Required (volume, OI) | Required (trade counts) | — | Required (wallet addresses) | — |

---

## Success Criteria

Phase 1 is successful if it produces a clear, evidence-backed answer to: **"Where, when, and why are prediction markets wrong?"**

Phase 2 is successful if a model achieves statistically significant out-of-sample improvement over the naive strategy of "trust the market price" — measured by Brier score on held-out resolved markets, with proper temporal train/test splits (no future leakage).
