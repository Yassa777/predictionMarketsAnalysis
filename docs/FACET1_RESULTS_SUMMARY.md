# Facet 1 Results Summary

Last updated: 2026-04-20

## Purpose

This document is the interpretation layer for the implemented Phase 1 / Facet 1
work. It is meant to answer what the evidence now suggests about where
prediction markets are wrong, when those errors are most likely to survive, and
what seems to explain the remaining variation. Detailed construction,
methodology, and output definitions remain in the linked supporting documents.

## What Facet 1 Now Says

The implemented Facet 1 results now support a fairly clean read of the market.
Pricing errors are real, but they are not uniform across platforms, horizons, or
market conditions. The strongest errors are concentrated in specific parts of
the sample rather than spread evenly across everything. The repo no longer needs
to spend much time on the question of whether mispricing exists at all. The more
important question is which market conditions allow those errors to persist.

- Kalshi remains the clearer source of persistent static miscalibration.
- Polymarket remains closer to calibrated overall and still appears to improve as resolution approaches.
- Freshness matters, especially for interpreting horizon snapshots, but it does not overturn the core time-to-expiry story.
- Liquidity explains part of the variation in calibration quality, especially on Kalshi, but it does not explain everything.
- Kalshi category differences survive both liquidity and freshness controls, so the remaining dispersion is not just a compositional artifact.
- Participation is now the right next diagnostic because the open question is why some markets stay wrong, not whether they ever were.

## Platform-Level Read

### Kalshi

The evidence now suggests that Kalshi's miscalibration is substantive rather
than cosmetic. The baseline favorite-longshot pattern is still there, and the
new control work does not wash it out. Liquidity helps explain part of the
problem, which is consistent with thin participation allowing errors to survive,
but the controlled category results show that market structure still matters
after liquidity is held fixed. The cleanest read is that Kalshi has genuine
condition-dependent inefficiency, not just a few noisy edge cases.

The time-to-expiry extension sharpens that point. Kalshi does not simply become
more efficient as close approaches. Its late-stage weakness still looks real
after freshness controls, which means the final-hour problem is not well
explained by stale prices alone. Something about the near-resolution state of
those markets still appears to produce worse pricing.

### Polymarket

Polymarket continues to look materially tighter on static calibration. That
result was already visible in the baseline Facet 1 work, and the newer
freshness checks make it easier to trust. The near-close improvement still looks
genuine rather than like a stale-snapshot illusion. The repo should therefore
treat Polymarket less as a broad static mispricing story and more as a market
where the remaining edge, if any, is likely to be conditional on participation,
freshness, or microstructure.

The cleanest read is that Polymarket is not perfectly efficient, but its errors
look narrower, more conditional, and less structurally broad than Kalshi's.

## What Freshness Changed

The freshness work changed the interpretation of the horizon dataset, but it did
not reverse it. The key issue is simple: a horizon snapshot is often the last
trade before the target cutoff, not a trade exactly at that cutoff. In quieter
markets, that means the observed price can be meaningfully older than the
intended horizon.

That caveat matters. It means some of the long-horizon signal was always partly
a stale-price signal. It also means headline time-to-expiry results should not
be read as if every row were equally fresh. But the evidence now suggests that
freshness is an important qualification, not a full reversal. Once stale rows
are filtered down, the main platform-level pattern still survives: Kalshi still
looks weaker very near resolution, and Polymarket still looks cleaner as close
approaches.

The practical consequence is straightforward. Future work should either model
freshness explicitly or pair any headline horizon result with a
freshness-controlled robustness check by default.

## What Category and Liquidity Controls Changed

The control analyses make the Kalshi category story more credible. Liquidity is
clearly part of the explanation. Markets with deeper participation proxies tend
to look better, especially on Kalshi, and any serious interpretation should keep
that in view. But liquidity does not eliminate the category effect. The
remaining dispersion survives after conditioning on liquidity, which means the
category story cannot be dismissed as a simple volume story.

Freshness changes magnitudes in the same way. Some category differences narrow
once fresher rows are required, but the ordering does not collapse. The cleanest
large-category read still looks stable: crypto remains one of the clearest
sources of persistent weakness, finance still matters, weather remains
comparatively clean, and esports still looks weak, though it deserves more
caution when the slice gets thin.

The evidence now suggests that Kalshi category structure is telling us something
real about where pricing quality breaks down. The remaining spread is not just
semantic labeling, and it is not explained away by either liquidity or
freshness alone.

## What This Means for Phase 2

The repo should now move past the basic question of whether prediction markets
misprice in the aggregate. Facet 1 already answered that. The more productive
next question is which conditions produce the remaining errors and which kinds
of crowd structure help remove them.

That is why participation is the next best diagnostic. It can test whether the
residual errors are mostly thin-crowd problems, concentration problems, or a
difference between whale-dominated and broad-retail markets. It is also the
most direct bridge from the current descriptive work to later modeling choices,
including whether wallet-level features are worth the complexity on Polymarket.

The next unresolved choice is whether later models should keep the broad sample
and treat freshness and participation as explicit features, or move to a
narrower high-quality subset where those issues are reduced before modeling
begins.

## Supporting Documents and Outputs

Supporting Facet 1 documents:

- [FACET1_UNIFIED_DATASET.md](./FACET1_UNIFIED_DATASET.md)
- [FACET1_SLICE_ANALYSES.md](./FACET1_SLICE_ANALYSES.md)
- [FACET1_TIME_TO_EXPIRY.md](./FACET1_TIME_TO_EXPIRY.md)
- [FACET1_STALENESS.md](./FACET1_STALENESS.md)
- [FACET1_CATEGORY_CONTROLS.md](./FACET1_CATEGORY_CONTROLS.md)

Tracked outputs:

- [research/tables/facet1](../research/tables/facet1/README.md)
- [research/latex_charts/facet1](../research/latex_charts/facet1/README.md)
