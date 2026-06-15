"""Deterministic Polymarket category mapping and category calibration exports."""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from src.analysis.comparison.facet1_slice_utils import (
    BASE_DIR,
    compute_bucket_calibration,
    compute_slice_summary,
    load_enriched_dataset,
)
from src.analysis.comparison.research_csv_utils import ResearchCsvAnalysis
from src.common.analysis import AnalysisOutput

CATEGORY_RULES: list[tuple[str, str, str]] = [
    ("Crypto", "crypto_assets_and_updown", r"\b(?:bitcoin|btc|ethereum|eth|solana|sol|xrp|doge|dogecoin|crypto|updown|up-or-down|up or down|sui|apt|aptos|avax|bnb|bonk|chainlink|litecoin|near|pepe|shib|toncoin|wif|defi|blockchain)\b"),
    ("Esports", "esports_leagues_and_titles", r"\b(?:esports|league of legends|valorant|counter-strike|counter strike|csgo|cs2|dota|dota2|fortnite|mlbb|mobile legends|iem|blast|lck|lec|lpl|vct|bo3|bo5)\b"),
    ("Sports", "sports_leagues_events_and_terms", r"\b(?:nfl|nba|wnba|mlb|nhl|ncaab|ncaaf|cbb|cfb|soccer|football|tennis|atp|wta|wimbledon|golf|pga|ufc|boxing|fifa|world cup|super bowl|champions league|olympic|cricket|baseball|basketball|hockey|ipl|epl|ucl|uefa|mls|f1|formula 1|nascar|touchdown|field goal|spread|handicap|btts|total games|total rounds|o/u|over/under|grand prix|snooker|darts|horse racing|win gold|gold medal|league cup|la liga|bundesliga|serie a|saudi pro league|leagues cup)\b"),
    ("Weather", "weather_temperature_and_storms", r"\b(?:weather|hurricane|storm|rain|snow|temperature|wildfire|earthquake|climate|tornado|heatwave|heat wave|wind speed|highest temp|highest temperature|low temperature|high temperature)\b"),
    ("Politics", "politics_elections_and_government", r"\b(?:election|president|presidential|trump|biden|senate|house|congress|governor|mayor|nominee|primary|democrat|republican|brexit|poll|vote|voting|cabinet|supreme court|scotus|government shutdown|approval rating|electoral college|popular vote|campaign)\b"),
    ("Finance", "finance_macro_equities_and_rates", r"\b(?:fed|fomc|cpi|inflation|recession|gdp|unemployment|rate cut|interest rate|stock|stocks|nasdaq|s&p|sp500|dow|oil|treasury|yield|economy|market cap|earnings|tesla|nvidia|apple|meta|amazon|google|microsoft)\b"),
    ("Entertainment", "entertainment_awards_music_and_film", r"\b(?:oscar|oscars|grammy|emmy|movie|film|album|billboard|box office|celebrity|netflix|disney|taylor swift|music|song|spotify|golden globe|anime|video game awards)\b"),
    ("Media", "media_social_and_public_attention", r"\b(?:tweet|tweets|twitter|x post|youtube|tiktok|instagram|subscribers|followers|views|likes|most liked|stream|livestream|podcast|headline|nyt|new york times|wikipedia)\b"),
    ("Science/Tech", "science_technology_space_and_health", r"\b(?:spacex|rocket|nasa|starship|satellite|launch|ai|artificial intelligence|gpt|chatgpt|openai|iphone|app store|robotaxi|chip|semiconductor|vaccine|fda|science|covid|pandemic|nobel prize|quantum|neuralink)\b"),
    ("World Events", "world_events_geopolitics_and_conflict", r"\b(?:ukraine|russia|israel|gaza|hamas|china|taiwan|nato|war|ceasefire|sanction|sanctions|iran|north korea|summit|invasion|tariff|united nations|european union|argentina|france|germany|canada|mexico|india|pakistan|south korea|japan)\b"),
]


class Facet1PolymarketCategoryMappingAnalysis(ResearchCsvAnalysis):
    """Map Polymarket questions into coarse categories and recompute calibration."""

    research_subdir = "facet1_polymarket_categories"

    def __init__(self, dataset_path: Path | str | None = None):
        super().__init__(
            name="facet1_polymarket_category_mapping",
            description="Polymarket deterministic category mapping and category calibration",
        )
        self.dataset_path = Path(dataset_path) if dataset_path else None

    def run(self) -> AnalysisOutput:
        df = load_enriched_dataset(self.dataset_path)
        pm_df = df[df["platform"] == "polymarket"].copy()

        market_text = self._load_polymarket_market_text()
        pm_df = pm_df.merge(market_text, on="market_id", how="left")
        pm_df = assign_polymarket_categories(pm_df)

        mapping_df = pm_df[
            ["market_id", "question", "slug", "category_group", "category_rule"]
        ].sort_values(["category_group", "category_rule", "market_id"])

        pm_bucket = compute_bucket_calibration(pm_df, ["category_group"])
        pm_summary = compute_slice_summary(pm_bucket, ["category_group"])
        pm_summary = pm_summary.sort_values(
            ["market_count", "category_group"],
            ascending=[False, True],
        ).reset_index(drop=True)

        cross_df = pd.concat(
            [
                df[df["platform"] == "kalshi"].copy(),
                pm_df.assign(platform="polymarket"),
            ],
            ignore_index=True,
        )
        cross_df["category_group"] = cross_df["category_group"].fillna("Other")
        cross_bucket = compute_bucket_calibration(cross_df, ["platform", "category_group"])
        cross_summary = compute_slice_summary(cross_bucket, ["platform", "category_group"])
        kalshi_summary = cross_summary[cross_summary["platform"] == "kalshi"]
        polymarket_summary = cross_summary[cross_summary["platform"] == "polymarket"]
        cross_comparison = kalshi_summary.merge(
            polymarket_summary,
            on="category_group",
            suffixes=("_kalshi", "_polymarket"),
            how="inner",
        )
        cross_comparison["ece_difference_polymarket_minus_kalshi"] = (
            cross_comparison["expected_calibration_error_polymarket"]
            - cross_comparison["expected_calibration_error_kalshi"]
        )
        cross_comparison["signed_gap_difference_polymarket_minus_kalshi"] = (
            cross_comparison["signed_calibration_gap_polymarket"]
            - cross_comparison["signed_calibration_gap_kalshi"]
        )
        cross_comparison = cross_comparison[
            [
                "category_group",
                "market_count_kalshi",
                "market_count_polymarket",
                "bucket_count_kalshi",
                "bucket_count_polymarket",
                "expected_calibration_error_kalshi",
                "expected_calibration_error_polymarket",
                "ece_difference_polymarket_minus_kalshi",
                "signed_calibration_gap_kalshi",
                "signed_calibration_gap_polymarket",
                "signed_gap_difference_polymarket_minus_kalshi",
                "low_tail_gap_kalshi",
                "low_tail_gap_polymarket",
                "high_tail_gap_kalshi",
                "high_tail_gap_polymarket",
            ]
        ].sort_values("category_group").reset_index(drop=True)

        self.extra_tables = {
            "facet1_polymarket_category_calibration": pm_summary,
            "facet1_cross_platform_category_calibration": cross_comparison,
        }
        return AnalysisOutput(data=mapping_df)

    def _load_polymarket_market_text(self) -> pd.DataFrame:
        con = duckdb.connect()
        query = f"""
        SELECT DISTINCT
            id AS market_id,
            question,
            slug
        FROM '{BASE_DIR / "data" / "polymarket" / "markets"}/*.parquet'
        """
        return con.execute(query).df()


def assign_polymarket_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Assign deterministic coarse categories from question and slug text."""
    df = df.copy()
    text = (
        df["question"].fillna("")
        + " "
        + df["slug"].fillna("")
        + " "
        + df["market_title"].fillna("")
    ).str.lower()

    category = np.full(len(df), "Other", dtype=object)
    rule = np.full(len(df), "fallback_other", dtype=object)
    unmatched = np.ones(len(df), dtype=bool)

    for category_name, rule_name, pattern in CATEGORY_RULES:
        matched = unmatched & text.str.contains(pattern, regex=True, na=False)
        category[matched] = category_name
        rule[matched] = rule_name
        unmatched &= ~matched

    df["category_group"] = category
    df["category_rule"] = rule
    return df
