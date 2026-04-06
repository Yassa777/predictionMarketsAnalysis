"""Data Dictionary for Polymarket and Kalshi datasets."""

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Data Dictionary", page_icon="\U0001F4DA", layout="wide")

st.title("Data Dictionary")
st.caption("Column definitions, types, and statistics for all datasets.")

# ---------------------------------------------------------------------------
# Dataset definitions – all stats are pre-computed so there are no heavy
# queries at page-load time.
# ---------------------------------------------------------------------------

POLYMARKET_DATASETS = {
    "Markets": {
        "description": "Each row represents a prediction market snapshot.",
        "rows": "408K",
        "rows_exact": "408,863",
        "files": 41,
        "size": "102 MB",
        "date_range": "2024-05 \u2013 2025-05",
        "columns": pd.DataFrame([
            ["id", "VARCHAR", "Unique market ID", "100%"],
            ["condition_id", "VARCHAR", "Condition ID (hex hash)", "100%"],
            ["question", "VARCHAR", "Market question text", "100%"],
            ["slug", "VARCHAR", "URL slug for the market page", "100%"],
            ["outcomes", "VARCHAR", "JSON array of outcome names, e.g. '[\"Yes\",\"No\"]'", "100%"],
            ["outcome_prices", "VARCHAR", "JSON array of current prices, e.g. '[\"0.65\",\"0.35\"]'", "100%"],
            ["clob_token_ids", "VARCHAR", "JSON array of CLOB token IDs for each outcome", "100%"],
            ["volume", "DOUBLE", "Total volume traded (USD)", "100%"],
            ["liquidity", "DOUBLE", "Current liquidity pool size (USD)", "100%"],
            ["active", "BOOLEAN", "Whether the market is currently active", "100%"],
            ["closed", "BOOLEAN", "Whether the market is closed", "100%"],
            ["end_date", "TIMESTAMP WITH TIME ZONE", "When the market ends", "99.6%"],
            ["created_at", "TIMESTAMP WITH TIME ZONE", "When the market was created", "90.1%"],
            ["market_maker_address", "VARCHAR", "Contract address of the market maker", "100%"],
            ["_fetched_at", "TIMESTAMP_NS", "When this record was fetched by the pipeline", "100%"],
        ], columns=["Column", "Type", "Description", "Non-Null %"]),
    },
    "Trades": {
        "description": "Each row is an `OrderFilled` event from Polygon. Covers both the CTF Exchange and Neg Risk Exchange contracts.",
        "rows": "404M",
        "rows_exact": "404,000,000+",
        "files": 40454,
        "size": "45 GB",
        "date_range": "2024-05 \u2013 2025-05",
        "note": "Prices are decimals between 0 and 1. Amounts use 6 decimal places (USDC).",
        "columns": pd.DataFrame([
            ["block_number", "BIGINT", "Polygon block number", "100%"],
            ["transaction_hash", "VARCHAR", "Blockchain transaction hash", "100%"],
            ["log_index", "BIGINT", "Log index within the transaction", "100%"],
            ["order_hash", "VARCHAR", "Unique order identifier", "100%"],
            ["maker", "VARCHAR", "Address of the limit-order placer", "100%"],
            ["taker", "VARCHAR", "Address that filled the order", "100%"],
            ["maker_asset_id", "VARCHAR", "Asset ID the maker provided (0 = USDC)", "100%"],
            ["taker_asset_id", "VARCHAR", "Asset ID the taker provided", "100%"],
            ["maker_amount", "BIGINT", "Amount the maker gave (6 decimals)", "100%"],
            ["taker_amount", "BIGINT", "Amount the taker gave (6 decimals)", "100%"],
            ["fee", "BIGINT", "Trading fee (6 decimals)", "100%"],
            ["timestamp", "INTEGER", "Unix timestamp of the block", "100%"],
            ["_fetched_at", "TIMESTAMP_NS", "When this record was fetched by the pipeline", "100%"],
            ["_contract", "VARCHAR", "Source contract: 'CTF Exchange' or 'NegRisk'", "100%"],
        ], columns=["Column", "Type", "Description", "Non-Null %"]),
    },
    "Legacy Trades (FPMM)": {
        "description": "Each row is an `FPMMBuy` or `FPMMSell` event from the legacy Fixed Product Market Maker contracts (~2020\u20132022).",
        "rows": "2.2M",
        "rows_exact": "2,207,336",
        "files": 221,
        "size": "211 MB",
        "date_range": "2020-09 \u2013 2022-06",
        "note": "amount, fee_amount, and outcome_tokens are strings to avoid integer overflow. Collateral uses 6 decimals (USDC); outcome tokens use 18 decimals.",
        "columns": pd.DataFrame([
            ["block_number", "BIGINT", "Polygon block number", "100%"],
            ["transaction_hash", "VARCHAR", "Blockchain transaction hash", "100%"],
            ["log_index", "BIGINT", "Log index within the transaction", "100%"],
            ["fpmm_address", "VARCHAR", "FPMM contract (market) address", "100%"],
            ["trader", "VARCHAR", "Buyer or seller address", "100%"],
            ["amount", "VARCHAR", "Investment amount (buy) or return amount (sell) in collateral units", "100%"],
            ["fee_amount", "VARCHAR", "Trading fee in collateral units", "100%"],
            ["outcome_index", "BIGINT", "Index of the outcome traded (0 or 1)", "100%"],
            ["outcome_tokens", "VARCHAR", "Outcome tokens bought or sold (18 decimals)", "100%"],
            ["is_buy", "BOOLEAN", "True for buy, False for sell", "100%"],
            ["timestamp", "INTEGER", "Unix timestamp (if enriched via block lookup)", "0%"],
            ["_fetched_at", "TIMESTAMP_NS", "When this record was fetched by the pipeline", "100%"],
        ], columns=["Column", "Type", "Description", "Non-Null %"]),
    },
    "Blocks": {
        "description": "Mapping from Polygon block numbers to timestamps. Used to enrich trades with human-readable times.",
        "rows": "78M",
        "rows_exact": "78,468,431",
        "files": 785,
        "size": "843 MB",
        "date_range": "Block 17\u2009000\u2009000 \u2013 95\u2009000\u2009000+",
        "columns": pd.DataFrame([
            ["block_number", "BIGINT", "Polygon block number", "100%"],
            ["timestamp", "VARCHAR", "ISO 8601 timestamp, e.g. '2024-01-15T12:30:00Z'", "100%"],
        ], columns=["Column", "Type", "Description", "Non-Null %"]),
    },
}

KALSHI_DATASETS = {
    "Markets": {
        "description": "Each row represents a prediction-market contract snapshot.",
        "rows": "7.7M",
        "rows_exact": "7,682,445",
        "files": 769,
        "size": "570 MB",
        "date_range": "2024-05 \u2013 2025-05",
        "note": "Prices are in cents (1\u201399). A yes_bid of 65 means the contract costs $0.65.",
        "columns": pd.DataFrame([
            ["ticker", "VARCHAR", "Unique market identifier, e.g. 'PRES-2024-DJT'", "100%"],
            ["event_ticker", "VARCHAR", "Parent event identifier for categorization", "100%"],
            ["market_type", "VARCHAR", "Market type (typically 'binary')", "100%"],
            ["title", "VARCHAR", "Human-readable market title", "100%"],
            ["yes_sub_title", "VARCHAR", "Label for the Yes outcome", "100%"],
            ["no_sub_title", "VARCHAR", "Label for the No outcome", "100%"],
            ["status", "VARCHAR", "Market status: open, closed, or finalized", "100%"],
            ["yes_bid", "BIGINT", "Best bid price for Yes contracts (cents, 1\u201399)", "100%"],
            ["yes_ask", "BIGINT", "Best ask price for Yes contracts (cents, 1\u201399)", "100%"],
            ["no_bid", "BIGINT", "Best bid price for No contracts (cents, 1\u201399)", "100%"],
            ["no_ask", "BIGINT", "Best ask price for No contracts (cents, 1\u201399)", "100%"],
            ["last_price", "BIGINT", "Last traded price (cents, 1\u201399)", "100%"],
            ["volume", "BIGINT", "Total contracts ever traded", "100%"],
            ["volume_24h", "BIGINT", "Contracts traded in the last 24 hours", "100%"],
            ["open_interest", "BIGINT", "Currently outstanding contracts", "100%"],
            ["result", "VARCHAR", "Market outcome: 'yes', 'no', or empty if unresolved", "100%"],
            ["created_time", "TIMESTAMP WITH TIME ZONE", "When the market was created", "100%"],
            ["open_time", "TIMESTAMP WITH TIME ZONE", "When trading opened", "100%"],
            ["close_time", "TIMESTAMP WITH TIME ZONE", "When trading closed", "100%"],
            ["_fetched_at", "TIMESTAMP_NS", "When this record was fetched by the pipeline", "100%"],
        ], columns=["Column", "Type", "Description", "Non-Null %"]),
    },
    "Trades": {
        "description": "Each row represents a single trade execution.",
        "rows": "72M",
        "rows_exact": "72,134,741",
        "files": 7214,
        "size": "3.3 GB",
        "date_range": "2024-05 \u2013 2025-05",
        "note": "no_price is always 100 \u2212 yes_price.",
        "columns": pd.DataFrame([
            ["trade_id", "VARCHAR", "Unique trade identifier", "100%"],
            ["ticker", "VARCHAR", "Market ticker this trade belongs to", "100%"],
            ["count", "BIGINT", "Number of contracts traded", "100%"],
            ["yes_price", "BIGINT", "Yes contract price (cents, 1\u201399)", "100%"],
            ["no_price", "BIGINT", "No contract price (cents, 1\u201399)", "100%"],
            ["taker_side", "VARCHAR", "Which side the taker bought: 'yes' or 'no'", "100%"],
            ["created_time", "TIMESTAMP WITH TIME ZONE", "When the trade occurred", "100%"],
            ["_fetched_at", "TIMESTAMP_NS", "When this record was fetched by the pipeline", "100%"],
        ], columns=["Column", "Type", "Description", "Non-Null %"]),
    },
}

# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def _render_dataset(name: str, info: dict) -> None:
    with st.expander(f"**{name}** \u2014 {info['rows']} rows, {info['files']:,} files, {info['size']}", expanded=False):
        # Overview metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Rows", info["rows_exact"])
        c2.metric("Files", f"{info['files']:,}")
        c3.metric("Size on disk", info["size"])
        c4.metric("Date range", info["date_range"])

        if "note" in info:
            st.info(info["note"], icon="\u2139\ufe0f")

        # Column table
        st.dataframe(
            info["columns"],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Column": st.column_config.TextColumn(width="medium"),
                "Type": st.column_config.TextColumn(width="medium"),
                "Description": st.column_config.TextColumn(width="large"),
                "Non-Null %": st.column_config.TextColumn(width="small"),
            },
        )

# ---------------------------------------------------------------------------
# Main layout
# ---------------------------------------------------------------------------

tab_poly, tab_kalshi = st.tabs(["Polymarket", "Kalshi"])

with tab_poly:
    st.header("Polymarket")
    st.caption("On-chain prediction market data from Polygon.")
    for name, info in POLYMARKET_DATASETS.items():
        _render_dataset(name, info)

with tab_kalshi:
    st.header("Kalshi")
    st.caption("Regulated exchange prediction market data.")
    for name, info in KALSHI_DATASETS.items():
        _render_dataset(name, info)
