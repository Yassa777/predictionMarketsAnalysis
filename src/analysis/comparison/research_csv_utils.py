"""Shared helpers for compact research CSV analysis outputs."""

from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.analysis import Analysis

BASE_DIR = Path(__file__).resolve().parents[3]
RESEARCH_TABLE_DIR = BASE_DIR / "research" / "tables"
MAX_PUBLISHED_CSV_BYTES = 95 * 1024 * 1024
CSV_SPLIT_ROWS = 250_000


class ResearchCsvAnalysis(Analysis):
    """Analysis base class for CSV-only research packets."""

    research_subdir: str = "remote_packet"
    extra_tables: dict[str, pd.DataFrame]

    def normalize_formats(self, formats: list[str] | None) -> list[str]:
        if formats is None:
            return ["csv"]
        return [fmt for fmt in formats if fmt == "csv"]

    def save(
        self,
        output_dir: Path | str,
        formats: list[str] | None = None,
        dpi: int = 300,
    ) -> dict[str, Path]:
        self.extra_tables = {}
        output_dir = Path(output_dir)
        normalized_formats = self.normalize_formats(formats)
        saved = super().save(output_dir, normalized_formats, dpi)

        if "csv" in normalized_formats:
            for table_name, table_df in self.extra_tables.items():
                path = output_dir / f"{table_name}.csv"
                table_df.to_csv(path, index=False)
                saved[f"{table_name}_csv"] = path

        saved.update(publish_research_tables(saved, self.research_subdir))
        return saved


def publish_research_tables(saved: dict[str, Path], subdir: str) -> dict[str, Path]:
    """Copy CSV outputs to a tracked research/tables subdirectory."""
    published: dict[str, Path] = {}
    destination_dir = RESEARCH_TABLE_DIR / subdir
    destination_dir.mkdir(parents=True, exist_ok=True)

    for key, path in saved.items():
        if path.suffix.lower() != ".csv" or not path.exists():
            continue
        if path.stat().st_size > MAX_PUBLISHED_CSV_BYTES:
            destinations = split_csv_for_research(path, destination_dir)
            for idx, destination in enumerate(destinations, start=1):
                published[f"published_{key}_part_{idx:03d}"] = destination
            continue

        destination = destination_dir / path.name
        remove_existing_split_parts(destination_dir, path.stem)
        shutil.copy2(path, destination)
        published[f"published_{key}"] = destination

    return published


def split_csv_for_research(source: Path, destination_dir: Path) -> list[Path]:
    """Split a large CSV into GitHub-safe chunk files with headers."""
    destination = destination_dir / source.name
    if destination.exists():
        destination.unlink()
    remove_existing_split_parts(destination_dir, source.stem)

    destinations: list[Path] = []
    for idx, chunk in enumerate(pd.read_csv(source, chunksize=CSV_SPLIT_ROWS), start=1):
        chunk_path = destination_dir / f"{source.stem}_part_{idx:03d}.csv"
        chunk.to_csv(chunk_path, index=False)
        destinations.append(chunk_path)
    return destinations


def remove_existing_split_parts(destination_dir: Path, stem: str) -> None:
    for old_part in destination_dir.glob(f"{stem}_part_*.csv"):
        old_part.unlink()


def add_quantile_bucket(
    df: pd.DataFrame,
    value_col: str,
    bucket_col: str,
    labels: list[str],
) -> pd.DataFrame:
    """Assign stable quantile buckets with a deterministic rank fallback."""
    df = df.copy()
    valid = df[value_col].notna()
    df[bucket_col] = pd.NA
    if valid.sum() == 0:
        return df

    ranks = df.loc[valid, value_col].rank(method="first")
    bucket_count = min(len(labels), int(valid.sum()))
    assigned = pd.qcut(ranks, bucket_count, labels=labels[:bucket_count])
    df.loc[valid, bucket_col] = assigned.astype("object")
    return df


def brier_score(y_true: pd.Series, y_pred: pd.Series) -> float:
    pred = y_pred.clip(1e-6, 1 - 1e-6)
    return float(np.mean((y_true.astype(float) - pred.astype(float)) ** 2))


def log_loss(y_true: pd.Series, y_pred: pd.Series) -> float:
    pred = y_pred.clip(1e-6, 1 - 1e-6)
    y = y_true.astype(float)
    return float(np.mean(-(y * np.log(pred) + (1 - y) * np.log(1 - pred))))
