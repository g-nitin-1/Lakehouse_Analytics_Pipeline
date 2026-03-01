from __future__ import annotations

from pathlib import Path

import polars as pl


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_parquet(df: pl.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.write_parquet(path)


def write_csv(df: pl.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.write_csv(path)


def write_text(text: str, path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")
