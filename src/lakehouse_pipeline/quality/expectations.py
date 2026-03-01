from __future__ import annotations

import polars as pl


def validate_required_columns(df: pl.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
