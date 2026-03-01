from __future__ import annotations

from pathlib import Path

import polars as pl


def read_raw(path: Path) -> pl.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path)
    if suffix == ".json":
        return pl.read_json(path)
    if suffix == ".parquet":
        return pl.read_parquet(path)
    raise ValueError(f"Unsupported input type: {suffix}")
