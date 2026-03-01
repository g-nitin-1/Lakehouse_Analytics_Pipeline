from __future__ import annotations

from typing import Any

import polars as pl

NUMERIC_TYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
    pl.Float32,
    pl.Float64,
}


def _top_category(series: pl.Series) -> str | None:
    if series.dtype != pl.Utf8:
        return None
    non_null = series.drop_nulls()
    if non_null.len() == 0:
        return None
    counts = non_null.value_counts(sort=True)
    return str(counts[0, series.name])


def _outlier_count(series: pl.Series) -> int | None:
    if series.dtype not in NUMERIC_TYPES:
        return None
    non_null = series.drop_nulls().cast(pl.Float64)
    if non_null.len() < 4:
        return 0
    q1 = non_null.quantile(0.25)
    q3 = non_null.quantile(0.75)
    if q1 is None or q3 is None:
        return 0
    iqr = q3 - q1
    if iqr == 0:
        return 0
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return int(((non_null < lower) | (non_null > upper)).sum())


def profile_dataframe(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    rows: list[dict[str, Any]] = []
    total = max(df.height, 1)

    for name in df.columns:
        series = df[name]
        min_value: Any = None
        max_value: Any = None

        if series.dtype in NUMERIC_TYPES or series.dtype in (pl.Date, pl.Datetime):
            min_value = series.min()
            max_value = series.max()

        rows.append(
            {
                "column": name,
                "dtype": str(series.dtype),
                "null_rate": float(series.null_count() / total),
                "distinct_count": int(series.n_unique()),
                "top_category": _top_category(series),
                "min_value": None if min_value is None else str(min_value),
                "max_value": None if max_value is None else str(max_value),
                "outlier_count": _outlier_count(series),
            }
        )

    profile = pl.DataFrame(rows)
    summary = profile.select(
        [
            pl.col("column"),
            pl.col("null_rate"),
            pl.col("distinct_count"),
            pl.col("outlier_count"),
        ]
    )
    return profile, summary
