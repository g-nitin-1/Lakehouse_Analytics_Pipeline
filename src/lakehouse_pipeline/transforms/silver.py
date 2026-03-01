from __future__ import annotations

import polars as pl

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_ts",
    "updated_at",
    "category",
    "amount",
    "quantity",
]


def normalize_and_cast(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("order_id").cast(pl.Utf8),
        pl.col("customer_id").cast(pl.Utf8),
        pl.col("product_id").cast(pl.Utf8),
        pl.col("category").cast(pl.Utf8).str.to_lowercase().str.strip_chars(),
        pl.col("amount").cast(pl.Float64),
        pl.col("quantity").cast(pl.Int64),
        pl.col("order_ts").cast(pl.Datetime, strict=False),
        pl.col("updated_at").cast(pl.Datetime, strict=False),
    )


def split_valid_invalid(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    mask = (
        pl.col("order_id").is_not_null()
        & pl.col("customer_id").is_not_null()
        & pl.col("product_id").is_not_null()
        & pl.col("order_ts").is_not_null()
        & pl.col("updated_at").is_not_null()
        & (pl.col("amount") > 0)
        & (pl.col("quantity") > 0)
    )
    return df.filter(mask), df.filter(~mask)


def deduplicate_latest(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.sort(["order_id", "updated_at"], descending=[False, True])
        .unique(subset=["order_id"], keep="first")
        .sort("order_ts")
    )
