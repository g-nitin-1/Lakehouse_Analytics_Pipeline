from __future__ import annotations

from pathlib import Path

import polars as pl

from lakehouse_pipeline.transforms.silver import REQUIRED_COLUMNS

TLC_REQUIRED = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
}


def adapt_to_canonical_schema(df: pl.DataFrame, source_path: Path) -> tuple[pl.DataFrame, str]:
    cols = set(df.columns)

    if set(REQUIRED_COLUMNS).issubset(cols):
        return df, "canonical"

    if TLC_REQUIRED.issubset(cols):
        mapped = df.with_columns(
            pl.concat_str(
                [
                    pl.lit("trip_"),
                    pl.col("VendorID").cast(pl.Utf8),
                    pl.col("tpep_pickup_datetime").cast(pl.Utf8),
                    pl.col("PULocationID").cast(pl.Utf8),
                    pl.col("DOLocationID").cast(pl.Utf8),
                ],
                separator="|",
            ).alias("order_id"),
            pl.concat_str([pl.lit("vendor_"), pl.col("VendorID").cast(pl.Utf8)]).alias(
                "customer_id"
            ),
            pl.concat_str(
                [
                    pl.lit("route_"),
                    pl.col("PULocationID").cast(pl.Utf8),
                    pl.col("DOLocationID").cast(pl.Utf8),
                ],
                separator="-",
            ).alias("product_id"),
            pl.col("tpep_pickup_datetime").alias("order_ts"),
            pl.coalesce([
                pl.col("tpep_dropoff_datetime"),
                pl.col("tpep_pickup_datetime"),
            ]).alias("updated_at"),
            pl.concat_str([pl.lit("vendor_"), pl.col("VendorID").cast(pl.Utf8)]).alias(
                "category"
            ),
            pl.coalesce(
                [
                    pl.col("total_amount").cast(pl.Float64, strict=False),
                    pl.col("fare_amount").cast(pl.Float64, strict=False),
                ]
            ).alias("amount"),
            pl.coalesce(
                [
                    pl.col("passenger_count").cast(pl.Int64, strict=False),
                    pl.lit(1),
                ]
            ).alias("quantity"),
        ).select(REQUIRED_COLUMNS)
        return mapped, "nyc_tlc_yellow_taxi"

    raise ValueError(
        f"Unsupported schema for input {source_path}. "
        f"Expected canonical columns {REQUIRED_COLUMNS} or NYC TLC yellow schema."
    )
