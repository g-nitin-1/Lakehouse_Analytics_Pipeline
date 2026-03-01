from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from lakehouse_pipeline.io.writers import write_parquet


def to_bronze(df: pl.DataFrame, source_file: Path) -> pl.DataFrame:
    ingest_ts = datetime.now(timezone.utc).isoformat()
    bronze_df = df.with_columns(
        pl.lit(source_file.name).alias("source_file"),
        pl.lit(ingest_ts).alias("ingest_time"),
    )
    # Spark 3.5.x cannot read Parquet TIMESTAMP_NANOS; normalize datetimes to micros.
    datetime_cols = [
        col
        for col, dtype in bronze_df.schema.items()
        if isinstance(dtype, pl.datatypes.Datetime)
    ]
    if datetime_cols:
        bronze_df = bronze_df.with_columns(
            [pl.col(col).cast(pl.Datetime("us"), strict=False) for col in datetime_cols]
        )
    return bronze_df


def persist_bronze(df: pl.DataFrame, output_dir: Path) -> Path:
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    target = output_dir / f"ingest_date={ingest_date}" / "orders.parquet"
    write_parquet(df, target)
    return target
