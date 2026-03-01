from pathlib import Path

import polars as pl

from lakehouse_pipeline.transforms.schema_adapter import adapt_to_canonical_schema
from lakehouse_pipeline.transforms.silver import REQUIRED_COLUMNS


def test_adapter_passthrough_for_canonical() -> None:
    df = pl.DataFrame(
        {
            "order_id": ["o1"],
            "customer_id": ["c1"],
            "product_id": ["p1"],
            "order_ts": ["2026-01-01T00:00:00"],
            "updated_at": ["2026-01-01T00:00:00"],
            "category": ["books"],
            "amount": [10.0],
            "quantity": [1],
        }
    )
    out, kind = adapt_to_canonical_schema(df, Path("sample.csv"))
    assert kind == "canonical"
    assert out.columns == REQUIRED_COLUMNS


def test_adapter_maps_tlc_schema() -> None:
    df = pl.DataFrame(
        {
            "VendorID": [1],
            "tpep_pickup_datetime": ["2024-01-01 00:00:00"],
            "tpep_dropoff_datetime": ["2024-01-01 00:15:00"],
            "PULocationID": [100],
            "DOLocationID": [200],
            "total_amount": [25.5],
            "fare_amount": [20.0],
            "passenger_count": [2],
        }
    )
    out, kind = adapt_to_canonical_schema(df, Path("yellow_tripdata.parquet"))
    assert kind == "nyc_tlc_yellow_taxi"
    assert out.columns == REQUIRED_COLUMNS
    assert out["amount"][0] == 25.5
    assert out["quantity"][0] == 2
