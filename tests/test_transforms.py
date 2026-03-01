import polars as pl

from lakehouse_pipeline.transforms.gold import build_gold_tables
from lakehouse_pipeline.transforms.silver import deduplicate_latest
from lakehouse_pipeline.utils.cache import TopKCache
from lakehouse_pipeline.utils.hash import composite_key_hash


def test_deduplicate_keeps_latest_updated_at() -> None:
    df = pl.DataFrame(
        {
            "order_id": ["o1", "o1", "o2"],
            "updated_at": ["2026-01-01T00:00:00", "2026-01-02T00:00:00", "2026-01-01T00:00:00"],
            "order_ts": ["2026-01-01T00:00:00", "2026-01-01T00:00:00", "2026-01-01T00:00:00"],
            "customer_id": ["c1", "c1", "c2"],
            "product_id": ["p1", "p1", "p2"],
            "category": ["x", "x", "y"],
            "amount": [10.0, 11.0, 5.0],
            "quantity": [1, 1, 1],
        }
    ).with_columns(
        pl.col("updated_at").str.strptime(pl.Datetime),
        pl.col("order_ts").str.strptime(pl.Datetime),
    )

    out = deduplicate_latest(df)
    assert out.height == 2
    assert out.filter(pl.col("order_id") == "o1")["amount"][0] == 11.0


def test_composite_key_hash_is_stable() -> None:
    assert composite_key_hash(" A ", "B") == composite_key_hash("a", "b")


def test_topk_cache_hits_and_misses() -> None:
    cache = TopKCache(capacity=2)
    rows = [("x", 1.0), ("y", 3.0), ("z", 2.0)]
    first = cache.get_or_compute("k1", rows, k=2)
    second = cache.get_or_compute("k1", rows, k=2)
    assert first == second
    assert cache.misses == 1
    assert cache.hits == 1


def test_gold_tables_include_enriched_dimensions() -> None:
    df = pl.DataFrame(
        {
            "order_id": ["o1"],
            "customer_id": ["vendor_1"],
            "product_id": ["route_10-20"],
            "order_ts": ["2026-01-01T00:00:00"],
            "updated_at": ["2026-01-01T00:30:00"],
            "category": ["vendor_1"],
            "amount": [20.0],
            "quantity": [2],
        }
    ).with_columns(
        pl.col("order_ts").cast(pl.Datetime),
        pl.col("updated_at").cast(pl.Datetime),
    )
    tables = build_gold_tables(df)
    assert "dim_vendor" in tables
    assert "dim_category" in tables
    assert "trip_duration_min" in tables["fact_orders"].columns
    assert "category_sk" in tables["fact_orders"].columns
