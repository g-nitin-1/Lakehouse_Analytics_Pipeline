from __future__ import annotations

import polars as pl


def build_gold_tables(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    enriched = df.with_columns(
        pl.col("order_ts").dt.date().alias("order_date"),
        ((pl.col("updated_at") - pl.col("order_ts")).dt.total_minutes())
        .fill_null(0)
        .clip(lower_bound=0)
        .alias("trip_duration_min"),
        (pl.col("amount") / pl.col("quantity")).alias("unit_amount"),
    )

    dim_customer = df.select("customer_id").unique().sort("customer_id")
    dim_vendor = (
        dim_customer.with_columns(
            pl.col("customer_id").alias("vendor_id"),
            pl.col("customer_id").str.split("_").list.last().alias("vendor_code"),
        )
        .select(["vendor_id", "vendor_code"])
        .sort("vendor_id")
    )
    dim_product = enriched.select(["product_id", "category"]).unique().sort("product_id")
    dim_category = (
        enriched.select("category")
        .unique()
        .sort("category")
        .with_row_index(name="category_sk", offset=1)
        .select(["category_sk", "category"])
    )
    dim_date = (
        enriched.select("order_date")
        .unique()
        .sort("order_date")
        .with_columns(
            pl.col("order_date").dt.year().alias("year"),
            pl.col("order_date").dt.month().alias("month"),
            pl.col("order_date").dt.week().alias("week_of_year"),
            pl.col("order_date").dt.weekday().alias("day_of_week"),
        )
    )
    fact_orders = (
        enriched.join(dim_category, on="category", how="left")
        .select(
            [
                "order_id",
                "customer_id",
                "product_id",
                "category_sk",
                "order_ts",
                "order_date",
                "amount",
                "quantity",
                "unit_amount",
                "trip_duration_min",
            ]
        )
        .with_columns((pl.col("amount") * pl.col("quantity")).alias("gross_revenue"))
    )

    return {
        "dim_customer": dim_customer,
        "dim_vendor": dim_vendor,
        "dim_product": dim_product,
        "dim_category": dim_category,
        "dim_date": dim_date,
        "fact_orders": fact_orders,
    }
