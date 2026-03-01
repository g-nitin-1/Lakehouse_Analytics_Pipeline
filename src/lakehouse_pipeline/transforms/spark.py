from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path

from lakehouse_pipeline.config import PipelineConfig


@dataclass(frozen=True)
class SparkRunResult:
    gold_paths: dict[str, Path]
    dedup_removed: int
    join_seconds: float
    total_seconds: float


def _spark_builder() -> object:
    from pyspark.sql import SparkSession

    shuffle_parts = max(8, min(16, (os.cpu_count() or 4)))
    return (
        SparkSession.builder.master("local[*]")
        .appName("lakehouse-pipeline")
        .config("spark.sql.shuffle.partitions", str(shuffle_parts))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.driver.memory", "4g")
    )


def run_spark_pipeline(
    bronze_path: Path,
    config: PipelineConfig,
    incremental: bool = False,
    full_refresh: bool = False,
) -> SparkRunResult:
    from pyspark.sql import Window
    from pyspark.sql import functions as f

    try:
        spark = _spark_builder().getOrCreate()
    except Exception as exc:
        raise RuntimeError(
            "Unable to start Spark. Install a compatible stack with "
            "`pip install -e .[spark]` (project pins pyspark 3.5.x) "
            "or install Java 17+ if using pyspark 4.x."
        ) from exc

    try:
        t0 = time.perf_counter()
        bronze_df = spark.read.parquet(bronze_path.as_posix())

        silver = (
            bronze_df.withColumn("order_id", f.col("order_id").cast("string"))
            .withColumn("customer_id", f.col("customer_id").cast("string"))
            .withColumn("product_id", f.col("product_id").cast("string"))
            .withColumn("category", f.lower(f.trim(f.col("category").cast("string"))))
            .withColumn("amount", f.col("amount").cast("double"))
            .withColumn("quantity", f.col("quantity").cast("long"))
            .withColumn("order_ts", f.to_timestamp("order_ts"))
            .withColumn("updated_at", f.to_timestamp("updated_at"))
        )

        valid_cond = (
            f.col("order_id").isNotNull()
            & f.col("customer_id").isNotNull()
            & f.col("product_id").isNotNull()
            & f.col("order_ts").isNotNull()
            & f.col("updated_at").isNotNull()
            & (f.col("amount") > 0)
            & (f.col("quantity") > 0)
        )

        valid = silver.filter(valid_cond)
        invalid = silver.filter(~valid_cond)

        silver_path = config.silver_dir / "orders"
        gold_paths = {
            "dim_customer": config.gold_dir / "dim_customer",
            "dim_vendor": config.gold_dir / "dim_vendor",
            "dim_product": config.gold_dir / "dim_product",
            "dim_category": config.gold_dir / "dim_category",
            "dim_date": config.gold_dir / "dim_date",
            "fact_orders": config.gold_dir / "fact_orders",
        }
        if incremental and not full_refresh and silver_path.exists():
            existing = spark.read.parquet(silver_path.as_posix())
            watermark = existing.agg(f.max("updated_at")).first()[0]
            if watermark is not None:
                new_rows = valid.filter(f.col("updated_at") > f.lit(watermark))
                if new_rows.limit(1).count() == 0:
                    total_seconds = time.perf_counter() - t0
                    return SparkRunResult(
                        gold_paths=gold_paths,
                        dedup_removed=0,
                        join_seconds=0.0,
                        total_seconds=total_seconds,
                    )
                valid = existing.unionByName(new_rows, allowMissingColumns=True)
            else:
                valid = existing.unionByName(valid, allowMissingColumns=True)

        valid_count = valid.count()
        window = Window.partitionBy("order_id").orderBy(f.col("updated_at").desc())
        dedup = (
            valid.withColumn("rn", f.row_number().over(window))
            .filter(f.col("rn") == 1)
            .drop("rn")
        )
        dedup_count = dedup.count()
        dedup = dedup.cache()
        dedup.count()

        quarantine_path = config.quarantine_dir / "invalid_orders"
        invalid.write.mode("overwrite").parquet(quarantine_path.as_posix())

        dedup_with_date = dedup.withColumn("order_date", f.to_date("order_ts")).repartition(
            4, "order_date"
        )
        (
            dedup_with_date.write.mode("overwrite")
            .partitionBy("order_date")
            .parquet(silver_path.as_posix())
        )

        fact_orders = (
            dedup.withColumn("order_date", f.to_date("order_ts"))
            .withColumn(
                "trip_duration_min",
                f.greatest(
                    f.lit(0.0),
                    (f.col("updated_at").cast("long") - f.col("order_ts").cast("long"))
                    / f.lit(60.0),
                ),
            )
            .withColumn("unit_amount", f.col("amount") / f.col("quantity"))
            .withColumn("gross_revenue", f.col("amount") * f.col("quantity"))
            .select(
                "order_id",
                "customer_id",
                "product_id",
                "category",
                "order_ts",
                "order_date",
                "amount",
                "quantity",
                "unit_amount",
                "trip_duration_min",
                "gross_revenue",
            )
        )

        dim_customer = dedup.select("customer_id").distinct()
        dim_vendor = (
            dim_customer.withColumnRenamed("customer_id", "vendor_id")
            .withColumn("vendor_code", f.element_at(f.split(f.col("vendor_id"), "_"), -1))
            .select("vendor_id", "vendor_code")
            .distinct()
        )
        dim_product = dedup.select("product_id", "category").distinct()
        dim_category = (
            dedup.select("category")
            .distinct()
            .withColumn("category_sk", f.abs(f.xxhash64("category")) + f.lit(1))
            .select("category_sk", "category")
        )
        dim_date = (
            dedup.select(f.to_date("order_ts").alias("order_date"))
            .distinct()
            .withColumn("year", f.year("order_date"))
            .withColumn("month", f.month("order_date"))
            .withColumn("week_of_year", f.weekofyear("order_date"))
            .withColumn("day_of_week", f.dayofweek("order_date"))
        )

        fact_orders = fact_orders.join(f.broadcast(dim_category), on="category", how="left").drop(
            "category"
        )

        join_t0 = time.perf_counter()
        # Avoid full-table action for timing; benchmark on bounded subset.
        join_probe = fact_orders.select("product_id", "customer_id").limit(500_000)
        _ = (
            join_probe.join(f.broadcast(dim_product), on="product_id", how="left")
            .join(f.broadcast(dim_customer), on="customer_id", how="left")
            .count()
        )
        join_seconds = time.perf_counter() - join_t0

        dim_customer.coalesce(1).write.mode("overwrite").parquet(gold_paths["dim_customer"].as_posix())
        dim_vendor.coalesce(1).write.mode("overwrite").parquet(gold_paths["dim_vendor"].as_posix())
        dim_product.coalesce(1).write.mode("overwrite").parquet(gold_paths["dim_product"].as_posix())
        dim_category.coalesce(1).write.mode("overwrite").parquet(
            gold_paths["dim_category"].as_posix()
        )
        dim_date.coalesce(1).write.mode("overwrite").parquet(gold_paths["dim_date"].as_posix())
        (
            fact_orders.repartition(4, "order_date")
            .write.mode("overwrite")
            .partitionBy("order_date")
            .parquet(gold_paths["fact_orders"].as_posix())
        )

        total_seconds = time.perf_counter() - t0
        dedup.unpersist()
        return SparkRunResult(
            gold_paths=gold_paths,
            dedup_removed=valid_count - dedup_count,
            join_seconds=join_seconds,
            total_seconds=total_seconds,
        )
    finally:
        try:
            spark.stop()
        except Exception:
            pass
