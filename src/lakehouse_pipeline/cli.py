from __future__ import annotations

import argparse
import glob
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import polars as pl

from lakehouse_pipeline.config import PipelineConfig
from lakehouse_pipeline.io.readers import read_raw
from lakehouse_pipeline.io.writers import ensure_dir, write_csv, write_parquet, write_text
from lakehouse_pipeline.quality.expectations import validate_required_columns
from lakehouse_pipeline.quality.profiling import profile_dataframe
from lakehouse_pipeline.reporting import build_html_report, build_quality_html
from lakehouse_pipeline.transforms.bronze import persist_bronze, to_bronze
from lakehouse_pipeline.transforms.gold import build_gold_tables
from lakehouse_pipeline.transforms.schema_adapter import adapt_to_canonical_schema
from lakehouse_pipeline.transforms.silver import (
    REQUIRED_COLUMNS,
    deduplicate_latest,
    normalize_and_cast,
    split_valid_invalid,
)
from lakehouse_pipeline.transforms.spark import run_spark_pipeline
from lakehouse_pipeline.utils.cache import TopKCache
from lakehouse_pipeline.utils.logging import get_logger

logger = get_logger("lakehouse_pipeline")


def _query_to_polars(con: duckdb.DuckDBPyConnection, query: str) -> pl.DataFrame:
    relation = con.execute(query)
    rows = relation.fetchall()
    columns = [desc[0] for desc in relation.description]
    return pl.DataFrame(rows, schema=columns, orient="row")


def _parse_named_queries(sql_text: str) -> dict[str, str]:
    queries: dict[str, str] = {}
    current_name: str | None = None
    current_lines: list[str] = []

    for line in sql_text.splitlines():
        if line.strip().startswith("-- name:"):
            if current_name and current_lines:
                queries[current_name] = "\n".join(current_lines).strip()
            current_name = line.split(":", 1)[1].strip()
            current_lines = []
            continue
        current_lines.append(line)

    if current_name and current_lines:
        queries[current_name] = "\n".join(current_lines).strip()

    return queries


def _run_named_queries(
    con: duckdb.DuckDBPyConnection,
    sql_text: str,
    label: str,
) -> dict[str, pl.DataFrame]:
    outputs: dict[str, pl.DataFrame] = {}
    for name, query in _parse_named_queries(sql_text).items():
        logger.info("Running %s query: %s", label, name)
        outputs[name] = _query_to_polars(con, query)
    return outputs


def _parquet_read_pattern(path: Path) -> str:
    if path.is_file():
        return path.as_posix()
    files = glob.glob(f"{path.as_posix()}/**/*.parquet", recursive=True)
    if files:
        return f"{path.as_posix()}/**/*.parquet"
    return f"{path.as_posix()}/*.parquet"


def _get_incremental_watermark(silver_orders_path: Path) -> object | None:
    if not silver_orders_path.exists():
        return None
    pattern = _parquet_read_pattern(silver_orders_path)
    try:
        existing = pl.read_parquet(pattern)
    except Exception:
        return None
    if existing.height == 0 or "updated_at" not in existing.columns:
        return None
    return existing.select(pl.max("updated_at")).item()


def _align_to_columns(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    additions = [pl.lit(None).alias(col) for col in columns if col not in df.columns]
    if additions:
        df = df.with_columns(additions)
    return df.select(columns)


def _cached_topk(
    cache: TopKCache,
    key: str,
    df: pl.DataFrame,
    label_col: str,
    value_col: str,
    k: int = 5,
) -> pl.DataFrame:
    rows = [
        (str(label), float(value))
        for label, value in df.select(label_col, value_col).iter_rows()
    ]
    topk = cache.get_or_compute(key=key, rows=rows, k=k)
    return pl.DataFrame(
        {
            label_col: [label for label, _ in topk],
            value_col: [value for _, value in topk],
        }
    )


def _plot_line(df: pl.DataFrame, x_col: str, y_col: str, title: str, out_path: Path) -> Path:
    ensure_dir(out_path.parent)
    plt.figure(figsize=(8, 4))
    plt.plot([str(v) for v in df[x_col].to_list()], df[y_col].to_list())
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    return out_path


def _plot_bar(df: pl.DataFrame, x_col: str, y_col: str, title: str, out_path: Path) -> Path:
    ensure_dir(out_path.parent)
    plt.figure(figsize=(8, 4))
    plt.bar([str(v) for v in df[x_col].to_list()], df[y_col].to_list())
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    return out_path


def run_pipeline(
    config: PipelineConfig,
    engine: str = "polars",
    incremental: bool = False,
    full_refresh: bool = False,
) -> None:
    ensure_dir(config.output_root)

    raw_df = read_raw(config.input_path)
    logger.info("Ingest complete: %s rows", raw_df.height)
    canonical_df, dataset_type = adapt_to_canonical_schema(raw_df, config.input_path)
    logger.info("Input schema adapter: %s", dataset_type)

    bronze_df = to_bronze(canonical_df, config.input_path)
    bronze_path = persist_bronze(bronze_df, config.bronze_dir)
    logger.info("Bronze write complete: %s", bronze_path)

    validate_required_columns(bronze_df, REQUIRED_COLUMNS)

    gold_paths: dict[str, Path] = {}
    silver_orders_path: Path
    if engine == "spark":
        logger.info("Using Spark engine for Silver/Gold")
        if full_refresh:
            logger.info("Full refresh enabled; skipping incremental short-circuit.")
        spark_result = run_spark_pipeline(
            bronze_path=bronze_path,
            config=config,
            incremental=incremental,
            full_refresh=full_refresh,
        )
        gold_paths = spark_result.gold_paths
        logger.info("Silver dedup removed %s rows", spark_result.dedup_removed)
        logger.info(
            "Spark timings: total=%.2fs, join_stage=%.2fs",
            spark_result.total_seconds,
            spark_result.join_seconds,
        )
        silver_orders_path = config.silver_dir / "orders"
        dedup_df = pl.read_parquet(_parquet_read_pattern(silver_orders_path))
    else:
        silver_df = normalize_and_cast(bronze_df)
        valid_df, invalid_df = split_valid_invalid(silver_df)

        silver_orders_path = config.silver_dir / "orders.parquet"
        if incremental and not full_refresh:
            watermark = _get_incremental_watermark(silver_orders_path)
            if watermark is not None:
                valid_df = valid_df.filter(pl.col("updated_at") > pl.lit(watermark))
                logger.info("Incremental watermark: %s", watermark)
            if silver_orders_path.exists():
                existing_df = pl.read_parquet(silver_orders_path)
                aligned_cols = sorted(set(existing_df.columns) | set(valid_df.columns))
                valid_df = pl.concat(
                    [
                        _align_to_columns(existing_df, aligned_cols),
                        _align_to_columns(valid_df, aligned_cols),
                    ],
                    how="vertical_relaxed",
                )
        dedup_df = deduplicate_latest(valid_df)

        write_parquet(dedup_df, silver_orders_path)
        write_parquet(invalid_df, config.quarantine_dir / "invalid_orders.parquet")
        logger.info("Silver dedup removed %s rows", valid_df.height - dedup_df.height)

        gold_tables = build_gold_tables(dedup_df)
        for table_name, table_df in gold_tables.items():
            target = config.gold_dir / f"{table_name}.parquet"
            write_parquet(table_df, target)
            gold_paths[table_name] = target
            logger.info("Gold table %s: %s rows", table_name, table_df.height)

    con = duckdb.connect(database=":memory:")
    con.execute(
        f"CREATE OR REPLACE VIEW bronze_orders AS "
        f"SELECT * FROM read_parquet('{_parquet_read_pattern(bronze_path)}')"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW silver_orders AS "
        f"SELECT * FROM read_parquet('{_parquet_read_pattern(silver_orders_path)}')"
    )

    for table_name, table_path in gold_paths.items():
        read_pattern = _parquet_read_pattern(table_path)
        con.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS "
            f"SELECT * FROM read_parquet('{read_pattern}')"
        )

    sql_dir = Path(__file__).parent / "sql"

    kpi_text = (sql_dir / "kpis.sql").read_text(encoding="utf-8")
    kpi_results = _run_named_queries(con, kpi_text, label="KPI")
    ensure_dir(config.kpi_dir)
    for name, result in kpi_results.items():
        write_csv(result, config.kpi_dir / f"{name}.csv")

    retention_sql = (sql_dir / "cohort_retention.sql").read_text(encoding="utf-8")
    retention_df = _query_to_polars(con, retention_sql)
    write_csv(retention_df, config.kpi_dir / "cohort_retention.csv")

    validation_sql = (sql_dir / "validation.sql").read_text(encoding="utf-8")
    validation_results = _run_named_queries(con, validation_sql, label="validation")
    ensure_dir(config.validation_dir)
    for name, result in validation_results.items():
        write_csv(result, config.validation_dir / f"{name}.csv")

    profile, dq_summary = profile_dataframe(dedup_df)
    write_csv(dq_summary, config.report_dir / "data_quality_summary.csv")
    write_csv(profile, config.report_dir / "data_profile.csv")
    write_text(
        build_quality_html(dq_summary, profile),
        config.report_dir / "data_quality_summary.html",
    )

    cache = TopKCache(capacity=8)
    top_categories = _cached_topk(
        cache,
        key="kpi_top_categories",
        df=kpi_results["kpi_top_categories"],
        label_col="category",
        value_col="revenue",
        k=5,
    )
    top_products = _cached_topk(
        cache,
        key="kpi_top_products",
        df=kpi_results["kpi_top_products"],
        label_col="product_id",
        value_col="revenue",
        k=5,
    )

    charts = [
        _plot_line(
            kpi_results["kpi_daily_revenue"],
            x_col="order_day",
            y_col="revenue",
            title="Daily Revenue",
            out_path=config.report_dir / "daily_revenue.png",
        ),
        _plot_bar(
            top_categories,
            x_col="category",
            y_col="revenue",
            title="Top Categories",
            out_path=config.report_dir / "top_categories.png",
        ),
        _plot_line(
            kpi_results["kpi_revenue_by_week"],
            x_col="order_week",
            y_col="revenue",
            title="Weekly Revenue",
            out_path=config.report_dir / "weekly_revenue.png",
        ),
        _plot_bar(
            top_products,
            x_col="product_id",
            y_col="revenue",
            title="Top Products",
            out_path=config.report_dir / "top_products.png",
        ),
    ]

    # Intentionally repeated to validate cache use on repeated KPI computations.
    _ = _cached_topk(
        cache,
        key="kpi_top_categories",
        df=kpi_results["kpi_top_categories"],
        label_col="category",
        value_col="revenue",
        k=5,
    )
    _ = _cached_topk(
        cache,
        key="kpi_top_products",
        df=kpi_results["kpi_top_products"],
        label_col="product_id",
        value_col="revenue",
        k=5,
    )
    logger.info("KPI cache stats: hits=%s misses=%s", cache.hits, cache.misses)

    write_text(
        build_html_report(
            kpi_results=kpi_results,
            validation_results=validation_results,
            dq_summary=dq_summary,
            profile=profile,
            chart_paths=charts,
        ),
        config.report_dir / "report.html",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lakehouse batch pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Execute pipeline")
    run.add_argument("--input", required=True, type=Path)
    run.add_argument("--output", required=True, type=Path)
    run.add_argument("--engine", choices=["polars", "spark"], default="polars")
    run.add_argument(
        "--incremental",
        action="store_true",
        help="Use incremental load with updated_at watermark from existing Silver output.",
    )
    run.add_argument(
        "--full-refresh",
        action="store_true",
        help="Force full recomputation and ignore incremental short-circuit behavior.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "run":
        cfg = PipelineConfig(input_path=args.input, output_root=args.output)
        run_pipeline(
            cfg,
            engine=args.engine,
            incremental=args.incremental,
            full_refresh=args.full_refresh,
        )


if __name__ == "__main__":
    main()
