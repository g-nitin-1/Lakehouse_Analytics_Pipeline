# Architecture

```mermaid
flowchart LR
    A[Raw Dataset\nCSV / JSON / Parquet] --> B[Ingestion Layer\nPolars readers + metadata]
    B --> C[Bronze Layer\nRaw + source_file + ingest_time\nParquet partitioned by ingest_date]
    C --> D[Silver Layer\nType casting, normalization,\nvalidation, quarantine, dedup]
    D --> E[Quarantine\nInvalid Rows]
    D --> F[Gold Layer Star Schema\nfact_orders + dim_customer + dim_product + dim_date]

    F --> G[Analytics SQL\nDuckDB KPI + Cohort + Validation]
    F --> H[Polars Profiling\nnull rates, distincts, top categories,\nmin/max, outliers]
    G --> I[Output Tables\noutput/kpis + output/validation]
    H --> J[Data Quality Outputs\nCSV + HTML]
    I --> K[Final Report\noutput/reports/report.html + charts]
    J --> K

    C -. optional engine .-> D
    C -. spark engine .-> L[PySpark Path\nbroadcast joins + partitioned writes + stage timing]
    L --> F
```

## Layer Responsibilities

- Ingestion: reads raw files and appends ingest metadata.
- Bronze: immutable raw records with ingestion context.
- Silver: standardized schema, validation checks, deterministic latest-row dedup, invalid-row quarantine.
- Gold: analytics-ready star schema for KPI computation.
- Analytics: DuckDB executes KPI, cohort retention, and validation SQL deliverables.
- Profiling and reporting: Polars profiling plus a reproducible HTML report with charts.
