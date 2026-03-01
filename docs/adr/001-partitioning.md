# ADR 001: Bronze Partitioning by Ingest Date

## Decision
Partition Bronze data by `ingest_date=YYYY-MM-DD`.

## Rationale
- Natural support for append-only batch runs.
- Easier backfills and day-based troubleshooting.
