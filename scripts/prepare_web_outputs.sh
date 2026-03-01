#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="${1:-output_big}"
DST_DIR="${2:-output_web}"
MAX_ROWS="${MAX_ROWS:-5000}"

echo "Preparing web outputs from: $SRC_DIR -> $DST_DIR (max rows per CSV: $MAX_ROWS)"

if [[ ! -d "$SRC_DIR" ]]; then
  echo "Source directory not found: $SRC_DIR" >&2
  exit 1
fi

rm -rf "$DST_DIR"
mkdir -p "$DST_DIR/kpis" "$DST_DIR/validation" "$DST_DIR/reports"

# Copy reports and charts as-is
if [[ -d "$SRC_DIR/reports" ]]; then
  cp -f "$SRC_DIR/reports"/*.html "$DST_DIR/reports" 2>/dev/null || true
  cp -f "$SRC_DIR/reports"/*.png "$DST_DIR/reports" 2>/dev/null || true
  cp -f "$SRC_DIR/reports"/data_quality_summary.csv "$DST_DIR/reports" 2>/dev/null || true
  cp -f "$SRC_DIR/reports"/data_profile.csv "$DST_DIR/reports" 2>/dev/null || true
fi

python3 - <<PY
from __future__ import annotations
import csv
from pathlib import Path

src = Path("$SRC_DIR")
dst = Path("$DST_DIR")
max_rows = int("$MAX_ROWS")

# Keep all validation files (usually tiny)
val_src = src / "validation"
val_dst = dst / "validation"
if val_src.exists():
    for f in sorted(val_src.glob("*.csv")):
        out = val_dst / f.name
        out.write_bytes(f.read_bytes())

# Curate KPI files: keep all files but truncate potentially large ones
kpi_src = src / "kpis"
kpi_dst = dst / "kpis"
if kpi_src.exists():
    for f in sorted(kpi_src.glob("*.csv")):
        out = kpi_dst / f.name
        with f.open("r", newline="", encoding="utf-8") as rf, out.open("w", newline="", encoding="utf-8") as wf:
            reader = csv.reader(rf)
            writer = csv.writer(wf)
            for i, row in enumerate(reader):
                writer.writerow(row)
                if i >= max_rows:
                    break

# Minimal manifest for transparency
manifest = dst / "manifest.txt"
lines = [
    f"source={src}",
    f"max_rows_per_csv={max_rows}",
    "note=output_web is a curated subset for Streamlit Cloud browsing",
]
manifest.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

echo "Done. Created curated web artifacts in: $DST_DIR"
