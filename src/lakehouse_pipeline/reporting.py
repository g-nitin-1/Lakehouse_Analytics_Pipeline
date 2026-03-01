from __future__ import annotations

from html import escape
from pathlib import Path

import polars as pl


def dataframe_to_html_table(df: pl.DataFrame, limit: int = 20) -> str:
    clipped = df.head(limit)
    header = "".join(f"<th>{escape(col)}</th>" for col in clipped.columns)
    body_rows = []
    for row in clipped.iter_rows():
        cells = "".join(f"<td>{escape(str(v))}</td>" for v in row)
        body_rows.append(f"<tr>{cells}</tr>")
    body = "".join(body_rows)
    return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"


def build_html_report(
    kpi_results: dict[str, pl.DataFrame],
    validation_results: dict[str, pl.DataFrame],
    dq_summary: pl.DataFrame,
    profile: pl.DataFrame,
    chart_paths: list[Path],
) -> str:
    chart_html = "".join(
        (
            f"<div><h3>{escape(path.stem)}</h3>"
            f"<img src='{escape(path.name)}' alt='{escape(path.stem)}' /></div>"
        )
        for path in chart_paths
    )

    kpi_html = "".join(
        f"<h3>{escape(name)}</h3>{dataframe_to_html_table(df)}" for name, df in kpi_results.items()
    )
    validation_html = "".join(
        f"<h3>{escape(name)}</h3>{dataframe_to_html_table(df)}"
        for name, df in validation_results.items()
    )

    return f"""
<!doctype html>
<html>
<head>
  <meta charset='utf-8' />
  <title>Lakehouse Analytics Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    table {{ border-collapse: collapse; margin-bottom: 24px; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 12px; text-align: left; }}
    th {{ background: #f7f7f7; }}
    img {{ max-width: 900px; width: 100%; border: 1px solid #ddd; margin-bottom: 20px; }}
  </style>
</head>
<body>
  <h1>Lakehouse Analytics Report</h1>
  <h2>Data Quality Summary</h2>
  {dataframe_to_html_table(dq_summary)}
  <h2>Detailed Profile</h2>
  {dataframe_to_html_table(profile)}
  <h2>Validation SQL Results</h2>
  {validation_html}
  <h2>KPI Charts</h2>
  {chart_html}
  <h2>KPI Tables</h2>
  {kpi_html}
</body>
</html>
""".strip()


def build_quality_html(summary: pl.DataFrame, profile: pl.DataFrame) -> str:
    return f"""
<!doctype html>
<html>
<head>
  <meta charset='utf-8' />
  <title>Data Quality Summary</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    table {{ border-collapse: collapse; margin-bottom: 24px; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 12px; text-align: left; }}
    th {{ background: #f7f7f7; }}
  </style>
</head>
<body>
  <h1>Data Quality Summary</h1>
  {dataframe_to_html_table(summary)}
  <h2>Detailed Profile</h2>
  {dataframe_to_html_table(profile)}
</body>
</html>
""".strip()
