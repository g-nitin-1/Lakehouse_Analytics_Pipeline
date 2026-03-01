from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import streamlit as st

# Allow imports from local src/ without editable install in Streamlit Cloud.
SRC_PATH = Path(__file__).resolve().parent / "src"
if SRC_PATH.exists():
    sys.path.insert(0, str(SRC_PATH))

st.set_page_config(page_title="Lakehouse KPI Browser", layout="wide")
st.title("Lakehouse KPI Browser")

default_output = "output_big" if Path("output_big").exists() else "output"
base_output = Path(st.sidebar.text_input("Output directory", value=default_output))
kpi_dir = base_output / "kpis"
validation_dir = base_output / "validation"
report_dir = base_output / "reports"

if not base_output.exists():
    st.warning(f"Output directory not found: {base_output}")
    st.info("Generate demo outputs from sample data to use the dashboard.")
    if st.button("Generate Demo Output"):
        try:
            from lakehouse_pipeline.cli import run_pipeline
            from lakehouse_pipeline.config import PipelineConfig
        except Exception as exc:  # pragma: no cover - runtime guard for cloud env
            st.error(f"Unable to import pipeline modules: {exc}")
            st.stop()

        sample_input = Path("data/sample/orders.csv")
        if not sample_input.exists():
            st.error(f"Sample input not found: {sample_input}")
            st.stop()

        with st.spinner("Running sample pipeline..."):
            run_pipeline(
                PipelineConfig(input_path=sample_input, output_root=base_output),
                engine="polars",
                incremental=False,
                full_refresh=True,
            )
        st.success(f"Demo output generated at: {base_output}")
        st.rerun()
    st.stop()

st.sidebar.markdown("Run pipeline first:")
st.sidebar.code(
    "python -m lakehouse_pipeline.cli run "
    "--input data/sample/orders.csv --output output --engine spark"
)


def _load_csv(path: Path) -> pl.DataFrame:
    return pl.read_csv(path)


def _render_table(df: pl.DataFrame, label: str) -> None:
    st.subheader(label)
    st.dataframe(df, use_container_width=True)


kpi_files = sorted(kpi_dir.glob("*.csv")) if kpi_dir.exists() else []
validation_files = sorted(validation_dir.glob("*.csv")) if validation_dir.exists() else []

col1, col2 = st.columns(2)

with col1:
    st.header("KPIs")
    if not kpi_files:
        st.warning("No KPI CSV files found. Run the pipeline first.")
    else:
        kpi_names = [f.stem for f in kpi_files]
        selected_kpi = st.selectbox("Select KPI", kpi_names)
        kpi_path = kpi_dir / f"{selected_kpi}.csv"
        kpi_df = _load_csv(kpi_path)
        _render_table(kpi_df, selected_kpi)

        if kpi_df.height > 0 and kpi_df.width >= 2:
            x_col = kpi_df.columns[0]
            y_candidates = [
                c
                for c in kpi_df.columns[1:]
                if kpi_df[c].dtype in (pl.Int64, pl.Int32, pl.Float64, pl.Float32)
            ]
            if y_candidates:
                y_col = y_candidates[0]
                chart_df = kpi_df.select([x_col, y_col]).to_pandas()
                chart_df = chart_df.set_index(x_col)
                st.line_chart(chart_df)

with col2:
    st.header("Validation")
    if not validation_files:
        st.warning("No validation CSV files found. Run the pipeline first.")
    else:
        validation_names = [f.stem for f in validation_files]
        selected_validation = st.selectbox("Select Validation", validation_names)
        validation_path = validation_dir / f"{selected_validation}.csv"
        validation_df = _load_csv(validation_path)
        _render_table(validation_df, selected_validation)

st.header("Reports")
if report_dir.exists():
    report_html = report_dir / "report.html"
    dq_html = report_dir / "data_quality_summary.html"

    files_col1, files_col2 = st.columns(2)
    with files_col1:
        st.write("Available chart files:")
        chart_files = sorted(report_dir.glob("*.png"))
        if chart_files:
            for file in chart_files:
                st.image(str(file), caption=file.name)
        else:
            st.info("No chart images found.")

    with files_col2:
        st.write("HTML report files:")
        if report_html.exists():
            st.code(str(report_html))
        if dq_html.exists():
            st.code(str(dq_html))
        if not report_html.exists() and not dq_html.exists():
            st.info("No HTML reports found.")
else:
    st.warning(f"Report directory not found: {report_dir}")
