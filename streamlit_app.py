from __future__ import annotations

import sys
import time
from pathlib import Path

import polars as pl
import streamlit as st

# Allow imports from local src/ without editable install in Streamlit Cloud.
SRC_PATH = Path(__file__).resolve().parent / "src"
if SRC_PATH.exists():
    sys.path.insert(0, str(SRC_PATH))

st.set_page_config(page_title="Lakehouse KPI Browser", layout="wide")
st.title("Lakehouse KPI Browser")

if Path("output_web").exists():
    default_output = "output_web"
elif Path("output_big").exists():
    default_output = "output_big"
else:
    default_output = "output"
base_output = Path(st.sidebar.text_input("Output directory", value=default_output))
default_input = (
    "data/raw/yellow_tripdata_2024-01.parquet"
    if Path("data/raw/yellow_tripdata_2024-01.parquet").exists()
    else "data/sample/orders.csv"
)
input_path = Path(st.sidebar.text_input("Input dataset", value=default_input))
run_engine = st.sidebar.selectbox("Engine", ["polars", "spark"], index=0)
yellow_target = Path("data/raw/yellow_tripdata_2024-01.parquet")
kpi_dir = base_output / "kpis"
validation_dir = base_output / "validation"
report_dir = base_output / "reports"
MAX_PREVIEW_ROWS = 2000


def _estimate_runtime_seconds(dataset: Path, engine: str) -> tuple[int, int]:
    name = dataset.name.lower()
    if "yellow_tripdata" in name:
        return (45, 120) if engine == "polars" else (90, 240)
    return (5, 20) if engine == "polars" else (10, 40)


def _is_heavy_dataset(dataset: Path) -> bool:
    return "yellow_tripdata" in dataset.name.lower()


if not base_output.exists():
    st.warning(f"Output directory not found: {base_output}")
    low, high = _estimate_runtime_seconds(input_path, run_engine)
    st.info(
        f"Estimated runtime for current input/engine: ~{low}-{high} seconds "
        f"(dataset: `{input_path.name}`, engine: `{run_engine}`)."
    )

    if yellow_target.exists():
        st.success(f"Dataset ready: {yellow_target}")
    elif st.button("Download yellow_tripdata_2024-01.parquet"):
        try:
            import urllib.request

            yellow_target.parent.mkdir(parents=True, exist_ok=True)
            url = (
                "https://d37ci6vzurychx.cloudfront.net/trip-data/"
                "yellow_tripdata_2024-01.parquet"
            )
            with st.spinner("Downloading yellow taxi dataset..."):
                urllib.request.urlretrieve(url, yellow_target)
            st.success(f"Downloaded: {yellow_target}")
            st.rerun()
        except Exception as exc:  # pragma: no cover
            st.error(f"Download failed: {exc}")

    if st.button("Generate Output"):
        try:
            from lakehouse_pipeline.cli import run_pipeline
            from lakehouse_pipeline.config import PipelineConfig
        except Exception as exc:  # pragma: no cover - runtime guard for cloud env
            st.error(f"Unable to import pipeline modules: {exc}")
            st.stop()

        if not input_path.exists():
            st.error(f"Input not found: {input_path}")
            st.stop()

        if _is_heavy_dataset(input_path):
            st.error(
                "Generating outputs from yellow_tripdata inside Streamlit Cloud often exceeds "
                "memory limits. Run big-data processing locally, then use lightweight "
                "outputs in Cloud."
            )
            st.info(
                "Cloud-safe option: set input to `data/sample/orders.csv` and click "
                "Generate Output."
            )
            st.stop()

        start = time.perf_counter()
        with st.spinner(f"Running pipeline on {input_path.name}..."):
            run_pipeline(
                PipelineConfig(input_path=input_path, output_root=base_output),
                engine=run_engine,
                incremental=False,
                full_refresh=True,
            )
        elapsed = time.perf_counter() - start
        st.success(f"Output generated at: {base_output} in {elapsed:.1f}s")
        st.rerun()
    st.stop()

st.sidebar.markdown("Run pipeline first:")
st.sidebar.code(
    "python -m lakehouse_pipeline.cli run "
    "--input data/raw/yellow_tripdata_2024-01.parquet --output output_big --engine spark"
)
st.sidebar.markdown("For Streamlit Cloud, prefer curated artifacts in `output_web`.")


def _load_csv(path: Path) -> pl.DataFrame:
    return pl.read_csv(path, n_rows=MAX_PREVIEW_ROWS)


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
        st.caption(f"Preview mode: showing up to {MAX_PREVIEW_ROWS} rows per file.")
        kpi_names = [f.stem for f in kpi_files]
        selected_kpi = st.selectbox("Select KPI", kpi_names)
        kpi_path = kpi_dir / f"{selected_kpi}.csv"
        file_size_mb = kpi_path.stat().st_size / (1024 * 1024)
        if file_size_mb > 20:
            st.info(f"Large file detected ({file_size_mb:.1f} MB). Loading preview only.")
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
        st.caption(f"Preview mode: showing up to {MAX_PREVIEW_ROWS} rows per file.")
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
