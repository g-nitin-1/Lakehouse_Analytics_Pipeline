import polars as pl
import pytest

from lakehouse_pipeline.quality.expectations import validate_required_columns
from lakehouse_pipeline.quality.profiling import profile_dataframe


def test_validate_required_columns_passes() -> None:
    df = pl.DataFrame({"a": [1], "b": [2]})
    validate_required_columns(df, ["a", "b"])


def test_validate_required_columns_fails() -> None:
    df = pl.DataFrame({"a": [1]})
    with pytest.raises(ValueError):
        validate_required_columns(df, ["a", "b"])


def test_profile_dataframe_contains_expected_columns() -> None:
    df = pl.DataFrame(
        {
            "category": ["a", "a", "b", None],
            "amount": [1.0, 2.0, 100.0, 3.0],
        }
    )
    profile, summary = profile_dataframe(df)
    assert "top_category" in profile.columns
    assert "outlier_count" in summary.columns
