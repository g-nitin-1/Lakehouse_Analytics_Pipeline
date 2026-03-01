from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    input_path: Path
    output_root: Path

    @property
    def bronze_dir(self) -> Path:
        return self.output_root / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.output_root / "silver"

    @property
    def quarantine_dir(self) -> Path:
        return self.output_root / "quarantine"

    @property
    def gold_dir(self) -> Path:
        return self.output_root / "gold"

    @property
    def kpi_dir(self) -> Path:
        return self.output_root / "kpis"

    @property
    def validation_dir(self) -> Path:
        return self.output_root / "validation"

    @property
    def report_dir(self) -> Path:
        return self.output_root / "reports"
