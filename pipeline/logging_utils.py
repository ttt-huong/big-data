"""Run metrics and logging for the local pipeline."""

import logging
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(config.LOCAL_LOGS_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("local_etl")


@dataclass
class PipelineMetrics:
    mode: str = "full"
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    extracted_records: int = 0
    valid_records: int = 0
    error_records: int = 0
    loaded_records: int = 0
    status: str = "PENDING"
    error_message: str = ""

    def finish(self, status: str, error_message: str = "") -> None:
        self.finished_at = time.time()
        self.status = status
        self.error_message = error_message

    @property
    def total_time_s(self) -> float:
        return round((self.finished_at or time.time()) - self.started_at, 3)

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["total_time_s"] = self.total_time_s
        return result
