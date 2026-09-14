"""
Logging & Run Summary Utilities cho ETL Pipeline.
Cung cấp logger ghi ra console/file và class PipelineMetrics để thống kê từng đợt thực thi.
"""

import os
import time
import logging
import json
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional
import config

# Cấu hình logger
log_file_path = os.path.join(config.LOCAL_LOGS_DIR, "etl_pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ETL_Pipeline")


@dataclass
class PipelineMetrics:
    mode: str = "full"                      # 'full' hoặc 'incremental'
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    total_time_s: float = 0.0
    
    extracted_records: int = 0
    valid_records: int = 0
    error_records: int = 0
    loaded_records: int = 0
    
    extract_time_s: float = 0.0
    transform_time_s: float = 0.0
    load_time_s: float = 0.0
    
    retries: int = 0
    status: str = "PENDING"                # 'SUCCESS', 'FAILED', 'PENDING'
    error_message: str = ""

    def finish(self, status: str = "SUCCESS", error_message: str = ""):
        self.end_time = time.time()
        self.total_time_s = round(self.end_time - self.start_time, 3)
        self.status = status
        self.error_message = error_message

    def print_summary(self):
        banner = "=" * 55
        print(f"\n{banner}")
        print(f"        ETL PIPELINE RUN SUMMARY ({self.mode.upper()} LOAD)")
        print(banner)
        print(f"Status           : {self.status}")
        print(f"Total Time       : {self.total_time_s:.3f} s")
        print(f"  - Extract Time : {self.extract_time_s:.3f} s")
        print(f"  - Transform    : {self.transform_time_s:.3f} s")
        print(f"  - Load Time    : {self.load_time_s:.3f} s")
        print("-" * 55)
        print(f"Records Extracted: {self.extracted_records:,}")
        print(f"Records Valid    : {self.valid_records:,}")
        print(f"Records Error    : {self.error_records:,}")
        print(f"Records Loaded   : {self.loaded_records:,}")
        print(f"Retries          : {self.retries}")
        if self.error_message:
            print(f"Error Details    : {self.error_message}")
        print(f"{banner}\n")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
