"""
Unit tests cho pipeline.transform module.
"""

import pytest
import pandas as pd
from pipeline.transform import transform_data
from pipeline.logging_utils import PipelineMetrics


def test_transform_data_enrichment(sample_raw_data):
    """Kiểm tra transform_data thực hiện enrich aspect_ratio, size_mb, ingested_at."""
    metrics = PipelineMetrics(mode="full")
    clean_df = transform_data(sample_raw_data, metrics=metrics)

    # 2 bản ghi sạch thu được
    assert len(clean_df) == 2
    assert metrics.valid_records == 2
    assert metrics.error_records == 5

    # Kiểm tra các cột enriched
    assert "aspect_ratio" in clean_df.columns
    assert "size_mb" in clean_df.columns
    assert "ingested_at" in clean_df.columns

    # Kiểm tra tính toán aspect_ratio = width / height rounded 2 decimals
    # ID 1: 1920 / 1080 = 1.78
    # ID 2: 1080 / 1080 = 1.00
    ratios = clean_df["aspect_ratio"].tolist()
    assert ratios == [1.78, 1.00]

    # Kiểm tra size_mb = file_size / (1024**2)
    # ID 1: 102400 / 1048576 = 0.098
    sizes = clean_df["size_mb"].tolist()
    assert sizes[0] == round(102400 / (1024 ** 2), 3)


def test_transform_late_data_detection(tmp_path):
    """Kiểm tra phát hiện dữ liệu muộn (Late-Arriving Data)."""
    import config
    from pipeline.extract import save_watermark

    test_wm_file = tmp_path / "watermark.json"
    original_wm = config.WATERMARK_FILE
    config.WATERMARK_FILE = str(test_wm_file)

    try:
        # Watermark hiện tại là 2026-02-01
        save_watermark(last_watermark_ts="2026-02-01 00:00:00", last_image_id=100)

        metrics = PipelineMetrics(mode="incremental")
        raw_df = pd.DataFrame([
            {
                "image_id": 101,
                "file_path": "a.jpg",
                "format": "jpg",
                "category": "san_pham",
                "width": 100,
                "height": 100,
                "file_size": 1000,
                "created_at": "2026-01-15 00:00:00"  # Cũ hơn watermark -> Late Data
            }
        ])

        clean_df = transform_data(raw_df, metrics=metrics)
        assert len(clean_df) == 1
        assert metrics.late_records == 1
        assert "is_late_arriving" in clean_df.columns
        assert clean_df["is_late_arriving"].tolist() == [True]
    finally:
        config.WATERMARK_FILE = original_wm

