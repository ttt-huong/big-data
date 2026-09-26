"""Run the local ETL pipeline from a generated Parquet source."""

import argparse
import os
import shutil
import time

import config
from generator.data_generator import generate_metadata
from pipeline import PipelineMetrics, get_watermark, load_data, read_chunks, save_watermark, transform_data


def run_pipeline(source_path: str, mode: str = "full", chunk_size: int = 25_000, backfill_before_id: int | None = None) -> PipelineMetrics:
    metrics = PipelineMetrics(mode=mode)
    watermark_before = int(get_watermark().get("last_image_id", 0))
    max_id = watermark_before
    seen_ids: set[int] = set()
    try:
        for chunk in read_chunks(source_path, chunk_size, mode, watermark_before, backfill_before_id):
            metrics.extracted_records += len(chunk)
            clean, errors = transform_data(chunk, seen_ids=seen_ids)
            metrics.valid_records += len(clean)
            metrics.error_records += len(errors)
            metrics.loaded_records += load_data(clean)
            if not clean.empty:
                seen_ids.update(clean["image_id"].tolist())
                max_id = max(max_id, int(clean["image_id"].max()))
        if mode != "backfill" and max_id > watermark_before:
            save_watermark(max_id)
        metrics.finish("SUCCESS")
    except Exception as exc:
        metrics.finish("FAILED", str(exc))
        raise
    return metrics


def create_source(rows: int, error_ratio: float, mode: str) -> str:
    watermark = int(get_watermark().get("last_image_id", 0))
    start_id = watermark + 1 if mode == "incremental" else 1
    frame = generate_metadata(rows, error_ratio=error_ratio, seed=42 if mode == "full" else 99, start_id=start_id)
    source_name = f"{mode}_{start_id}_{start_id + rows - 1}.parquet"
    source_path = os.path.join(config.RAW_DATA_DIR, source_name)
    frame.to_parquet(source_path, index=False)
    return source_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local ETL/Lakehouse demo")
    parser.add_argument("--mode", choices=["full", "incremental", "backfill"], default="full")
    parser.add_argument("--rows", "--n", dest="rows", type=int, default=config.DEFAULT_ROWS)
    parser.add_argument("--error-ratio", type=float, default=config.DEFAULT_ERROR_RATIO)
    parser.add_argument("--chunk-size", "--batch-size", dest="chunk_size", type=int, default=25_000)
    parser.add_argument("--backfill-before-id", type=int, default=None)
    args = parser.parse_args()

    if args.mode == "full" and os.path.exists(config.TARGET_DELTA_PATH):
        shutil.rmtree(config.TARGET_DELTA_PATH)
    source = create_source(args.rows, args.error_ratio, args.mode)
    started = time.perf_counter()
    result = run_pipeline(source, args.mode, args.chunk_size, args.backfill_before_id)
    print({**result.to_dict(), "wall_time_s": round(time.perf_counter() - started, 3), "source": source})
