"""Run the local ETL pipeline from a generated Parquet source."""

import argparse
import os
import shutil
import time

import config
from generator.data_generator import generate_metadata
from pipeline import PipelineMetrics, get_watermark, load_data, logger, read_chunks, save_watermark, transform_data


def run_pipeline(
    source_path: str,
    mode: str = "full",
    chunk_size: int = 25_000,
    backfill_before_id: int | None = None,
    max_attempts: int = config.DEFAULT_MAX_ATTEMPTS,
    retry_delay: float = config.DEFAULT_RETRY_DELAY,
) -> PipelineMetrics:
    metrics = PipelineMetrics(mode=mode)
    state = get_watermark()
    watermark_before = 0 if mode == "full" else int(state.get("last_image_id", 0))
    processed_sources_before = set(state.get("processed_sources", []))
    max_id = watermark_before
    seen_ids: set[int] = set()

    if os.path.isdir(source_path):
        all_files = sorted([
            os.path.join(source_path, f)
            for f in os.listdir(source_path)
            if f.endswith((".parquet", ".csv"))
        ])
    else:
        all_files = [source_path]

    if mode == "incremental":
        files_to_process = [
            f for f in all_files
            if os.path.basename(f) not in processed_sources_before
        ]
        if not files_to_process:
            logger.info("No new source batches to process in incremental mode.")
            metrics.finish("SUCCESS")
            return metrics
        processed_sources_current = set(processed_sources_before)
    elif mode == "full":
        files_to_process = all_files
        processed_sources_current = set()
    else:  # backfill
        files_to_process = all_files
        processed_sources_current = set(processed_sources_before)

    def record_retry(attempt: int, exc: Exception) -> None:
        metrics.retry_count += 1

    try:
        for file_path in files_to_process:
            source_stem = os.path.splitext(os.path.basename(file_path))[0]
            for chunk_idx, chunk in enumerate(
                read_chunks(file_path, chunk_size, mode, watermark_before, backfill_before_id)
            ):
                metrics.extracted_records += len(chunk)
                batch_id = f"{source_stem}_c{chunk_idx}"
                clean, errors = transform_data(chunk, seen_ids=seen_ids, batch_id=batch_id)
                metrics.valid_records += len(clean)
                metrics.error_records += len(errors)
                metrics.loaded_records += load_data(
                    clean,
                    max_attempts=max_attempts,
                    retry_delay=retry_delay,
                    on_retry=record_retry,
                )
                if not clean.empty:
                    seen_ids.update(clean["image_id"].tolist())
                    max_id = max(max_id, int(clean["image_id"].max()))
            if mode != "backfill":
                processed_sources_current.add(os.path.basename(file_path))

        if mode != "backfill":
            new_watermark = max(watermark_before, max_id)
            save_watermark(new_watermark, list(processed_sources_current))
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
    parser.add_argument("--max-attempts", type=int, default=config.DEFAULT_MAX_ATTEMPTS)
    parser.add_argument("--retry-delay", type=float, default=config.DEFAULT_RETRY_DELAY)
    args = parser.parse_args()

    if args.mode == "full":
        if os.path.exists(config.TARGET_DELTA_PATH):
            shutil.rmtree(config.TARGET_DELTA_PATH)
        if os.path.exists(config.ERROR_DATA_DIR):
            shutil.rmtree(config.ERROR_DATA_DIR)
            os.makedirs(config.ERROR_DATA_DIR, exist_ok=True)
    source = create_source(args.rows, args.error_ratio, args.mode)
    started = time.perf_counter()
    result = run_pipeline(
        source,
        args.mode,
        args.chunk_size,
        args.backfill_before_id,
        max_attempts=args.max_attempts,
        retry_delay=args.retry_delay,
    )
    print({**result.to_dict(), "wall_time_s": round(time.perf_counter() - started, 3), "source": source})
