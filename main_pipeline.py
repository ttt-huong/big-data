"""
MAIN ETL PIPELINE RUNNER
Điều hành toàn bộ luồng ETL/ELT: EXTRACT -> TRANSFORM -> LOAD -> REPORT.

Chạy thử:
1. Full Load với 100,000 bản ghi (tỷ lệ lỗi 5%):
   python main_pipeline.py --mode full --n 100000 --error-ratio 0.05

2. Incremental Load bổ sung thêm 20,000 bản ghi mới:
   python main_pipeline.py --mode incremental --n 20000 --error-ratio 0.02
"""

import argparse
import sys
import os

import config
from generator.data_generator import generate_clean_metadata, generate_dirty_metadata
from pipeline import extract_data, extract_data_chunks, transform_data, load_data, PipelineMetrics, logger


def run_pipeline(
    source_input,
    mode: str = "full",
    batch_size: int = None,
    start_date: str = None,
    end_date: str = None
) -> PipelineMetrics:
    """Thực thi toàn bộ luồng Pipeline ETL."""
    metrics = PipelineMetrics(mode=mode)
    if mode == "incremental":
        from pipeline.extract import get_watermark
        metrics.initial_watermark_ts = get_watermark().get("last_watermark_ts")

    logger.info(f"===> BẮT ĐẦU CHẠY PIPELINE ETL (MODE: {mode.upper()}) <===")

    try:
        if batch_size:
            logger.info(f"Áp dụng Chunk Processing với batch_size={batch_size:,}")
            for chunk_df in extract_data_chunks(
                source_input,
                chunk_size=batch_size,
                mode=mode,
                metrics=metrics,
                start_date=start_date,
                end_date=end_date,
            ):
                if chunk_df.empty:
                    continue
                clean_chunk = transform_data(chunk_df, metrics=metrics)
                success = load_data(clean_chunk, metrics=metrics)
                if not success:
                    metrics.finish(status="FAILED", error_message="Ghi dữ liệu chunk vào Target thất bại")
                    metrics.print_summary()
                    return metrics
            metrics.finish(status="SUCCESS")
        else:
            # 1. EXTRACT
            extracted_df = extract_data(source_input, mode=mode, metrics=metrics, start_date=start_date, end_date=end_date)
            if extracted_df.empty:
                logger.info("Không có dữ liệu trích xuất. Kết thúc pipeline.")
                metrics.finish(status="SUCCESS")
                metrics.print_summary()
                return metrics

            # 2. TRANSFORM & VALIDATE
            clean_df = transform_data(extracted_df, metrics=metrics)

            # 3. LOAD
            success = load_data(clean_df, metrics=metrics)
            if success:
                metrics.finish(status="SUCCESS")
            else:
                metrics.finish(status="FAILED", error_message="Ghi dữ liệu vào Target thất bại")

    except Exception as e:
        logger.error(f"Lỗi hệ thống khi chạy Pipeline: {e}", exc_info=True)
        metrics.finish(status="FAILED", error_message=str(e))

    metrics.print_summary()
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Pipeline Runner")
    parser.add_argument("--mode", type=str, choices=["full", "incremental", "backfill"], default="full", help="Chế độ load")
    parser.add_argument("--n", type=int, default=100_000, help="Số lượng bản ghi sinh để chạy demo")
    parser.add_argument("--error-ratio", type=float, default=0.05, help="Tỷ lệ bản ghi lỗi chèn vào (ví dụ 0.05 = 5%)")
    parser.add_argument("--batch-size", type=int, default=None, help="Kích thước batch nếu trích xuất từng đợt")
    parser.add_argument("--start-date", type=str, default=None, help="Ngày bắt đầu dùng cho chế độ Backfill (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="Ngày kết thúc dùng cho chế độ Backfill (YYYY-MM-DD)")
    args = parser.parse_args()

    # Tạo dữ liệu demo đầu vào
    if args.mode == "full":
        print(f"Đang sinh {args.n:,} bản ghi thô (tỷ lệ lỗi {args.error_ratio * 100:.1f}%)...")
        raw_data = generate_dirty_metadata(args.n, error_ratio=args.error_ratio, seed=42, start_id=1)
    elif args.mode == "backfill":
        print(f"Đang sinh {args.n:,} bản ghi thô cho chế độ Backfill ({args.start_date or 'Quá khứ'} -> {args.end_date or 'Hiện tại'})...")
        raw_data = generate_dirty_metadata(args.n, error_ratio=args.error_ratio, seed=123, start_id=5000)
    else:
        # Dành cho incremental: tạo dữ liệu với ID nối tiếp
        from pipeline.extract import get_watermark
        watermark = get_watermark()
        start_id = watermark.get("last_image_id", 0) + 1
        print(f"Đang sinh {args.n:,} bản ghi mới bổ sung (bắt đầu từ ID {start_id:,})...")
        raw_data = generate_dirty_metadata(args.n, error_ratio=args.error_ratio, seed=999, start_id=start_id)

    run_pipeline(raw_data, mode=args.mode, batch_size=args.batch_size, start_date=args.start_date, end_date=args.end_date)

