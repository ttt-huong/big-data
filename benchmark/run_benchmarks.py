"""Small, real local benchmarks for CSV/Parquet, chunks and DuckDB."""

import os
import time

import duckdb
import pandas as pd

import config
from generator.data_generator import generate_metadata
from pipeline.extract import read_chunks


def benchmark_storage(rows: int) -> dict:
    frame = generate_metadata(rows, seed=7)
    csv_path = os.path.join(config.PROCESSED_DATA_DIR, f"benchmark_{rows}.csv")
    parquet_path = os.path.join(config.PROCESSED_DATA_DIR, f"benchmark_{rows}.parquet")
    start = time.perf_counter(); frame.to_csv(csv_path, index=False); csv_write = time.perf_counter() - start
    start = time.perf_counter(); frame.to_parquet(parquet_path, index=False); parquet_write = time.perf_counter() - start
    start = time.perf_counter(); pd.read_csv(csv_path); csv_read = time.perf_counter() - start
    start = time.perf_counter(); pd.read_parquet(parquet_path); parquet_read = time.perf_counter() - start
    return {"rows": rows, "csv_mb": round(os.path.getsize(csv_path) / 1024**2, 3), "parquet_mb": round(os.path.getsize(parquet_path) / 1024**2, 3), "csv_write_s": round(csv_write, 4), "parquet_write_s": round(parquet_write, 4), "csv_read_s": round(csv_read, 4), "parquet_read_s": round(parquet_read, 4)}


def benchmark_chunks(rows: int) -> dict:
    path = os.path.join(config.PROCESSED_DATA_DIR, f"chunk_benchmark_{rows}.parquet")
    generate_metadata(rows, seed=8).to_parquet(path, index=False)
    start = time.perf_counter(); pd.read_parquet(path); full_s = time.perf_counter() - start
    start = time.perf_counter(); chunk_rows = sum(len(chunk) for chunk in read_chunks(path, chunk_size=10_000)); chunk_s = time.perf_counter() - start
    return {"rows": rows, "chunk_size": 10_000, "full_read_s": round(full_s, 4), "chunk_read_s": round(chunk_s, 4), "chunk_rows": chunk_rows}


def benchmark_query(rows: int) -> dict:
    path = os.path.join(config.PROCESSED_DATA_DIR, f"query_benchmark_{rows}.parquet")
    generate_metadata(rows, seed=9).to_parquet(path, index=False)
    connection = duckdb.connect()
    start = time.perf_counter()
    count = connection.sql(f"SELECT category, COUNT(*) FROM read_parquet('{path.replace(chr(92), '/') }') GROUP BY category").fetchall()
    elapsed = time.perf_counter() - start
    return {"rows": rows, "query_s": round(elapsed, 4), "groups": len(count)}


if __name__ == "__main__":
    storage = pd.DataFrame([benchmark_storage(rows) for rows in config.BENCHMARK_ROWS])
    chunks = pd.DataFrame([benchmark_chunks(rows) for rows in config.BENCHMARK_ROWS])
    queries = pd.DataFrame([benchmark_query(rows) for rows in config.BENCHMARK_ROWS])
    storage.to_csv(os.path.join(config.LOCAL_RESULTS_DIR, "storage_benchmark.csv"), index=False)
    chunks.to_csv(os.path.join(config.LOCAL_RESULTS_DIR, "chunk_benchmark.csv"), index=False)
    queries.to_csv(os.path.join(config.LOCAL_RESULTS_DIR, "query_benchmark.csv"), index=False)
    print("Storage benchmark:\n", storage)
    print("Chunk benchmark:\n", chunks)
    print("Query benchmark:\n", queries)
