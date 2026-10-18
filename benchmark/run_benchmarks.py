"""Small, real local benchmarks for CSV/Parquet, chunks and DuckDB."""

import statistics
import tempfile

from deltalake import DeltaTable, write_deltalake

import os
import time

import duckdb
import pandas as pd

import config
from generator.data_generator import generate_metadata
from pipeline.extract import read_chunks

from benchmark.analytics_baseline import run_queries as run_baseline
from query_analytics import run_queries as run_optimized

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

def benchmark_delta_queries(rows: int) -> list[dict]:
    queries = [
        ("by_category", "SELECT category, COUNT(*), SUM(file_size), AVG(file_size) FROM clean_data GROUP BY category ORDER BY category", []),
        ("by_month", "SELECT year, month, COUNT(*), SUM(file_size) FROM clean_data GROUP BY year, month ORDER BY year, month", []),
        ("filtered_formats", "SELECT format, COUNT(*), SUM(file_size) FROM clean_data WHERE category = ? AND created_at >= CAST(? AS TIMESTAMP) AND created_at < CAST(? AS TIMESTAMP) GROUP BY format ORDER BY format", ["do_an", "2025-01-01", "2026-01-01"]),
    ]
    results = []

    with tempfile.TemporaryDirectory(prefix="olap_benchmark_") as directory:
        target = os.path.join(directory, "table")
        frame = generate_metadata(rows, seed=9, error_ratio=0)
        write_deltalake(target, frame, mode="overwrite", partition_by=["category"])

        with duckdb.connect() as connection:
            connection.register("clean_data", DeltaTable(target).to_pyarrow_dataset())

            for name, sql, params in queries:
                durations = []
                first_result = None

                for run in range(6):
                    start = time.perf_counter()
                    output = connection.execute(sql, params).fetchall()
                    durations.append(time.perf_counter() - start)

                    if run == 0:
                        first_result = output
                    elif output != first_result:
                        raise AssertionError(f"Inconsistent results: {name}")

                if rows == 100_000 and name == "filtered_formats":
                    plan = connection.execute("EXPLAIN ANALYZE " + sql, params).fetchall()
                    plan_path = os.path.join(config.LOCAL_RESULTS_DIR, "duckdb_filtered_explain.txt")
                    with open(plan_path, "w", encoding="utf-8") as file:
                        file.write("\n".join(str(row[1]) for row in plan))

                results.append({
                    "rows": rows,
                    "query": name,
                    "first_run_s": durations[0],
                    "repeat_median_s": statistics.median(durations[1:]),
                    "result_rows": len(first_result),
                    "repeats": 5,
                })

    return results

def benchmark_partitioning(rows: int) -> list[dict]:
    sql = "SELECT format, COUNT(*), SUM(file_size) FROM clean_data WHERE category = ? AND created_at >= CAST(? AS TIMESTAMP) AND created_at < CAST(? AS TIMESTAMP) GROUP BY format ORDER BY format"
    params = ["do_an", "2025-01-01", "2026-01-01"]
    frame = generate_metadata(rows, seed=9, error_ratio=0)
    timings = {"unpartitioned": [], "partitioned": []}
    datasets = {}

    with tempfile.TemporaryDirectory(prefix="partition_benchmark_") as directory:
        for layout in timings:
            target = os.path.join(directory, layout)
            partitions = ["category"] if layout == "partitioned" else []
            write_deltalake(target, frame, mode="overwrite", partition_by=partitions)
            datasets[layout] = DeltaTable(target).to_pyarrow_dataset()

        with duckdb.connect() as plain, duckdb.connect() as partitioned:
            connections = {"unpartitioned": plain, "partitioned": partitioned}

            for layout, connection in connections.items():
                connection.execute("SET threads = 4")
                connection.register("clean_data", datasets[layout])

            reference = None
            for run in range(6):
                order = list(connections)
                if run % 2:
                    order.reverse()

                for layout in order:
                    start = time.perf_counter()
                    output = connections[layout].execute(sql, params).fetchall()
                    elapsed = time.perf_counter() - start

                    if reference is None:
                        reference = output
                    elif output != reference:
                        raise AssertionError("Partition layouts returned different results")

                    timings[layout].append(elapsed)

        results = [{
            "rows": rows,
            "layout": layout,
            "data_files": len(datasets[layout].files),
            "threads": 4,
            "first_run_s": durations[0],
            "repeat_median_s": statistics.median(durations[1:]),
            "result_rows": len(reference),
            "repeats": 5,
        } for layout, durations in timings.items()]

    return results

def benchmark_analytics_optimization(rows: int) -> list[dict]:
    functions = {"baseline": run_baseline, "optimized": run_optimized}
    timings = {name: [] for name in functions}

    with tempfile.TemporaryDirectory(prefix="analytics_comparison_") as directory:
        target = os.path.join(directory, "table")
        errors = os.path.join(directory, "errors")
        os.makedirs(errors)

        frame = generate_metadata(rows, seed=9, error_ratio=0)
        write_deltalake(target, frame, mode="overwrite", partition_by=["category"])

        pd.DataFrame({
            "image_id": [rows + 1, rows + 2],
            "error_reason": ["INVALID_FORMAT", "INVALID_FORMAT;INVALID_FILE_SIZE"],
        }).to_parquet(os.path.join(errors, "errors_sample.parquet"), index=False)

        reference = None
        for run in range(6):
            order = list(functions)
            if run % 2:
                order.reverse()

            for name in order:
                start = time.perf_counter()
                output = functions[name](target, errors)
                elapsed = time.perf_counter() - start

                if reference is None:
                    reference = output
                elif output != reference:
                    raise AssertionError(f"Analytics results differ: {name}")

                timings[name].append(elapsed)

    return [{
        "rows": rows,
        "version": name,
        "first_run_s": durations[0],
        "repeat_median_s": statistics.median(durations[1:]),
        "repeats": 5,
        "results_equal": True,
    } for name, durations in timings.items()]

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
    delta_queries = pd.DataFrame([result for rows in config.BENCHMARK_ROWS for result in benchmark_delta_queries(rows)])
    delta_queries.to_csv(os.path.join(config.LOCAL_RESULTS_DIR, "delta_query_benchmark.csv"), index=False)
    print("Delta query benchmark:\n", delta_queries)