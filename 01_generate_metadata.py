"""Generate a local Parquet source file for the ETL demo."""

import argparse
import os

from generator.data_generator import generate_metadata
import config


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", "--n", dest="rows", type=int, default=config.DEFAULT_ROWS)
    parser.add_argument("--error-ratio", type=float, default=config.DEFAULT_ERROR_RATIO)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    output = args.out or os.path.join(config.RAW_DATA_DIR, f"metadata_{args.rows}.parquet")
    generate_metadata(args.rows, error_ratio=args.error_ratio).to_parquet(output, index=False)
    print(f"Generated {args.rows:,} records at {output}")
