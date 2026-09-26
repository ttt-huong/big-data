"""Local Delta Lake loader with idempotent upsert semantics."""

import os

import pandas as pd
from deltalake import DeltaTable, write_deltalake

import config


def load_data(clean: pd.DataFrame, target_path: str | None = None) -> int:
    if clean.empty:
        return 0

    target_path = target_path or config.TARGET_DELTA_PATH
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    if not DeltaTable.is_deltatable(target_path):
        write_deltalake(target_path, clean, mode="overwrite", partition_by=["category"])
    else:
        table = DeltaTable(target_path)
        (
            table.merge(
                source=clean,
                predicate="target.image_id = source.image_id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    return len(clean)
