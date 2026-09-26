"""Local Delta Lake loader with idempotent upsert semantics and transient error retry."""

from collections.abc import Callable
import os
import time

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, DeltaProtocolError

import config
from pipeline.logging_utils import logger

RETRYABLE_EXCEPTIONS = (
    IOError,
    OSError,
    CommitFailedError,
    DeltaProtocolError,
)


def _write_to_delta(clean: pd.DataFrame, target_path: str) -> None:
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
                merge_schema=True,
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )


def load_data(
    clean: pd.DataFrame,
    target_path: str | None = None,
    max_attempts: int = config.DEFAULT_MAX_ATTEMPTS,
    retry_delay: float = config.DEFAULT_RETRY_DELAY,
    backoff_factor: float = config.DEFAULT_BACKOFF_FACTOR,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> int:
    if clean.empty:
        return 0

    target_path = target_path or config.TARGET_DELTA_PATH
    attempts_limit = max(1, max_attempts)
    delay = max(0.0, retry_delay)

    for attempt in range(1, attempts_limit + 1):
        try:
            _write_to_delta(clean, target_path)
            return len(clean)
        except RETRYABLE_EXCEPTIONS as exc:
            if attempt >= attempts_limit:
                logger.error(
                    f"Load failed on final attempt {attempt}/{attempts_limit}: {exc}"
                )
                raise
            logger.warning(
                f"Load attempt {attempt}/{attempts_limit} failed with {type(exc).__name__}: {exc}. "
                f"Retrying in {delay:.2f}s..."
            )
            if on_retry is not None:
                on_retry(attempt, exc)
            if delay > 0:
                time.sleep(delay)
            delay *= backoff_factor
        except Exception:
            # Non-retryable exceptions (e.g. ValueError, SchemaMismatchError, TypeError) fail immediately
            raise
