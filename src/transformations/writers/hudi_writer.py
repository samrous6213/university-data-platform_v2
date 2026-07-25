from __future__ import annotations

import time
from typing import Dict, Optional

from pyspark.sql import DataFrame

from src.transformations.config.hudi_config import HudiTableConfig
from src.transformations.utils.logger import get_logger

logger = get_logger(__name__)


class HudiWriteError(Exception):
    pass


def _write_with_retry(
    df: DataFrame,
    base_path: str,
    options: Dict[str, str],
    max_retries: int = 3,
    retry_delay: int = 5,
) -> None:
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            (
                df.write.format("hudi")
                .mode("append")
                .options(**options)
                .save(base_path)
            )
            logger.info(
                f"Hudi write succeeded",
                extra={
                    "table": options["hoodie.table.name"],
                    "path": base_path,
                    "attempt": attempt,
                },
            )
            return
        except Exception as e:
            last_error = e
            logger.warning(
                f"Hudi write attempt {attempt}/{max_retries} failed",
                extra={"table": options["hoodie.table.name"], "error": str(e)},
            )
            if attempt < max_retries:
                time.sleep(retry_delay)

    raise HudiWriteError(
        f"Failed to write Hudi table {options['hoodie.table.name']} "
        f"after {max_retries} attempts: {last_error}"
    )


def write_hudi_table(
    df: DataFrame,
    config: HudiTableConfig,
    max_retries: int = 3,
    retry_delay: int = 5,
) -> int:
    record_count = df.count()
    if record_count == 0:
        logger.warning(f"Skipping write for {config.table_name}: empty DataFrame")
        return 0

    df = df.dropDuplicates([config.record_key])
    write_count = df.count()

    options = config.write_options()
    base_path = config.base_path

    logger.info(
        f"Writing {config.table_name} to Hudi",
        extra={
            "records": record_count,
            "unique_records": write_count,
            "path": base_path,
            "record_key": config.record_key,
            "partition_field": config.partition_field,
            "table_type": config.table_type,
        },
    )

    _write_with_retry(df, base_path, options, max_retries, retry_delay)
    return write_count
