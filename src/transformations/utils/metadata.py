from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


def generate_record_id(
    source_system: str, source_url: str, content: Dict[str, Any]
) -> str:
    stable = {k: v for k, v in content.items() if k not in ("record_id",)}
    raw = json.dumps(stable, sort_keys=True, default=str)
    digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"{source_system}_{digest}"


def generate_content_hash(content: Dict[str, Any]) -> str:
    raw = json.dumps(content, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def add_processing_timestamp(df: DataFrame) -> DataFrame:
    now_ts = datetime.now(timezone.utc).isoformat()
    return df.withColumn(
        "processing_timestamp", F.lit(now_ts).cast(TimestampType())
    )


def add_record_id_if_missing(df: DataFrame) -> DataFrame:
    has_record_id = "record_id" in df.columns
    if not has_record_id:
        concat_cols = F.concat_ws(
            "_",
            F.col("source_system"),
            F.sha2(F.to_json(F.struct(F.col("*"))), 256).substr(1, 16),
        )
        return df.withColumn("record_id", concat_cols)
    return df
