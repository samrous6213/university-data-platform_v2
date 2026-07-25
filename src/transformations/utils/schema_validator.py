from __future__ import annotations

from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType, StructType

from src.transformations.utils.logger import get_logger

logger = get_logger(__name__)


class SchemaValidationError(Exception):
    pass


def validate_schema(
    df: DataFrame,
    expected_schema: StructType,
    strict: bool = False,
) -> None:
    missing: List[str] = []
    type_mismatch: List[str] = []

    actual_fields: Dict[str, DataType] = {
        f.name: f.dataType for f in df.schema.fields
    }
    expected_fields: Dict[str, DataType] = {
        f.name: f.dataType for f in expected_schema.fields
    }

    for name, exp_type in expected_fields.items():
        if name not in actual_fields:
            missing.append(name)
            continue
        if strict and str(actual_fields[name]) != str(exp_type):
            type_mismatch.append(
                f"{name}: expected {exp_type}, got {actual_fields[name]}"
            )

    issues: List[str] = []
    if missing:
        issues.append(f"missing columns: {missing}")
    if type_mismatch:
        issues.append(f"type mismatches: {type_mismatch}")

    if issues:
        msg = " ; ".join(issues)
        logger.warning(
            f"Schema validation issues: {msg}",
            extra={"strict": str(strict)},
        )
        if strict:
            raise SchemaValidationError(msg)

    logger.info(
        "Schema validation passed",
        extra={
            "columns": len(actual_fields),
            "expected": len(expected_fields),
            "missing": len(missing),
            "type_mismatches": len(type_mismatch),
        },
    )


def has_required_columns(df: DataFrame, required: List[str]) -> bool:
    existing = set(df.columns)
    missing = [c for c in required if c not in existing]
    if missing:
        logger.warning(f"Missing required columns: {missing}")
        return False
    return True
