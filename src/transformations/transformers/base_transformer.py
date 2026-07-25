from __future__ import annotations

from typing import Dict, List, Optional

from pyspark.sql import DataFrame, Window, functions as F


def drop_nulls(df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
    if subset:
        existing = [c for c in subset if c in df.columns]
        if not existing:
            return df
        return df.dropna(subset=existing)
    return df.dropna(how="all")


def fill_defaults(df: DataFrame, defaults: Dict[str, str]) -> DataFrame:
    for col_name, default_val in defaults.items():
        if col_name in df.columns:
            df = df.fillna({col_name: default_val})
    return df


def normalize_string(df: DataFrame, columns: List[str]) -> DataFrame:
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(
                    F.col(col_name).isNotNull(),
                    F.trim(F.regexp_replace(F.col(col_name), r"\s+", " ")),
                ),
            )
    return df


def deduplicate_by(
    df: DataFrame,
    keys: List[str],
    order_col: str = "processing_timestamp",
) -> DataFrame:
    if df.isEmpty():
        return df

    window_spec = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def explode_array_field(df: DataFrame, field_name: str) -> DataFrame:
    if field_name in df.columns:
        return df.selectExpr(
            f"inline_outer({field_name})",
            "input_file_name() as _source_file",
        )
    return df
