"""
Tests unitaires : quality_checks (split_valid_and_quarantine, deduplicate_on_record_id).
"""

from datetime import datetime

from src.transformations.spark.transforms.quality_checks import (
    deduplicate_on_record_id,
    split_valid_and_quarantine,
)

BASE_ROW_VALID = {
    "record_id": "id_1",
    "source_system": "web_crawler",
    "raw_object_path": "s3://raw-web-html/x.html",
    "crawl_timestamp": datetime(2026, 7, 20, 10, 0, 0),
    "normalized_text": "Un texte suffisamment long pour ne pas etre mis en quarantaine par erreur.",
}


def test_row_with_missing_required_field_is_quarantined(spark):
    row_missing_raw_path = {**BASE_ROW_VALID, "raw_object_path": None}
    df = spark.createDataFrame([BASE_ROW_VALID, row_missing_raw_path])

    df_valid, df_quarantine = split_valid_and_quarantine(df)

    assert df_valid.count() == 1
    assert df_quarantine.count() == 1


def test_row_with_short_text_is_quarantined(spark):
    row_short_text = {**BASE_ROW_VALID, "record_id": "id_2", "normalized_text": "trop court"}
    df = spark.createDataFrame([BASE_ROW_VALID, row_short_text])

    df_valid, df_quarantine = split_valid_and_quarantine(df)

    assert df_valid.count() == 1
    assert df_quarantine.count() == 1


def test_valid_row_passes_through(spark):
    df = spark.createDataFrame([BASE_ROW_VALID])
    df_valid, df_quarantine = split_valid_and_quarantine(df)

    assert df_valid.count() == 1
    assert df_quarantine.count() == 0


def test_deduplicate_keeps_most_recent(spark):
    older = {**BASE_ROW_VALID, "crawl_timestamp": datetime(2026, 7, 19, 8, 0, 0)}
    newer = {**BASE_ROW_VALID, "crawl_timestamp": datetime(2026, 7, 20, 8, 0, 0)}
    df = spark.createDataFrame([older, newer])

    df_dedup, dropped = deduplicate_on_record_id(df)

    assert dropped == 1
    assert df_dedup.count() == 1
    result_row = df_dedup.collect()[0]
    assert result_row["crawl_timestamp"] == newer["crawl_timestamp"]


def test_deduplicate_no_duplicates_no_drop(spark):
    row_a = {**BASE_ROW_VALID, "record_id": "id_a"}
    row_b = {**BASE_ROW_VALID, "record_id": "id_b"}
    df = spark.createDataFrame([row_a, row_b])

    df_dedup, dropped = deduplicate_on_record_id(df)

    assert dropped == 0
    assert df_dedup.count() == 2