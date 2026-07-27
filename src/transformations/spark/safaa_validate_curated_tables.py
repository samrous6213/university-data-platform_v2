from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    when
)

BASE_PATH = "/opt/spark/work-dir/data/curated/safaa"

TABLES = {
    "faculty_profiles": {
        "path": f"{BASE_PATH}/faculty_profiles",
        "key_column": "record_id",
        "required_columns": [
            "record_id",
            "full_name",
            "institution",
            "source_system",
            "source_url",
            "content_hash",
            "crawl_timestamp"
        ]
    },
    "university_news": {
        "path": f"{BASE_PATH}/university_news",
        "key_column": "record_id",
        "required_columns": [
            "record_id",
            "title",
            "institution",
            "category",
            "source_system",
            "source_url",
            "content_hash",
            "crawl_timestamp"
        ]
    },
    "research_publications": {
        "path": f"{BASE_PATH}/research_publications",
        "key_column": "record_id",
        "required_columns": [
            "record_id",
            "orcid_id",
            "author_name",
            "title",
            "publication_year",
            "source_system",
            "source_url",
            "content_hash",
            "crawl_timestamp"
        ]
    }
}


def validate_table(spark, table_name, config):
    print("=" * 80)
    print(f"VALIDATION TABLE: {table_name}")
    print("=" * 80)

    df = spark.read.parquet(config["path"])

    total_rows = df.count()
    key_col = config["key_column"]

    print(f"Path: {config['path']}")
    print(f"Total rows: {total_rows}")

    print("\nSchema:")
    df.printSchema()

    print("\nRequired columns check:")
    missing_columns = []

    for column_name in config["required_columns"]:
        if column_name in df.columns:
            print(f"OK     - {column_name}")
        else:
            print(f"MISSING - {column_name}")
            missing_columns.append(column_name)

    print("\nNull check for required columns:")

    for column_name in config["required_columns"]:
        if column_name in df.columns:
            null_count = df.filter(
                col(column_name).isNull() | (col(column_name) == "")
            ).count()
            print(f"{column_name}: {null_count} null/empty")

    if key_col in df.columns:
        distinct_keys = df.select(key_col).distinct().count()
        duplicate_keys = total_rows - distinct_keys

        print("\nKey uniqueness:")
        print(f"Distinct {key_col}: {distinct_keys}")
        print(f"Duplicate {key_col}: {duplicate_keys}")

    print("\nSample:")
    df.show(10, truncate=False)

    if missing_columns:
        print(f"\nSTATUS: FAILED - missing columns: {missing_columns}")
        return False

    print("\nSTATUS: PASSED")
    return True


def main():
    spark = (
        SparkSession.builder
        .appName("Safaa Validate Curated Tables")
        .getOrCreate()
    )

    print("=" * 80)
    print("SAFAA CURATED TABLES VALIDATION")
    print("=" * 80)

    all_passed = True

    for table_name, config in TABLES.items():
        result = validate_table(spark, table_name, config)
        all_passed = all_passed and result

    print("=" * 80)

    if all_passed:
        print("GLOBAL VALIDATION STATUS: PASSED")
    else:
        print("GLOBAL VALIDATION STATUS: FAILED")

    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()