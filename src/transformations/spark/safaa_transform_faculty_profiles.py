from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    regexp_replace,
    sha2,
    concat_ws,
    when,
    coalesce,
    explode_outer,
    current_timestamp,
    length
)

INPUT_PATH = "/opt/spark/work-dir/data/spark_input/safaa/faculty_profiles/*.json"
OUTPUT_PATH = "/opt/spark/work-dir/data/curated/safaa/faculty_profiles"


def main():
    spark = (
        SparkSession.builder
        .appName("Safaa Transform Faculty Profiles")
        .getOrCreate()
    )

    print("=" * 70)
    print("SAFAA TRANSFORM - FACULTY_PROFILES")
    print("=" * 70)
    print(f"Input: {INPUT_PATH}")
    print(f"Output: {OUTPUT_PATH}")

    raw_df = spark.read.option("multiLine", "true").json(INPUT_PATH)

    print("Raw schema:")
    raw_df.printSchema()

    extracted_dfs = []

    if "faculty_members" in raw_df.columns:
        df_members = raw_df.select(
            explode_outer(col("faculty_members")).alias("item"),
            col("scrape_timestamp").alias("top_scrape_timestamp")
        )
        extracted_dfs.append(df_members)

    if "faculty_items" in raw_df.columns:
        df_items = raw_df.select(
            explode_outer(col("faculty_items")).alias("item"),
            col("scrape_timestamp").alias("top_scrape_timestamp")
        )
        extracted_dfs.append(df_items)

    if not extracted_dfs:
        print("ERROR: No faculty_members or faculty_items found in raw JSON.")
        spark.stop()
        return

    faculty_raw = extracted_dfs[0]

    for df in extracted_dfs[1:]:
        faculty_raw = faculty_raw.unionByName(df, allowMissingColumns=True)

    faculty_df = faculty_raw.select(
        col("item.record_id").alias("record_id"),
        col("item.full_name").alias("full_name"),
        col("item.first_name").alias("first_name"),
        col("item.last_name").alias("last_name"),
        col("item.email").alias("email"),
        col("item.institution").alias("institution"),
        col("item.department").alias("department"),
        coalesce(
            col("item.source_system"),
            col("item.source"),
            lit("web_scraper")
        ).alias("source_system"),
        col("item.source_url").alias("source_url"),
        col("item.content_hash").alias("content_hash"),
        coalesce(
            col("item.crawl_timestamp"),
            col("top_scrape_timestamp")
        ).alias("crawl_timestamp")
    )

    clean_df = (
        faculty_df
        .withColumn("full_name", trim(regexp_replace(col("full_name"), r"\s+", " ")))
        .withColumn("first_name", trim(regexp_replace(col("first_name"), r"\s+", " ")))
        .withColumn("last_name", trim(regexp_replace(col("last_name"), r"\s+", " ")))
        .withColumn("email", trim(col("email")))
        .withColumn("institution", trim(col("institution")))
        .withColumn("department", trim(col("department")))
        .withColumn("source_url", trim(col("source_url")))

        # Keep only real person-like names.
        # This removes garbage values such as "*".
        .withColumn(
            "name_letters",
            regexp_replace(col("full_name"), r"[^A-Za-zÀ-ÖØ-öø-ÿ]", "")
        )
        .filter(col("full_name").isNotNull())
        .filter(col("full_name") != "")
        .filter(col("full_name") != "*")
        .filter(length(col("full_name")) >= 3)
        .filter(length(col("name_letters")) >= 2)

        .withColumn(
            "record_id",
            when(
                col("record_id").isNull() | (col("record_id") == ""),
                sha2(
                    concat_ws(
                        "||",
                        col("full_name"),
                        col("institution"),
                        col("email")
                    ),
                    256
                )
            ).otherwise(col("record_id"))
        )

        .withColumn(
            "content_hash",
            when(
                col("content_hash").isNull() | (col("content_hash") == ""),
                sha2(
                    concat_ws(
                        "||",
                        col("full_name"),
                        col("institution"),
                        col("department"),
                        col("source_url")
                    ),
                    256
                )
            ).otherwise(col("content_hash"))
        )

        .withColumn("curated_table", lit("faculty_profiles"))
        .withColumn("processed_at", current_timestamp())

        .dropDuplicates(["full_name", "institution", "email"])
        .drop("name_letters")
    )

    print("Clean schema:")
    clean_df.printSchema()

    print("Sample data:")
    clean_df.show(20, truncate=False)

    total = clean_df.count()
    print(f"Final faculty_profiles count: {total}")

    clean_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    print("=" * 70)
    print("FACULTY_PROFILES TRANSFORM COMPLETED")
    print(f"Rows written: {total}")
    print(f"Output path: {OUTPUT_PATH}")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()