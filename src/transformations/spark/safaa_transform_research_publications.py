from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    regexp_replace,
    sha2,
    concat_ws,
    when,
    explode_outer,
    current_timestamp,
    length
)

INPUT_PATH = "/opt/spark/work-dir/data/spark_input/safaa/research_publications/*.json"
OUTPUT_PATH = "/opt/spark/work-dir/data/curated/safaa/research_publications"


def main():
    spark = (
        SparkSession.builder
        .appName("Safaa Transform Research Publications")
        .getOrCreate()
    )

    print("=" * 70)
    print("SAFAA TRANSFORM - RESEARCH_PUBLICATIONS V2")
    print("=" * 70)
    print(f"Input: {INPUT_PATH}")
    print(f"Output: {OUTPUT_PATH}")

    raw_df = spark.read.option("multiLine", "true").json(INPUT_PATH)

    print("Raw schema:")
    raw_df.printSchema()

    if "hudi_ready_data" not in raw_df.columns:
        print("ERROR: hudi_ready_data not found in ORCID raw JSON.")
        spark.stop()
        return

    publications_raw = raw_df.select(
        explode_outer(col("hudi_ready_data.research_publications")).alias("item")
    )

    print("Exploded publications schema:")
    publications_raw.printSchema()

    publications_df = publications_raw.select(
        col("item.record_id").alias("record_id"),
        col("item.source_system").alias("source_system"),
        col("item.source_url").alias("source_url"),
        col("item.content_hash").alias("content_hash"),
        col("item.crawl_timestamp").alias("crawl_timestamp"),
        col("item.business_timestamp").alias("business_timestamp"),
        col("item.is_deleted").alias("is_deleted"),
        col("item.language").alias("language"),
        col("item.orcid_id").alias("orcid_id"),
        col("item.author_name").alias("author_name"),
        col("item.title").alias("title"),
        col("item.doi").alias("doi"),
        col("item.publication_year").alias("publication_year"),
        col("item.journal").alias("journal"),
        col("item.publication_type").alias("publication_type"),
        col("item.source").alias("source"),
        col("item.scrape_timestamp").alias("scrape_timestamp")
    )

    clean_df = (
        publications_df
        .withColumn(
            "source_system",
            when(
                col("source_system").isNull() | (trim(col("source_system")) == ""),
                lit("orcid_api")
            ).otherwise(trim(col("source_system")))
        )
        .withColumn("source_url", trim(col("source_url")))
        .withColumn("content_hash", trim(col("content_hash")))
        .withColumn("crawl_timestamp", trim(col("crawl_timestamp")))
        .withColumn("business_timestamp", trim(col("business_timestamp")))
        .withColumn(
            "language",
            when(
                col("language").isNull() | (trim(col("language")) == ""),
                lit("en")
            ).otherwise(trim(col("language")))
        )
        .withColumn("orcid_id", trim(col("orcid_id")))
        .withColumn("author_name", trim(regexp_replace(col("author_name"), r"\s+", " ")))
        .withColumn("title", trim(regexp_replace(col("title"), r"\s+", " ")))
        .withColumn("doi", trim(col("doi")))
        .withColumn("publication_year", trim(col("publication_year")))
        .withColumn(
            "publication_year",
            when(
                col("publication_year").isNull() | (col("publication_year") == ""),
                lit("unknown")
            ).otherwise(col("publication_year"))
        )
        .withColumn("journal", trim(regexp_replace(col("journal"), r"\s+", " ")))
        .withColumn("publication_type", trim(col("publication_type")))
        .withColumn(
            "source",
            when(
                col("source").isNull() | (trim(col("source")) == ""),
                lit("orcid")
            ).otherwise(trim(col("source")))
        )

        # Data quality filters
        .filter(col("title").isNotNull())
        .filter(col("title") != "")
        .filter(col("title") != "*")
        .filter(length(col("title")) >= 5)
        .filter(col("orcid_id").isNotNull())
        .filter(col("orcid_id") != "")
        .filter(col("author_name").isNotNull())
        .filter(col("author_name") != "")

        # Generate record_id if missing
        .withColumn(
            "record_id",
            when(
                col("record_id").isNull() | (col("record_id") == ""),
                sha2(
                    concat_ws(
                        "||",
                        col("orcid_id"),
                        col("title"),
                        col("publication_year"),
                        col("source_url")
                    ),
                    256
                )
            ).otherwise(col("record_id"))
        )

        # Generate content_hash if missing
        .withColumn(
            "content_hash",
            when(
                col("content_hash").isNull() | (col("content_hash") == ""),
                sha2(
                    concat_ws(
                        "||",
                        col("orcid_id"),
                        col("author_name"),
                        col("title"),
                        col("doi"),
                        col("publication_year"),
                        col("journal"),
                        col("source_url")
                    ),
                    256
                )
            ).otherwise(col("content_hash"))
        )

        .withColumn("curated_table", lit("research_publications"))
        .withColumn("processed_at", current_timestamp())

        # Deduplication
        .dropDuplicates(["orcid_id", "title", "publication_year", "source_url"])
    )

    print("Clean schema:")
    clean_df.printSchema()

    print("Sample data:")
    clean_df.show(20, truncate=False)

    total = clean_df.count()
    print(f"Final research_publications count: {total}")

    clean_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    print("=" * 70)
    print("RESEARCH_PUBLICATIONS TRANSFORM COMPLETED")
    print(f"Rows written: {total}")
    print(f"Output path: {OUTPUT_PATH}")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()