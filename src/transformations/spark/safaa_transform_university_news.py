import re

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
    length,
    udf
)
from pyspark.sql.types import StructType, ArrayType, StringType


INPUT_PATH = "/opt/spark/work-dir/data/spark_input/safaa/university_news/*.json"
OUTPUT_PATH = "/opt/spark/work-dir/data/curated/safaa/university_news"


def fix_encoding_value(value):
    """
    Fix common mojibake/encoding issues coming from web pages.
    Example:
    - PÃ©dagogiques -> Pédagogiques
    - 5Ã¨me -> 5ème
    - dâ€™Open -> d’Open
    """
    if value is None:
        return None

    text = str(value)

    replacements = {
        "Ã©": "é",
        "Ã¨": "è",
        "Ãª": "ê",
        "Ã«": "ë",
        "Ã ": "à",
        "Ã¢": "â",
        "Ã´": "ô",
        "Ã»": "û",
        "Ã¹": "ù",
        "Ã®": "î",
        "Ã¯": "ï",
        "Ã§": "ç",
        "Ã‰": "É",
        "Ãˆ": "È",
        "ÃŠ": "Ê",
        "Ã€": "À",
        "Ã‡": "Ç",
        "Ãdition": "Édition",
        "Ãtudes": "Études",
        "Ãcole": "École",
        "â€™": "’",
        "â€˜": "‘",
        "â€œ": "“",
        "â€": "”",
        "â€“": "–",
        "â€”": "—",
        "Â«": "«",
        "Â»": "»",
        "Â°": "°",
        "Â": ""
    }

    def apply_replacements(s):
        if s is None:
            return None

        fixed = s

        for bad, good in replacements.items():
            fixed = fixed.replace(bad, good)

        # Cases like dâOpen -> d’Open when the full â€™ sequence was broken.
        fixed = re.sub(r"\b([dDlLmMtTsScC])â([A-ZÀ-ÖØ-Þ])", r"\1’\2", fixed)

        fixed = re.sub(r"\s+", " ", fixed).strip()

        return fixed

    candidates = [apply_replacements(text)]

    # Try latin1 -> utf8 recovery for common mojibake.
    if any(marker in text for marker in ["Ã", "Â", "â"]):
        try:
            recovered = text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
            candidates.append(apply_replacements(recovered))
        except Exception:
            pass

    def bad_score(s):
        if s is None:
            return 999999
        return s.count("Ã") + s.count("Â") + s.count("â")

    # Choose the candidate with fewer mojibake markers.
    # If equal, keep the longer one because it probably lost fewer characters.
    best = sorted(candidates, key=lambda s: (bad_score(s), -len(s or "")))[0]

    return best


fix_encoding_udf = udf(fix_encoding_value, StringType())


def get_item_field(df, field_name):
    item_schema = df.schema["item"].dataType

    if isinstance(item_schema, StructType):
        field_names = [f.name for f in item_schema.fields]
        if field_name in field_names:
            return col(f"item.{field_name}")

    return lit(None).cast("string")


def first_existing_field(df, field_names):
    cols = [get_item_field(df, name) for name in field_names]
    return coalesce(*cols)


def main():
    spark = (
        SparkSession.builder
        .appName("Safaa Transform University News")
        .getOrCreate()
    )

    print("=" * 70)
    print("SAFAA TRANSFORM - UNIVERSITY_NEWS")
    print("=" * 70)
    print(f"Input: {INPUT_PATH}")
    print(f"Output: {OUTPUT_PATH}")

    raw_df = spark.read.option("multiLine", "true").json(INPUT_PATH)

    print("Raw schema:")
    raw_df.printSchema()

    extracted_dfs = []

    possible_news_arrays = [
        "news_items",
        "university_news",
        "news",
        "articles",
        "items"
    ]

    for array_col in possible_news_arrays:
        if array_col in raw_df.columns and isinstance(raw_df.schema[array_col].dataType, ArrayType):

            top_scrape_timestamp = (
                col("scrape_timestamp")
                if "scrape_timestamp" in raw_df.columns
                else lit(None).cast("string")
            )

            top_source = (
                col("source")
                if "source" in raw_df.columns
                else lit(None).cast("string")
            )

            df_items = raw_df.select(
                explode_outer(col(array_col)).alias("item"),
                top_scrape_timestamp.alias("top_scrape_timestamp"),
                top_source.alias("top_source")
            )

            extracted_dfs.append(df_items)

    if not extracted_dfs:
        print("ERROR: No news array found in raw JSON.")
        print("Expected one of: news_items, university_news, news, articles, items")
        spark.stop()
        return

    news_raw = extracted_dfs[0]

    for df in extracted_dfs[1:]:
        news_raw = news_raw.unionByName(df, allowMissingColumns=True)

    print("Exploded news schema:")
    news_raw.printSchema()

    news_df = news_raw.select(
        first_existing_field(
            news_raw,
            ["record_id", "id", "news_id"]
        ).alias("record_id"),

        first_existing_field(
            news_raw,
            ["title", "news_title", "headline", "name"]
        ).alias("title"),

        first_existing_field(
            news_raw,
            ["summary", "description", "excerpt", "content", "text", "normalized_text"]
        ).alias("summary"),

        first_existing_field(
            news_raw,
            ["institution", "faculty", "school"]
        ).alias("institution"),

        first_existing_field(
            news_raw,
            ["category", "type"]
        ).alias("category"),

        first_existing_field(
            news_raw,
            ["published_date", "date", "publication_date", "business_timestamp"]
        ).alias("published_date"),

        coalesce(
            first_existing_field(news_raw, ["source_system", "source"]),
            col("top_source"),
            lit("web_scraper")
        ).alias("source_system"),

        first_existing_field(
            news_raw,
            ["source_url", "url", "link"]
        ).alias("source_url"),

        first_existing_field(
            news_raw,
            ["content_hash"]
        ).alias("content_hash"),

        coalesce(
            first_existing_field(news_raw, ["crawl_timestamp", "scrape_timestamp"]),
            col("top_scrape_timestamp")
        ).alias("crawl_timestamp"),

        first_existing_field(
            news_raw,
            ["language"]
        ).alias("language")
    )

    clean_df = (
        news_df
        .withColumn("title", trim(regexp_replace(col("title"), r"\s+", " ")))
        .withColumn("summary", trim(regexp_replace(col("summary"), r"\s+", " ")))
        .withColumn("institution", trim(col("institution")))
        .withColumn("category", trim(col("category")))
        .withColumn("published_date", trim(col("published_date")))
        .withColumn("source_system", trim(col("source_system")))
        .withColumn("source_url", trim(col("source_url")))
        .withColumn("language", trim(col("language")))

        # Fix encoding/mojibake issues in curated fields.
        .withColumn("title", fix_encoding_udf(col("title")))
        .withColumn("summary", fix_encoding_udf(col("summary")))
        .withColumn("institution", fix_encoding_udf(col("institution")))
        .withColumn("category", fix_encoding_udf(col("category")))

        # Data quality filters
        .filter(col("title").isNotNull())
        .filter(col("title") != "")
        .filter(col("title") != "*")
        .filter(length(col("title")) >= 5)

        # Generate record_id if missing
        .withColumn(
            "record_id",
            when(
                col("record_id").isNull() | (col("record_id") == ""),
                sha2(
                    concat_ws(
                        "||",
                        col("title"),
                        col("institution"),
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
                        col("title"),
                        col("summary"),
                        col("institution"),
                        col("source_url")
                    ),
                    256
                )
            ).otherwise(col("content_hash"))
        )

        .withColumn("curated_table", lit("university_news"))
        .withColumn("processed_at", current_timestamp())

        # Deduplication
        .dropDuplicates(["title", "institution", "source_url"])
    )

    print("Clean schema:")
    clean_df.printSchema()

    print("Sample data:")
    clean_df.show(20, truncate=False)

    total = clean_df.count()
    print(f"Final university_news count: {total}")

    clean_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    print("=" * 70)
    print("UNIVERSITY_NEWS TRANSFORM COMPLETED")
    print(f"Rows written: {total}")
    print(f"Output path: {OUTPUT_PATH}")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()