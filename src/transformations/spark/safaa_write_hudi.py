from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

CURATED_BASE_PATH = "/opt/spark/work-dir/data/curated/safaa"
HUDI_BASE_PATH = "/opt/spark/work-dir/data/hudi/safaa"

TABLES = {
    "faculty_profiles": {
        "input_path": f"{CURATED_BASE_PATH}/faculty_profiles",
        "output_path": f"{HUDI_BASE_PATH}/faculty_profiles",
        "record_key": "record_id",
        "precombine": "processed_at"
    },
    "university_news": {
        "input_path": f"{CURATED_BASE_PATH}/university_news",
        "output_path": f"{HUDI_BASE_PATH}/university_news",
        "record_key": "record_id",
        "precombine": "processed_at"
    },
    "research_publications": {
        "input_path": f"{CURATED_BASE_PATH}/research_publications",
        "output_path": f"{HUDI_BASE_PATH}/research_publications",
        "record_key": "record_id",
        "precombine": "processed_at"
    }
}


def write_hudi_table(spark, table_name, config):
    print("=" * 80)
    print(f"WRITING HUDI TABLE: {table_name}")
    print("=" * 80)

    input_path = config["input_path"]
    output_path = config["output_path"]
    record_key = config["record_key"]
    precombine = config["precombine"]

    print(f"Input Parquet: {input_path}")
    print(f"Output Hudi:   {output_path}")

    df = spark.read.parquet(input_path)

    print("Input schema:")
    df.printSchema()

    total_rows = df.count()
    print(f"Input rows: {total_rows}")

    if record_key not in df.columns:
        raise Exception(f"Missing record key column: {record_key}")

    if precombine not in df.columns:
        raise Exception(f"Missing precombine column: {precombine}")

    null_keys = df.filter(col(record_key).isNull() | (col(record_key) == "")).count()

    if null_keys > 0:
        raise Exception(f"Table {table_name} has {null_keys} null/empty record_id values")

    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.datasource.write.partitionpath.field": "",
        "hoodie.datasource.write.hive_style_partitioning": "false"
    }

    (
        df.write
        .format("hudi")
        .options(**hudi_options)
        .mode("overwrite")
        .save(output_path)
    )

    print("Hudi write completed.")

    hudi_df = spark.read.format("hudi").load(output_path)
    hudi_count = hudi_df.count()

    print(f"Hudi rows read back: {hudi_count}")

    if hudi_count != total_rows:
        raise Exception(
            f"Row count mismatch for {table_name}: input={total_rows}, hudi={hudi_count}"
        )

    print(f"STATUS: PASSED - {table_name}")


def main():
    spark = (
        SparkSession.builder
        .appName("Safaa Write Hudi Tables")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("SAFAA WRITE HUDI TABLES")
    print("=" * 80)

    os.makedirs(HUDI_BASE_PATH, exist_ok=True)

    for table_name, config in TABLES.items():
        write_hudi_table(spark, table_name, config)

    print("=" * 80)
    print("ALL HUDI TABLES WRITTEN SUCCESSFULLY")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()