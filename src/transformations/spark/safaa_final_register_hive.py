from pyspark.sql import SparkSession
import os

DATABASE = "safaa_curated"

HUDI_BASE_PATH = "/opt/spark/work-dir/data/hudi/safaa"
PARQUET_BASE_PATH = "/tmp/safaa_hive_tables"

TABLES = {
    "faculty_profiles": f"{HUDI_BASE_PATH}/faculty_profiles",
    "university_news": f"{HUDI_BASE_PATH}/university_news",
    "research_publications": f"{HUDI_BASE_PATH}/research_publications",
}

TECHNICAL_COLS = [
    "_hoodie_commit_time",
    "_hoodie_commit_seqno",
    "_hoodie_record_key",
    "_hoodie_partition_path",
    "_hoodie_file_name",
]

def sql_type(data_type):
    return data_type.simpleString()

def table_exists(spark, table_name):
    rows = spark.sql(f"SHOW TABLES IN {DATABASE}").collect()
    return any(row.tableName == table_name for row in rows)

def main():
    print("=" * 80)
    print("SAFAA FINAL HIVE REGISTRATION")
    print("=" * 80)

    os.makedirs(PARQUET_BASE_PATH, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Safaa Final Hive Registration")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.hadoop.hive.metastore.uris", "thrift://safaa-hive-metastore:9083")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DATABASE} "
        f"LOCATION 'file:/tmp/safaa_hive/warehouse/{DATABASE}.db'"
    )

    for table_name, hudi_path in TABLES.items():
        full_table = f"{DATABASE}.{table_name}"
        parquet_path = f"{PARQUET_BASE_PATH}/{table_name}"

        print("=" * 80)
        print(f"PROCESSING TABLE: {full_table}")
        print("=" * 80)

        if table_exists(spark, table_name):
            count_existing = spark.sql(f"SELECT COUNT(*) AS total FROM {full_table}").collect()[0]["total"]
            print(f"TABLE ALREADY EXISTS: {full_table}")
            print(f"Existing Hive rows: {count_existing}")
            print(f"STATUS: SKIPPED - {full_table}")
            continue

        print(f"Reading Hudi: {hudi_path}")
        df = spark.read.format("hudi").load(hudi_path)

        clean_cols = [c for c in df.columns if c not in TECHNICAL_COLS]
        df_clean = df.select(clean_cols)

        hudi_count = df_clean.count()
        print(f"Rows from Hudi: {hudi_count}")
        print(f"Writing Parquet: {parquet_path}")

        df_clean.write.mode("overwrite").parquet(parquet_path)

        columns_sql = []
        for field in df_clean.schema.fields:
            columns_sql.append(f"`{field.name}` {sql_type(field.dataType)}")

        columns_sql_text = ",\n    ".join(columns_sql)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            {columns_sql_text}
        )
        USING PARQUET
        LOCATION 'file:{parquet_path}'
        """

        print("Creating Hive table...")
        spark.sql(create_sql)

        hive_count = spark.sql(f"SELECT COUNT(*) AS total FROM {full_table}").collect()[0]["total"]
        print(f"Hive rows: {hive_count}")

        if hive_count != hudi_count:
            raise Exception(f"COUNT MISMATCH for {full_table}: Hudi={hudi_count}, Hive={hive_count}")

        print(f"STATUS: PASSED - {full_table}")

    print("=" * 80)
    print("FINAL HIVE TABLES")
    spark.sql(f"SHOW TABLES IN {DATABASE}").show(truncate=False)

    print("=" * 80)
    print("FINAL COUNTS")
    for table_name in TABLES.keys():
        if table_exists(spark, table_name):
            full_table = f"{DATABASE}.{table_name}"
            spark.sql(f"SELECT '{table_name}' AS table_name, COUNT(*) AS total_rows FROM {full_table}").show(truncate=False)

    print("=" * 80)
    print("SAFAA FINAL HIVE REGISTRATION COMPLETED")
    print("=" * 80)

    spark.stop()

if __name__ == "__main__":
    main()