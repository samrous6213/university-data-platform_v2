from pyspark.sql import SparkSession

DATABASE = "safaa_curated"

def main():
    print("=" * 80)
    print("SAFAA CHECK HIVE TABLES")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName("Safaa Check Hive Tables")
        .config("spark.hadoop.hive.metastore.uris", "thrift://safaa-hive-metastore:9083")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("SHOW DATABASES")
    spark.sql("SHOW DATABASES").show(truncate=False)

    print(f"SHOW TABLES IN {DATABASE}")
    spark.sql(f"SHOW TABLES IN {DATABASE}").show(truncate=False)

    tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {DATABASE}").collect()]

    for table in tables:
        full_name = f"{DATABASE}.{table}"
        print("=" * 80)
        print(f"COUNT TABLE: {full_name}")
        spark.sql(f"SELECT COUNT(*) AS total_rows FROM {full_name}").show(truncate=False)

    print("=" * 80)
    print("CHECK COMPLETED")
    print("=" * 80)

    spark.stop()

if __name__ == "__main__":
    main()