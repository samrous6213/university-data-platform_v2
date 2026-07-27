import os
import re
from pyspark.sql import SparkSession


OUTPUT_SQL = "/opt/spark/work-dir/safaa_metabase_export.sql"

TABLES = [
    {
        "table_name": "faculty_profiles",
        "path": "/opt/spark/work-dir/data/curated/safaa/faculty_profiles",
    },
    {
        "table_name": "university_news",
        "path": "/opt/spark/work-dir/data/curated/safaa/university_news",
    },
    {
        "table_name": "research_publications",
        "path": "/opt/spark/work-dir/data/curated/safaa/research_publications",
    },
]


def clean_column_name(name):
    name = name.lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "col"
    if name[0].isdigit():
        name = "col_" + name
    return name


def sql_escape(value):
    if value is None:
        return "NULL"

    value = str(value)
    value = value.replace("\\", "\\\\")
    value = value.replace("'", "''")
    value = value.replace("\x00", "")
    return "'" + value + "'"


def export_table_to_sql(spark, sql_file, table_name, path):
    print("=" * 80)
    print(f"EXPORTING TABLE TO POSTGRES SQL: {table_name}")
    print("=" * 80)
    print("Input path:", path)

    df = spark.read.parquet(path)

    original_columns = df.columns
    clean_columns = [clean_column_name(c) for c in original_columns]

    count_rows = df.count()
    print(f"Rows: {count_rows}")
    print(f"Columns: {len(clean_columns)}")

    full_table_name = f"safaa_dashboard.{table_name}"

    sql_file.write(f"DROP TABLE IF EXISTS {full_table_name};\n")

    create_columns = []
    for col in clean_columns:
        create_columns.append(f'    "{col}" TEXT')

    create_sql = (
        f"CREATE TABLE {full_table_name} (\n"
        + ",\n".join(create_columns)
        + "\n);\n\n"
    )

    sql_file.write(create_sql)

    insert_columns = ", ".join([f'"{c}"' for c in clean_columns])

    batch_values = []
    batch_size = 200
    inserted = 0

    for row in df.toLocalIterator():
        row_dict = row.asDict(recursive=True)

        values = []
        for col in original_columns:
            values.append(sql_escape(row_dict.get(col)))

        batch_values.append("(" + ", ".join(values) + ")")

        if len(batch_values) >= batch_size:
            sql_file.write(
                f"INSERT INTO {full_table_name} ({insert_columns}) VALUES\n"
                + ",\n".join(batch_values)
                + ";\n\n"
            )
            inserted += len(batch_values)
            print(f"Prepared {inserted}/{count_rows} rows for {table_name}")
            batch_values = []

    if batch_values:
        sql_file.write(
            f"INSERT INTO {full_table_name} ({insert_columns}) VALUES\n"
            + ",\n".join(batch_values)
            + ";\n\n"
        )
        inserted += len(batch_values)

    sql_file.write(f"SELECT COUNT(*) AS {table_name}_count FROM {full_table_name};\n\n")

    print(f"SQL export prepared for {table_name}: {inserted} rows")


def main():
    print("=" * 80)
    print("SAFAA EXPORT CURATED TABLES TO POSTGRES SQL")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName("Safaa Export To PostgreSQL SQL")
        .getOrCreate()
    )

    with open(OUTPUT_SQL, "w", encoding="utf-8") as sql_file:
        sql_file.write("CREATE SCHEMA IF NOT EXISTS safaa_dashboard;\n\n")

        for table in TABLES:
            export_table_to_sql(
                spark=spark,
                sql_file=sql_file,
                table_name=table["table_name"],
                path=table["path"],
            )

    spark.stop()

    print("=" * 80)
    print("POSTGRES SQL FILE CREATED SUCCESSFULLY")
    print("=" * 80)
    print("Output SQL:", OUTPUT_SQL)


if __name__ == "__main__":
    main()