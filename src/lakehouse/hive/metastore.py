"""
Utilitaires de vérification du Hive Metastore, une fois les tables Hudi écrites.
"""

from src.lakehouse.hudi.tables import HIVE_DATABASE, ALL_TABLES


def list_registered_tables(spark) -> list:
    spark.sql(f"USE {HIVE_DATABASE}")
    rows = spark.sql("SHOW TABLES").collect()
    return [r["tableName"] for r in rows]


def check_all_tables_registered(spark) -> None:
    registered = set(list_registered_tables(spark))
    expected = {t.name for t in ALL_TABLES}
    missing = expected - registered
    if missing:
        print(f"⚠️  Tables manquantes dans Hive : {missing}")
    else:
        print(f"✅ Toutes les tables sont dans Hive : {sorted(registered)}")


def preview_table(spark, table_name: str, n: int = 10):
    spark.sql(f"USE {HIVE_DATABASE}")
    spark.sql(f"SELECT * FROM {table_name} LIMIT {n}").show(truncate=False)