"""
Fonction générique d'écriture (upsert) vers une table Hudi.
"""

from pyspark.sql import DataFrame

from src.lakehouse.hudi.tables import HudiTableConfig, HIVE_DATABASE


def ensure_database_exists(spark) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")


def upsert_to_hudi(df: DataFrame, table_config: HudiTableConfig) -> None:
    required_cols = {table_config.record_key, table_config.partition_field, table_config.precombine_field}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"[{table_config.name}] Colonnes manquantes : {missing}")

    count = df.count()
    print(f"→ Upsert vers Hudi : {table_config.hive_table_name} ({count} lignes)")
    if count == 0:
        print(f"⚠️  Aucune ligne pour '{table_config.name}', on saute.")
        return

    (
        df.write.format("hudi")
        .options(**table_config.hudi_options())
        .mode("append")
        .save(table_config.base_path)
    )
    print(f"✅ Table '{table_config.hive_table_name}' synchronisée avec Hive.")