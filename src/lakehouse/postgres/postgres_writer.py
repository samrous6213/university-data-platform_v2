"""
Writer psycopg2 (PAS Spark JDBC) pour dupliquer les tables curated vers
Postgres (base `analytics`), consommee par Metabase pour le dashboard.

Pourquoi psycopg2 et pas Spark JDBC :
Sous Windows, SparkContext.addJar() ne met a jour de facon fiable que le
classpath des EXECUTORS, pas celui utilise par Utils.classForName cote
DRIVER (c'est cette methode interne, differente de java.lang.Class.forName
via py4j, que Spark SQL's DriverRegistry appelle) -> ClassNotFoundException
systematique sur org.postgresql.Driver, meme avec une URI de fichier
correcte et un chargement force de la classe. Le contournement propre
demanderait de passer le jar a la CREATION de la SparkSession
(spark.jars), donc de modifier spark_session.py.

Vu la taille des tables curated pour ce MVP (dizaines/centaines de lignes),
on evite tout ce probleme de classpath Java : les donnees sont rapatriees
sur le driver (df.collect()) et inserees avec un connecteur Python pur
(psycopg2), sans aucune dependance a un jar Java cote Spark.

Complementaire a hudi_writer.py, pas un remplacement : Hudi/Hive restent la
source de verite versionnee (exigence brief "Register in Hive for SQL
access"). Postgres est une copie plate dediee au BI, ecrite juste apres
Hudi dans les pipelines.
"""

import logging

import psycopg2
import psycopg2.extras
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    TimestampType,
)

from configs.spark_config import (
    MAX_WRITE_RETRIES,
    POSTGRES_ANALYTICS_DB,
    POSTGRES_ANALYTICS_HOST,
    POSTGRES_ANALYTICS_PASSWORD,
    POSTGRES_ANALYTICS_PORT,
    POSTGRES_ANALYTICS_USER,
    POSTGRES_TABLES,
    RETRY_BACKOFF_FACTOR,
)
from src.transformations.spark.utils.retry import retry

logger = logging.getLogger(__name__)


def _get_connection():
    return psycopg2.connect(
        host=POSTGRES_ANALYTICS_HOST,
        port=POSTGRES_ANALYTICS_PORT,
        dbname=POSTGRES_ANALYTICS_DB,
        user=POSTGRES_ANALYTICS_USER,
        password=POSTGRES_ANALYTICS_PASSWORD,
        connect_timeout=10,
    )


def _pg_type(spark_type) -> str:
    """Mapping simple type Spark -> type Postgres pour un CREATE TABLE generique."""
    if isinstance(spark_type, BooleanType):
        return "BOOLEAN"
    if isinstance(spark_type, (IntegerType, LongType)):
        return "BIGINT"
    if isinstance(spark_type, TimestampType):
        return "TIMESTAMP"
    return "TEXT"  # StringType, ArrayType (aplati en texte), et fallback


def _ensure_table(conn, pg_table: str, df: DataFrame) -> None:
    columns_sql = ", ".join(f'"{f.name}" {_pg_type(f.dataType)}' for f in df.schema.fields)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{pg_table}" ({columns_sql});')
    conn.commit()


def _row_to_tuple(row, field_names: list) -> tuple:
    values = []
    for name in field_names:
        v = row[name]
        if isinstance(v, list):
            v = ", ".join(str(x) for x in v if x is not None) or None
        values.append(v)
    return tuple(values)


@retry(max_attempts=MAX_WRITE_RETRIES, backoff_factor=RETRY_BACKOFF_FACTOR, exceptions=(Exception,))
def _write_with_retry(pg_table: str, field_names: list, rows: list) -> None:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE "{pg_table}";')
            columns_sql = ", ".join(f'"{c}"' for c in field_names)
            query = f'INSERT INTO "{pg_table}" ({columns_sql}) VALUES %s'
            psycopg2.extras.execute_values(cur, query, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()


def sync_to_postgres(df: DataFrame, table_name: str) -> int:
    """
    Ecrit un snapshot complet de la table curated `table_name` dans Postgres
    (base `analytics`), pour consommation par Metabase.

    A appeler juste apres upsert_to_hudi(df_dedup, table_name), avec le meme
    DataFrame deja deduplique/valide. Cree la table Postgres automatiquement
    au premier run si elle n'existe pas encore (types simples : BOOLEAN,
    BIGINT, TIMESTAMP, TEXT -- suffisant pour un dashboard BI).
    """
    if table_name not in POSTGRES_TABLES:
        raise ValueError(f"Table '{table_name}' non declaree dans POSTGRES_TABLES")

    record_count = df.count()
    if record_count == 0:
        logger.warning("Aucune ligne a synchroniser vers Postgres pour '%s'.", table_name)
        return 0

    pg_table = POSTGRES_TABLES[table_name]
    field_names = [f.name for f in df.schema.fields]

    conn = _get_connection()
    try:
        _ensure_table(conn, pg_table, df)
    finally:
        conn.close()

    # Rapatrie les lignes sur le driver -- OK pour un MVP (tables curated de
    # taille modeste : dizaines/centaines de lignes, pas un big data job).
    collected_rows = df.collect()
    rows = [_row_to_tuple(r, field_names) for r in collected_rows]

    logger.info("Synchronisation Postgres (psycopg2) : table=%s lignes=%s", pg_table, record_count)
    _write_with_retry(pg_table, field_names, rows)
    logger.info("Synchronisation Postgres terminee : table=%s", pg_table)
    return record_count