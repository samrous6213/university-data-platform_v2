r"""
Étape 7 (bis) : Export des tables curated Hudi -> Postgres (base "analytics").
Sert de couche de service légère pour Metabase, sans dépendre de Trino/Presto.

Peut être lancé directement :
    cd D:\university-data-platform_v2\src\transformations\spark
    python export_to_postgres.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType, StructField

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
HUDI_WAREHOUSE_PATH = os.getenv("HUDI_WAREHOUSE_PATH", "s3a://curated-zone/hudi_warehouse")

# --- Connexion Postgres (base "analytics" créée manuellement au préalable) ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "hive")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "hive")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

TABLES = [
    "faculty_profiles",
    "course_catalog",
    "research_publications",
    "university_news",
]


def get_spark_session(app_name: str = "university-etape7-export") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def make_schema_nullable(spark: SparkSession, df):
    """
    Force tout le schéma en nullable=True.
    Certaines colonnes Hudi sont déclarées 'nullable=false' alors que les
    données réelles contiennent parfois des valeurs null : le moteur de
    génération de code de Spark fait confiance à cette déclaration et plante
    (NullPointerException) sur l'écriture JDBC / la collecte toPandas().
    Reconstruire le DataFrame avec un schéma entièrement nullable réactive
    les vérifications de null et évite le crash.
    """
    nullable_schema = StructType([
        StructField(f.name, f.dataType, True, f.metadata) for f in df.schema.fields
    ])
    return spark.createDataFrame(df.rdd, nullable_schema)


def fill_declared_nonnull(df):
    """
    Certaines colonnes Hudi sont déclarées 'nullable=false' alors que les
    données réelles contiennent parfois des valeurs null. Le moteur de
    génération de code de Spark fait confiance à cette déclaration et plante
    (NullPointerException) sur l'écriture JDBC. On remplace ces valeurs null
    par un défaut raisonnable selon le type, pour respecter la déclaration.
    """
    from pyspark.sql.types import StringType, BooleanType, LongType, IntegerType, DoubleType

    defaults = {}
    for field in df.schema.fields:
        if not field.nullable:
            if isinstance(field.dataType, StringType):
                defaults[field.name] = ""
            elif isinstance(field.dataType, BooleanType):
                defaults[field.name] = False
            elif isinstance(field.dataType, (LongType, IntegerType)):
                defaults[field.name] = 0
            elif isinstance(field.dataType, DoubleType):
                defaults[field.name] = 0.0

    if defaults:
        print(f"  Valeurs par défaut appliquées sur colonnes non-nullables : {list(defaults.keys())}")
        df = df.fillna(defaults)

    return df


def export_table(spark: SparkSession, table_name: str) -> None:
    base_path = f"{HUDI_WAREHOUSE_PATH}/{table_name}"
    print(f"→ Lecture de la table Hudi '{table_name}' depuis {base_path}")

    df = (
        spark.read.format("hudi")
        .option("hoodie.schema.on.read.enable", "false")
        .option("hoodie.datasource.read.schema.use.end.instanttime", "false")
        .option("hoodie.file.group.reader.enabled", "false")
        .load(base_path)
    )

    # On retire les colonnes techniques Hudi (_hoodie_*) qui ne servent à rien pour le BI
    hoodie_cols = [c for c in df.columns if c.startswith("_hoodie_")]
    if hoodie_cols:
        df = df.drop(*hoodie_cols)

    # Le writer JDBC de Spark ne gère pas bien les types complexes (array/struct/map) :
    # on les convertit en JSON texte pour que Postgres puisse les stocker sans erreur.
    complex_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, (ArrayType, MapType, StructType))
    ]
    if complex_cols:
        print(f"  Colonnes complexes converties en JSON texte : {complex_cols}")
        for col_name in complex_cols:
            df = df.withColumn(col_name, F.to_json(F.col(col_name)))

    # Colonnes de texte brut potentiellement très volumineuses : inutiles pour
    # un dashboard BI et responsables d'un bug connu du buffer d'écriture Spark
    # (NullPointerException dans UnsafeWriter sur des chaînes très longues).
    large_text_cols = [c for c in df.columns if c in ("normalized_text",)]
    if large_text_cols:
        print(f"  Colonnes de texte volumineux exclues de l'export BI : {large_text_cols}")
        df = df.drop(*large_text_cols)

    df = fill_declared_nonnull(df)

    # PostgreSQL rejette le caractère NUL (\u0000) et certains caractères de
    # contrôle dans les colonnes texte. Des données issues de scraping web
    # peuvent en contenir, ce qui fait planter le writer JDBC de Spark avec
    # un NullPointerException obscur (bug bien documenté). On nettoie donc
    # toutes les colonnes texte avant l'écriture.
    string_cols = [
        field.name for field in df.schema.fields
        if str(field.dataType) == "StringType()"
    ]
    for col_name in string_cols:
        df = df.withColumn(
            col_name,
            F.regexp_replace(F.col(col_name), "[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", "")
        )
    if string_cols:
        print(f"  Caractères de contrôle nettoyés sur colonnes texte : {string_cols}")

    # Certaines colonnes (souvent des timestamps/dates comme crawl_timestamp
    # ou business_timestamp) restent déclarées non-nullables dans le schéma
    # Hudi même quand des valeurs null existent réellement (ex: extraction
    # de date échouée sur un cours au format PDF). fill_declared_nonnull()
    # ne couvre pas ces types-là, donc Spark plante encore en codegen au
    # moment de la collecte toPandas(). On force tout le schéma en nullable
    # juste avant, pour de bon.
    df = make_schema_nullable(spark, df)

    count = df.count()
    print(f"  {count} lignes à exporter vers Postgres (table '{table_name}')")

    # Le writer JDBC natif de Spark plante de façon obscure (bug interne)
    # sur cette table. Comme le volume est petit, on contourne le problème
    # en collectant les données sur le driver (Pandas) puis en les écrivant
    # directement via psycopg2, qui ne passe pas par le moteur JDBC de Spark.
    import psycopg2
    from psycopg2.extras import execute_values

    pdf = df.toPandas()
    pdf = pdf.where(pdf.notnull(), None)  # NaN -> None pour psycopg2

    columns = list(pdf.columns)
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
            col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
            cur.execute(f'CREATE TABLE "{table_name}" ({col_defs});')

            values = [tuple(row) for row in pdf.itertuples(index=False, name=None)]
            quoted_columns = ", ".join(f'"{c}"' for c in columns)
            insert_sql = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES %s'
            execute_values(cur, insert_sql, values, page_size=500)
        conn.commit()
    finally:
        conn.close()
    print(f"✅ Table '{table_name}' exportée vers Postgres (base '{POSTGRES_DB}').")


def run_all() -> None:
    spark = get_spark_session()

    failed_tables = []
    for table_name in TABLES:
        try:
            export_table(spark, table_name)
        except Exception as e:
            print(f"❌ Échec sur la table '{table_name}' : {e}")
            failed_tables.append(table_name)
            continue

    if failed_tables:
        print(f"\n⚠️ Étape 7 terminée avec des erreurs sur : {failed_tables}")
    else:
        print("\n✅ Étape 7 (export) terminée : 4 tables exportées vers Postgres pour Metabase.")
    spark.stop()


if __name__ == "__main__":
    run_all()