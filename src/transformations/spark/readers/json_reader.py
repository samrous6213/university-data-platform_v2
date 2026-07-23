"""
Lecture de la zone raw-json pour les 3 sources d'ingestion.

FIX 1 (raw_object_path manquant) : le JSON stocke par generic_crawler.py ne
contient PAS son propre chemin (seulement html_object_path, vers son jumeau).
On utilise input_file_name() pour recuperer le chemin S3A reel du fichier lu
-> c'est litteralement l'objet raw, pas besoin qu'il se reference lui-meme.

FIX 2 (COLUMN_ALREADY_EXISTS sur OpenAlex) : les works OpenAlex contiennent
`abstract_inverted_index`, un dict dynamique {mot: [positions]} ou chaque mot
du resume devient une cle JSON. Spark etant insensible a la casse par defaut,
deux mots qui ne different que par la casse (frequent avec des notations
mathematiques comme "$h^{0}$") provoquent une collision de nom de colonne des
que Spark tente d'inferer un schema sur cette structure. On fournit donc un
schema EXPLICITE qui ne garde que les champs utiles -> Spark ignore alors
silencieusement tout champ non declare, sans jamais essayer de l'inferer.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from configs.spark_config import RAW_JSON_BUCKET


def _base_path(prefix: str = "") -> str:
    return f"s3a://{RAW_JSON_BUCKET}/{prefix}"


# ── Schema explicite OpenAlex (voir FIX 2 ci-dessus) ─────────────────────
_OPENALEX_METADATA_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("institution", StringType()),
        StructField("ingestion_id", StringType()),
        StructField("connector_version", StringType()),
        StructField("ingested_at", StringType()),
        StructField("url_source", StringType()),
        StructField("http_status", IntegerType()),
        StructField("record_count", IntegerType()),
        StructField("content_hash", StringType()),
        StructField("raw_object_path", StringType()),
    ]
)

_OPENALEX_WORK_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("doi", StringType()),
        StructField("title", StringType()),
        StructField("publication_year", IntegerType()),
        StructField("cited_by_count", IntegerType()),
        StructField("type", StringType()),
        # Volontairement AUCUN champ pour abstract_inverted_index : cf. FIX 2.
        # Si un jour ce champ devient necessaire, le typer explicitement en
        # MapType(StringType(), ArrayType(IntegerType())) et JAMAIS le laisser
        # a l'inference automatique.
    ]
)

_OPENALEX_FILE_SCHEMA = StructType(
    [
        StructField("metadata", _OPENALEX_METADATA_SCHEMA),
        StructField("data", ArrayType(_OPENALEX_WORK_SCHEMA)),
    ]
)


def read_web_crawler_json(spark: SparkSession, entity_type: str) -> DataFrame:
    """
    Lit les pages JSON produites par generic_crawler.py pour un entity_type donne
    ("faculty_profiles" ou "course_catalog"), pour toutes les ecoles (source=*).
    """
    path = _base_path(f"source=*/entity={entity_type}/*/*/*/*.json")

    df = (
        spark.read.option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path)
    )

    if "_corrupt_record" in df.columns:
        n_corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count()
        if n_corrupt:
            import logging

            logging.getLogger(__name__).warning(
                "%s fichier(s) JSON corrompu(s) ignore(s) pour entity_type=%s",
                n_corrupt, entity_type,
            )
        df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    return (
        df.withColumn("record_id", F.sha2(F.col("source_url"), 256))
        .withColumn("crawl_timestamp", F.to_timestamp("extraction_timestamp"))
        .withColumn("raw_object_path", F.input_file_name())  # FIX 1
        .withColumnRenamed("content_checksum", "content_hash")
        .withColumn("is_deleted", F.lit(False))
    )


def read_openalex_json(spark: SparkSession) -> DataFrame:
    """
    Lit les fichiers OpenAlex avec un schema explicite (FIX 2) et retourne
    UNE ligne par work, avec les metadonnees d'ingestion aplaties. Utilise
    pour enrichir faculty_profiles (publication_count par institution).
    """
    path = _base_path("source=openalex/entity=works_*/*/*/*/*.json")

    df = (
        spark.read.schema(_OPENALEX_FILE_SCHEMA)
        .option("multiLine", "true")
        .json(path)
    )

    exploded = df.select(
        F.col("metadata.institution").alias("institution_key"),
        F.col("metadata.url_source").alias("source_url"),
        F.col("metadata.ingested_at").alias("ingested_at"),
        F.col("metadata.content_hash").alias("content_hash"),
        F.col("metadata.raw_object_path").alias("raw_object_path"),
        F.explode_outer("data").alias("work"),
    )

    return exploded.withColumn("crawl_timestamp", F.to_timestamp("ingested_at"))


def read_datagov_json(spark: SparkSession) -> DataFrame:
    """
    Lit les fichiers package_metadata.json (CKAN) produits par fahd_datagov.py.
    Utilise en jointure de reference, pas comme table Hudi a part entiere.

    ATTENTION : les packages CKAN peuvent eux aussi contenir des structures
    dynamiques (ex: 'extras' en liste de {key, value} arbitraires). Si un
    COLUMN_ALREADY_EXISTS apparait ici aussi, appliquer le meme FIX 2 :
    schema explicite plutot que inferSchema.
    """
    path = _base_path("source=data_gov_ma/entity=*/*/*/*/package_metadata.json")
    return (
        spark.read.option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(path)
        .withColumn("raw_object_path", F.input_file_name())
    )