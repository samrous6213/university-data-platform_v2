"""
minio_reader.py
================

Module de lecture des donnees brutes depuis MinIO/S3 pour la couche
transformations du projet University Data Platform.

Ce module fournit les fonctions utilitaires de lecture partagees par
tous les pipelines ETL de la couche transformations :
    - ``discover_source_prefixes`` : decouvre les prefixes (repertoires)
      au sommet d'un bucket MinIO
    - ``extract_source_name`` : extrait un nom lisible a partir d'un
      prefixe (ex : ``source=openalex/`` -> ``openalex``)
    - ``read_json`` : lit les fichiers JSON depuis un prefixe dans
      un bucket MinIO via Spark
    - ``read_raw_records`` : lit et combine les donnees brutes depuis
      tous les prefixes d'un bucket, avec explosion optionnelle des
      champs de type tableau

Les donnees sont accessibles via le protocole ``s3a://``, le bucket
MinIO etant expose en tant que systeme de fichiers Hadoop compatible.

Compatibilite : Apache Spark 3.5.1 / MinIO (S3-compatible)
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.transformations.utils.logger import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# 1. Decouverte des prefixes source
# --------------------------------------------------------------------------- #


def discover_source_prefixes(spark: SparkSession, bucket: str) -> List[str]:
    """
    Decouvre les repertoires de premier niveau dans un bucket MinIO.

    Utilise l'API Hadoop FileSystem (``s3a://``) pour lister les
    entrees au sommet du bucket specifie. Seuls les repertoires
    (.isDirectory) sont retournes, avec un slash de fin pour
    faciliter la construction des chemins ulterieurs.

    Args:
        spark: SparkSession active (utilisee pour acceder au
            FileSystem Hadoop configure pour S3/MinIO).
        bucket: nom du bucket MinIO a scanner.

    Returns:
        List[str]: liste des prefixes de premier niveau
            (ex : ``["source=openalex/", "source=univh2c/"]``).
            Retourne une liste vide si le bucket est vide ou
            inaccessible.

    Raises:
        Aucune exception levee explicitement. En cas d'erreur
            d'acces au bucket, un message d'erreur est journalise
            et une liste vide est retournee.
    """
    hadoop_path_class = spark._jvm.org.apache.hadoop.fs.Path
    fs_class = spark._jvm.org.apache.hadoop.fs.FileSystem

    bucket_path = hadoop_path_class(f"s3a://{bucket}/")

    try:
        fs = fs_class.get(bucket_path.toUri(), spark._jsc.hadoopConfiguration())
        statuses = fs.listStatus(bucket_path)
    except Exception as error:
        logger.error(
            f"Impossible de lister les prefixes dans le bucket '{bucket}': {error}"
        )
        return []

    prefixes: List[str] = []

    for status in statuses:
        if status.isDirectory():
            dir_name = status.getPath().getName()
            prefixes.append(f"{dir_name}/")

    logger.info(
        f"Bucket '{bucket}': {len(prefixes)} prefixes decouverts"
    )

    return prefixes


# --------------------------------------------------------------------------- #
# 2. Extraction du nom source
# --------------------------------------------------------------------------- #


def extract_source_name(prefix: str) -> str:
    """
    Extrait un nom de source lisible a partir d'un prefixe.

    Gere les deux conventions de nommage courantes dans le projet :
        - ``source=openalex/``   -> ``openalex``
        - ``openalex/``          -> ``openalex``

    Le slash de fin et le preambule ``source=`` sont strippes
    automatiquement.

    Args:
        prefix: chemin du prefixe (ex : ``"source=openalex/"``).

    Returns:
        str: nom lisible de la source (ex : ``"openalex"``).
            Retourne ``"unknown"`` si le prefixe est vide ou
            ne contient aucun composant exploitable.
    """
    cleaned = prefix.strip("/")

    if not cleaned:
        return "unknown"

    name = cleaned.split("/")[-1]

    if name.startswith("source="):
        name = name[len("source="):]

    if not name:
        return "unknown"

    return name


# --------------------------------------------------------------------------- #
# 3. Lecture JSON depuis MinIO
# --------------------------------------------------------------------------- #


def read_json(
    spark: SparkSession,
    bucket: str,
    prefix: str,
) -> DataFrame:
    """
    Lit les fichiers JSON depuis un prefixe dans un bucket MinIO.

    Les fichiers JSON sont lus via le reader natif Spark
    (``spark.read.json``) avec l'option ``multiLine=true`` pour
    supporter les fichiers JSON formattes sur plusieurs lignes.

    Le chemin complet est construit en concatenant le protocole
    ``s3a://``, le nom du bucket et le prefixe specifie.

    Args:
        spark: SparkSession active.
        bucket: nom du bucket MinIO.
        prefix: prefixe (repertoire) dans le bucket contenant les
            fichiers JSON (ex : ``"source=openalex/"``).

    Returns:
        DataFrame: DataFrame contenant les donnees JSON lues.
            Le schema est infere par Spark a partir du contenu.

    Raises:
        Aucune exception levee explicitement. En cas d'erreur de
            lecture, un DataFrame vide ou un message d'erreur est
            journalise selon le comportement du reader Spark.
    """
    path = f"s3a://{bucket}/{prefix}"

    logger.info(f"Lecture JSON depuis : {path}")

    try:
        dataframe = (
            spark.read
            .option("multiLine", "true")
            .json(path)
        )

        logger.info(
            f"Fichiers JSON lus depuis '{path}': {dataframe.count()} lignes, "
            f"{len(dataframe.columns)} colonnes"
        )

        return dataframe

    except Exception as error:
        logger.error(
            f"Echec de la lecture JSON depuis '{path}': {error}"
        )
        raise


# --------------------------------------------------------------------------- #
# 4. Lecture combinee des enregistrements bruts
# --------------------------------------------------------------------------- #


def read_raw_records(
    spark: SparkSession,
    bucket: str,
    source_prefixes: Optional[List[str]] = None,
    array_fields: Optional[List[str]] = None,
) -> DataFrame:
    """
    Lit et combine les enregistrements bruts depuis plusieurs prefixes
    dans un bucket MinIO.

    Cette fonction est une abstraction de niveau superieure par rapport
    a ``read_json``. Elle gere automatiquement :
        1. La decouverte des prefixes (si ``source_prefixes`` est None).
        2. La lecture JSON de chaque prefixe.
        3. L'explosion optionnelle des champs de type tableau
           (``inline_outer``) si des ``array_fields`` sont specifies.
        4. L'ajout de la colonne ``_source_prefix`` a chaque DataFrame.
        5. La combinaison de tous les DataFrames en un seul.

    Cette fonction est utilisee par les pipelines ETL qui necessitent
    une lecture directe des enregistrements bruts sans transformation
    specifique au niveau du pipeline (ex : documents_registry).

    Args:
        spark: SparkSession active.
        bucket: nom du bucket MinIO contenant les donnees brutes.
        source_prefixes: liste optionnelle des prefixes a lire.
            Si None, tous les prefixes du bucket sont decouverts
            automatiquement via ``discover_source_prefixes``.
        array_fields: liste optionnelle des champs de type tableau
            a exploser via ``inline_outer``. Si None ou vide, aucune
            explosion n'est appliquee. Pour chaque DataFrame, seul le
            premier champ trouve dans ``array_fields`` est explosee.

    Returns:
        DataFrame: DataFrame combine contenant tous les enregistrements
            bruts lus depuis les prefixes specifies, avec la colonne
            additionnelle ``_source_prefix``.
    """
    if source_prefixes is None:
        source_prefixes = discover_source_prefixes(spark, bucket)

    if not source_prefixes:
        logger.warning(f"Aucun prefixe a lire dans le bucket '{bucket}'")
        empty_df = spark.createDataFrame([], "dummy STRING")
        return empty_df.filter(F.lit(False))

    if array_fields is None:
        array_fields = []

    combined_sources: List[DataFrame] = []

    for prefix in source_prefixes:

        raw_df = read_json(spark, bucket, prefix=prefix)

        if raw_df.count() == 0:
            continue

        source_name = extract_source_name(prefix)

        matched = [f for f in array_fields if f in raw_df.columns]

        if matched:
            raw_df = raw_df.selectExpr(
                f"inline_outer({matched[0]})",
                "input_file_name() as _source_file",
            )

        raw_df = raw_df.withColumn(
            "_source_prefix",
            F.lit(source_name),
        )

        combined_sources.append(raw_df)

        logger.info(
            f"Source '{source_name}': {raw_df.count()} enregistrements bruts charges"
        )

    if not combined_sources:
        logger.warning("Aucune donnee brute trouvee dans les prefixes")
        empty_df = spark.createDataFrame([], "dummy STRING")
        return empty_df.filter(F.lit(False))

    combined = combined_sources[0]

    for df in combined_sources[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    logger.info(
        f"Total des enregistrements bruts combines : {combined.count()}"
    )

    return combined
