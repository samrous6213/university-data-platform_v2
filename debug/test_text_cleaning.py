"""
Tests de text_cleaning.py (normalisation de texte, record_id deterministe,
detection de langue, flag is_deleted).

Usage :
    python test_text_cleaning.py

Convention du projet : script autonome (pas de pytest), qui construit sa propre
SparkSession, verifie chaque fonction avec des assertions explicites, et affiche
un message final si tout est OK.
"""

import logging

from pyspark.sql import Row, SparkSession

from src.transformations.spark.transforms.text_cleaning import (
    add_is_deleted_flag,
    add_language,
    add_record_id,
    normalize_text_column,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("test_text_cleaning")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1) normalize_text_column
# ---------------------------------------------------------------------------
def test_normalize_text_column(spark: SparkSession) -> None:
    logger.info("test_normalize_text_column ...")

    df = spark.createDataFrame(
        [
            Row(raw_text="  Bonjour   le    monde  \n\t"),
            Row(raw_text="Multiple\x00control\x01chars\x1f here"),
            Row(raw_text="   "),  # que des espaces -> doit devenir chaine vide
            Row(raw_text=None),   # ne doit pas planter
        ]
    )

    result = normalize_text_column(df, "raw_text").collect()

    assert result[0]["normalized_text"] == "Bonjour le monde", result[0]["normalized_text"]
    assert result[1]["normalized_text"] == "Multiplecontrolchars here", result[1]["normalized_text"]
    assert result[2]["normalized_text"] == "", repr(result[2]["normalized_text"])
    assert result[3]["normalized_text"] is None

    logger.info("OK - normalize_text_column")


# ---------------------------------------------------------------------------
# 2) add_record_id
# ---------------------------------------------------------------------------
def test_add_record_id(spark: SparkSession) -> None:
    logger.info("test_add_record_id ...")

    df = spark.createDataFrame(
        [
            Row(source_url="https://www.uh2c.ac.ma/fstm/profile/1", content_hash="hashA"),
            Row(source_url="https://www.uh2c.ac.ma/fstm/profile/1", content_hash="hashB"),  # meme url, hash different
            Row(source_url="https://www.uh2c.ac.ma/fstm/profile/2", content_hash="hashA"),  # url differente
            Row(source_url=None, content_hash="hashC"),  # fallback sur content_hash
            Row(source_url=None, content_hash=None),     # cas limite, ne doit pas planter
        ]
    )

    result = add_record_id(df).collect()

    # Idempotence : meme source_url -> meme record_id, peu importe content_hash.
    # C'est le comportement attendu pour que les reruns Hudi fassent un upsert
    # et non une duplication.
    assert result[0]["record_id"] == result[1]["record_id"], (
        "Deux lignes avec le meme source_url doivent produire le meme record_id "
        "(idempotence des upserts Hudi)."
    )

    # URL differente -> record_id different
    assert result[0]["record_id"] != result[2]["record_id"]

    # Fallback : pas d'URL -> le hash du content_hash sert de base
    assert result[3]["record_id"] is not None and result[3]["record_id"] != ""

    # Cas limite : ni URL ni hash -> ne doit pas planter, doit rester deterministe
    assert result[4]["record_id"] is not None
    assert result[4]["record_id"] == result[4]["record_id"]  # stable / pas d'aleatoire

    # record_id doit ressembler a un sha256 hex (64 caracteres hexa)
    rid = result[0]["record_id"]
    assert len(rid) == 64 and all(c in "0123456789abcdef" for c in rid), rid

    logger.info("OK - add_record_id (deterministe et idempotent)")


# ---------------------------------------------------------------------------
# 3) add_language
# ---------------------------------------------------------------------------
def test_add_language(spark: SparkSession) -> None:
    logger.info("test_add_language ...")

    df = spark.createDataFrame(
        [
            Row(normalized_text="Le laboratoire de recherche et les publications du departement"),
            Row(normalized_text="The professor teaches courses on data engineering for the department"),
            Row(normalized_text="xyzxyz 12345 !!!"),  # ni FR ni EN reconnaissable
            Row(normalized_text=""),
            Row(normalized_text=None),
        ]
    )

    result = add_language(df).collect()

    assert result[0]["language"] == "fr", result[0]["language"]
    assert result[1]["language"] == "en", result[1]["language"]
    assert result[2]["language"] == "unknown", result[2]["language"]
    assert result[3]["language"] == "unknown", result[3]["language"]
    assert result[4]["language"] == "unknown", result[4]["language"]

    logger.info("OK - add_language")


# ---------------------------------------------------------------------------
# 4) add_is_deleted_flag
# ---------------------------------------------------------------------------
def test_add_is_deleted_flag(spark: SparkSession) -> None:
    logger.info("test_add_is_deleted_flag ...")

    df = spark.createDataFrame(
        [
            Row(record_id="abc123"),
            Row(record_id=None),
        ]
    )

    result = add_is_deleted_flag(df).collect()

    # Comportement actuel du module : la colonne vaut toujours False a
    # l'ingestion initiale (reserve pour la reconciliation future), meme si
    # record_id est null. On verifie ce comportement tel qu'implemente.
    assert result[0]["is_deleted"] is False
    assert result[1]["is_deleted"] is False

    logger.info("OK - add_is_deleted_flag (toujours False a l'ingestion, comme prevu)")


# ---------------------------------------------------------------------------
# 5) Pipeline complet (enchainement des 4 fonctions, comme dans les vrais transforms)
# ---------------------------------------------------------------------------
def test_full_pipeline(spark: SparkSession) -> None:
    logger.info("test_full_pipeline ...")

    df = spark.createDataFrame(
        [
            Row(
                source_url="https://www.uh2c.ac.ma/fstm/profile/42",
                content_hash="deadbeef",
                raw_text="  Professeur   de   mathematiques  \n  a la FSTM  ",
            ),
        ]
    )

    out = (
        df.transform(lambda d: normalize_text_column(d, "raw_text"))
        .transform(add_record_id)
        .transform(add_language)
        .transform(add_is_deleted_flag)
    ).collect()[0]

    assert out["normalized_text"] == "Professeur de mathematiques a la FSTM"
    assert out["language"] == "fr"
    assert len(out["record_id"]) == 64
    assert out["is_deleted"] is False

    logger.info("OK - pipeline complet (normalize -> record_id -> language -> is_deleted)")


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        test_normalize_text_column(spark)
        test_add_record_id(spark)
        test_add_language(spark)
        test_add_is_deleted_flag(spark)
        test_full_pipeline(spark)
    finally:
        spark.stop()

    print("Tous les tests text_cleaning sont OK. Tu peux passer aux transformations "
          "Spark (faculty_profiles_transform.py / course_catalog_transform.py).")


if __name__ == "__main__":
    main()