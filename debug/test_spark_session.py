"""
debug/test_spark_session.py

Objectif : verifier que la SparkSession demarre correctement avec les configs
Hudi / Hive / S3A -> MinIO, AVANT de tester quoi que ce soit d'autre.

Si ce script echoue, rien d'autre ne marchera derriere (readers, transforms, hudi_writer).
A lancer en tout premier.

HYPOTHESE (a confirmer) : config/spark_session.py expose une fonction
    get_spark_session() -> SparkSession
qui lit configs/spark_config.py en interne. Si le nom/la signature reelle
est different, adapte l'import ci-dessous -- le reste du script ne change pas.

Usage :
    python -m debug.test_spark_session
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("debug.test_spark_session")


def main() -> int:
    try:
        from src.transformations.spark.config.spark_session import get_spark_session
    except ImportError as e:
        logger.error(
            "Impossible d'importer get_spark_session() depuis "
            "src/transformations/spark/config/spark_session.py : %s", e
        )
        logger.error(
            "-> Verifie le nom exact de la fonction dans spark_session.py "
            "et adapte l'import ci-dessus."
        )
        return 1

    logger.info("Demarrage de la SparkSession...")
    try:
        spark = get_spark_session()
    except Exception:
        logger.exception("Echec au demarrage de la SparkSession")
        return 1

    logger.info("SparkSession demarree : app_id=%s", spark.sparkContext.applicationId)

    # 1) Configs critiques a verifier explicitement (celles qui plantent tout
    #    silencieusement si mal chargees : endpoint S3A, style de path, Hive uri)
    checks = {
        "spark.hadoop.fs.s3a.endpoint": None,
        "spark.hadoop.fs.s3a.access.key": None,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "hive.metastore.uris": None,
    }

    conf = spark.sparkContext.getConf()
    print("\n--- Configs Spark actives (celles qu'on surveille) ---")
    all_ok = True
    for key, expected in checks.items():
        value = conf.get(key, "<NON DEFINIE>")
        status = "OK"
        if value == "<NON DEFINIE>":
            status = "MANQUANTE"
            all_ok = False
        elif expected is not None and value != expected:
            status = f"ATTENDU={expected}"
            all_ok = False
        print(f"  {key:45s} = {value:45s} [{status}]")

    # 2) Test fonctionnel minimal : creer un DataFrame en memoire, faire une
    #    action (.count()), s'assurer que l'executeur repond vraiment.
    print("\n--- Test fonctionnel (DataFrame en memoire) ---")
    df = spark.createDataFrame([(1, "faculty_profiles"), (2, "course_catalog")], ["id", "table"])
    df.show(truncate=False)
    count = df.count()
    assert count == 2, f"Attendu 2 lignes, obtenu {count}"
    print(f"OK : {count} lignes lues en local, Spark fonctionne.")

    # 3) Test optionnel : ping MinIO via S3A (si le bucket raw-json existe deja).
    #    Ne fait pas planter le script si le bucket est vide/absent -- juste informatif.
    print("\n--- Test optionnel : acces S3A a MinIO ---")
    try:
        from configs.spark_config import RAW_JSON_BUCKET
        path = f"s3a://{RAW_JSON_BUCKET}/"
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path), hadoop_conf
        )
        exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))
        print(f"  Bucket {RAW_JSON_BUCKET} accessible via S3A : {exists}")
    except Exception as e:
        print(f"  (info) Impossible de verifier l'acces S3A ici : {e}")
        print("  Ce n'est pas bloquant si MinIO n'est pas encore lance localement.")

    spark.stop()

    if not all_ok:
        logger.warning(
            "Certaines configs sont manquantes ou differentes de l'attendu -- "
            "verifie spark_session.py avant de continuer."
        )
        return 1

    logger.info("Toutes les verifications sont OK. Tu peux passer a test_json_reader.py.")
    return 0


if __name__ == "__main__":
    sys.exit(main())