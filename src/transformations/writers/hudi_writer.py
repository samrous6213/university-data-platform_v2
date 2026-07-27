"""
hudi_writer.py
===============

Module d'ecriture dans Apache Hudi pour la couche transformations
du projet University Data Platform.

Ce module fournit la fonction ``write_hudi_table``, reutilisable par
tous les pipelines ETL de la couche transformations, qui ecrit un
DataFrame Spark dans une table Apache Hudi en utilisant la
configuration fournie par une instance ``HudiTableConfig``.

L'ecriture est realisee via le format ``hudi`` de Spark avec les
options construites dynamiquement par ``HudiTableConfig.build_options()``.
Le mode d'ecriture est ``append`` : c'est Hudi lui-meme (via
``hoodie.datasource.write.operation=upsert``) qui gere la logique
d'insertion / mise a jour, pas le mode Spark.

Ce module ne cree PAS de SparkSession. Il s'attend a recevoir une
SparkSession active en parametre, conformement au principe de
separation des responsabilites du projet.

Compatibilite : Apache Spark 3.5.1 / Apache Hudi 0.15.0
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.transformations.config.hudi_config import HudiTableConfig
from src.transformations.utils.logger import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Constantes Spark / Hudi
# --------------------------------------------------------------------------- #

# Le package Hudi n'est PAS declare ici via "spark.jars.packages" : il est
# deja charge au demarrage du conteneur Spark grace a la variable
# d'environnement Docker :
#   ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 pyspark-shell"

# Mode d'ecriture Spark (DataFrameWriter.mode) associe a l'ecriture Hudi.
# "append" est le mode recommande par Hudi : c'est Hudi lui-meme qui gere
# la logique d'insertion/mise a jour, pas le mode Spark.
SPARK_WRITE_MODE: str = "append"


# --------------------------------------------------------------------------- #
# 1. Ecriture dans Apache Hudi
# --------------------------------------------------------------------------- #


def write_hudi_table(
    dataframe: DataFrame,
    config: HudiTableConfig,
) -> None:
    """
    Ecrit un DataFrame dans une table Apache Hudi.

    Utilise les options construites par ``HudiTableConfig.build_options()``
    pour configurer l'ecriture Hudi (cle d'enregistrement, precombine,
    partitionnement, operation upsert, parallélisme).

    Le mode d'ecriture Spark est ``append`` : Hudi gere lui-meme la
    logique d'upsert via l'operation configuree.

    Args:
        dataframe: DataFrame Spark contenant les donnees a ecrire
            (schema pre-transforme par le transformer du pipeline).
        config: instance ``HudiTableConfig`` contenant la configuration
            complete de la table Hudi cible (nom, cle, precombine,
            partition, chemin, type, operation).

    Raises:
        RuntimeError: si l'ecriture Hudi echoue (chemin invalide,
            schema incompatible, probleme de connectivite, ...).
    """
    hudi_options = config.build_options()

    logger.info(
        f"Ecriture de {dataframe.count()} enregistrements "
        f"dans la table Hudi '{config.qualified_table_name}' "
        f"(chemin : {config.base_path}, "
        f"operation : {hudi_options.get('hoodie.datasource.write.operation')})"
    )

    try:
        (
            dataframe.write
            .format("hudi")
            .options(**hudi_options)
            .mode(SPARK_WRITE_MODE)
            .save(config.base_path)
        )

        logger.info(
            f"Ecriture Hudi terminee avec succes dans : {config.base_path}"
        )

    except Exception as error:
        logger.exception(
            f"Echec de l'ecriture Hudi vers {config.base_path}"
        )
        raise
