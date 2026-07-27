"""
hudi_config.py
===============

Configuration centralisee pour les tables Apache Hudi du projet
University Data Platform.

Ce module regroupe UNIQUEMENT la configuration Hudi (aucune logique de
lecture/écriture Spark ici) :
    - la classe ``HudiTableConfig`` qui encapsule les parametres d'une
      table Hudi (nom, cle, precombine, partition, type, operation)
    - les instances preconfigurees pour chaque table du pipeline :
        * ``COURSE_CATALOG_HUDI``        - table course_catalog
        * ``FACULTY_PROFILES_HUDI``      - table faculty_profiles
        * ``DOCUMENTS_REGISTRY_HUDI``    - table documents_registry
        * ``RESEARCH_PUBLICATIONS_HUDI`` - table research_publications
        * ``UNIVERSITY_NEWS_HUDI``       - table university_news

Chaque instance ``HudiTableConfig`` expose :
    - une propriete ``qualified_table_name`` (database.table)
    - une methode ``build_options()`` qui retourne le dictionnaire
      d'options Hudi pret a etre injecte dans un DataFrameWriter

Ce fichier est volontairement independant de toute SparkSession : il ne fait
que declarer des constantes et construire des configurations Hudi.
La construction de la SparkSession, la lecture et l'ecriture se trouvent
dans hudi_writer.py.

Compatibilite : Apache Spark 3.5.1 / Apache Hudi 0.15.0
"""

from __future__ import annotations

from dataclasses import dataclass, field


# --------------------------------------------------------------------------- #
# 1. Classe de configuration d'une table Hudi
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class HudiTableConfig:
    """
    Configuration complete pour une seule table Apache Hudi.

    Chaque instance encapsule l'ensemble des parametres necessaires a
    l'ecriture d'une table Hudi via Spark, y compris :
        - identifiants (database, table)
        - champs fonctionnels (record key, precombine, partition)
        - type de table (Copy on Write / Merge on Read)
        - operation d'ecriture (upsert)
        - parallélisme de shuffle

    Cette classe est immutable (``frozen=True``) pour garantir que les
    configurations ne sont pas modifiees apres instanciation.

    Attributes:
        database_name: nom de la base Hudi (ex : ``university_data_platform``).
        table_name: nom de la table Hudi (ex : ``faculty_profiles``).
        record_key_field: colonne servant de cle d'enregistrement unique.
        precombine_field: colonne utilisee par Hudi pour determiner la
            version la plus recente en cas de doublon.
        partition_field: colonne de partitionnement physique.
        base_path: chemin de stockage de la table Hudi.
        table_type: type de table (``COPY_ON_WRITE`` ou ``MERGE_ON_READ``).
        write_operation: operation d'ecriture Hudi (``upsert``, ``insert``, ...).
        insert_shuffle_parallelism: parallélisme de shuffle pour les inserts.
        upsert_shuffle_parallelism: parallélisme de shuffle pour les upserts.
    """

    database_name: str
    table_name: str
    record_key_field: str
    precombine_field: str
    partition_field: str
    base_path: str
    table_type: str = field(default="COPY_ON_WRITE")
    write_operation: str = field(default="upsert")
    insert_shuffle_parallelism: str = field(default="2")
    upsert_shuffle_parallelism: str = field(default="2")
    metastore_uri: str = field(default="thrift://hive-metastore:9083")

    @property
    def qualified_table_name(self) -> str:
        """
        Retourne le nom qualifie de la table au format ``database.table``.

        Returns:
            str: nom qualifie de la table Hudi.
        """
        return f"{self.database_name}.{self.table_name}"

    def build_options(self) -> dict:
        """
        Construit le dictionnaire d'options Hudi a transmettre au
        DataFrameWriter Spark via ``.options(**hudi_options)``.

        Ce dictionnaire regroupe :
            - le nom de la table
            - le type de table (COW/MOR)
            - la cle d'enregistrement
            - le champ de precombine
            - le champ de partitionnement
            - l'operation d'ecriture (upsert)
            - le parallélisme d'insertion et d'upsert
            - le partitionnement au format Hive
            - la synchronisation Hive Metastore (HMS)

        Returns:
            dict: options Hudi pretes a l'emploi.
        """
        return {
            "hoodie.table.name": self.table_name,
            "hoodie.datasource.write.table.type": self.table_type,
            "hoodie.datasource.write.recordkey.field": self.record_key_field,
            "hoodie.datasource.write.precombine.field": self.precombine_field,
            "hoodie.datasource.write.partitionpath.field": self.partition_field,
            "hoodie.datasource.write.operation": self.write_operation,
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "hoodie.insert.shuffle.parallelism": self.insert_shuffle_parallelism,
            "hoodie.upsert.shuffle.parallelism": self.upsert_shuffle_parallelism,
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.mode": "hms",
            "hoodie.datasource.hive_sync.database": self.database_name,
            "hoodie.datasource.hive_sync.table": self.table_name,
            "hoodie.datasource.hive_sync.metastore.uris": self.metastore_uri,
            "hoodie.datasource.hive_sync.partition_fields": self.partition_field,
        }


# --------------------------------------------------------------------------- #
# 2. Configuration de la table course_catalog
# --------------------------------------------------------------------------- #

COURSE_CATALOG_HUDI: HudiTableConfig = HudiTableConfig(
    database_name="university_data_platform",
    table_name="course_catalog",
    record_key_field="record_id",
    precombine_field="crawl_timestamp",
    partition_field="source_system",
    base_path="s3a://hudi/course_catalog",
)


# --------------------------------------------------------------------------- #
# 3. Configuration de la table faculty_profiles
# --------------------------------------------------------------------------- #

FACULTY_PROFILES_HUDI: HudiTableConfig = HudiTableConfig(
    database_name="university_data_platform",
    table_name="faculty_profiles",
    record_key_field="record_id",
    precombine_field="crawl_timestamp",
    partition_field="source_system",
    base_path="s3a://hudi/faculty_profiles",
)


# --------------------------------------------------------------------------- #
# 4. Configuration de la table documents_registry
# --------------------------------------------------------------------------- #

DOCUMENTS_REGISTRY_HUDI: HudiTableConfig = HudiTableConfig(
    database_name="university_data_platform",
    table_name="documents_registry",
    record_key_field="record_id",
    precombine_field="crawl_timestamp",
    partition_field="source_system",
    base_path="s3a://hudi/documents_registry",
)


# --------------------------------------------------------------------------- #
# 5. Configuration de la table research_publications
# --------------------------------------------------------------------------- #

RESEARCH_PUBLICATIONS_HUDI: HudiTableConfig = HudiTableConfig(
    database_name="university_data_platform",
    table_name="research_publications",
    record_key_field="record_id",
    precombine_field="crawl_timestamp",
    partition_field="source_system",
    base_path="s3a://hudi/research_publications",
)


# --------------------------------------------------------------------------- #
# 6. Configuration de la table university_news
# --------------------------------------------------------------------------- #

UNIVERSITY_NEWS_HUDI: HudiTableConfig = HudiTableConfig(
    database_name="university_data_platform",
    table_name="university_news",
    record_key_field="record_id",
    precombine_field="crawl_timestamp",
    partition_field="source_system",
    base_path="s3a://hudi/university_news",
)
