"""
Définition des tables Apache Hudi de la zone curated.
"""

from dataclasses import dataclass

HIVE_DATABASE = "university_lakehouse"
HUDI_WAREHOUSE_PATH = "s3a://curated-zone/hudi_warehouse"


@dataclass
class HudiTableConfig:
    name: str
    record_key: str
    partition_field: str
    precombine_field: str
    table_type: str = "COPY_ON_WRITE"

    @property
    def hive_table_name(self) -> str:
        return f"{HIVE_DATABASE}.{self.name}"

    @property
    def base_path(self) -> str:
        return f"{HUDI_WAREHOUSE_PATH}/{self.name}"

    def hudi_options(self) -> dict:
        return {
            "hoodie.table.name": self.name,
            "hoodie.datasource.write.table.type": self.table_type,
            "hoodie.datasource.write.recordkey.field": self.record_key,
            "hoodie.datasource.write.partitionpath.field": self.partition_field,
            "hoodie.datasource.write.precombine.field": self.precombine_field,
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.database": HIVE_DATABASE,
            "hoodie.datasource.hive_sync.table": self.name,
            "hoodie.datasource.hive_sync.partition_fields": self.partition_field,
            "hoodie.datasource.hive_sync.partition_extractor_class": (
                "org.apache.hudi.hive.MultiPartKeysValueExtractor"
            ),
            "hoodie.datasource.hive_sync.mode": "hms",
            "hoodie.datasource.hive_sync.use_jdbc": "false",
        }


FACULTY_PROFILES = HudiTableConfig("faculty_profiles", "record_id", "faculty", "crawl_timestamp")
COURSE_CATALOG = HudiTableConfig("course_catalog", "record_id", "department", "crawl_timestamp")
RESEARCH_PUBLICATIONS = HudiTableConfig("research_publications", "record_id", "publication_year", "crawl_timestamp")
UNIVERSITY_NEWS = HudiTableConfig("university_news", "record_id", "publication_year", "crawl_timestamp")

ALL_TABLES = [FACULTY_PROFILES, COURSE_CATALOG, RESEARCH_PUBLICATIONS, UNIVERSITY_NEWS]