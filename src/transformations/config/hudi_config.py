from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass(frozen=True)
class HudiTableConfig:
    table_name: str
    record_key: str = "record_id"
    pre_combine: str = "processing_timestamp"
    partition_field: str = "source_system"
    table_type: str = "COPY_ON_WRITE"
    operation: str = "upsert"
    base_path: str = ""
    hive_sync: bool = True
    hive_database: str = "default"

    def write_options(self) -> Dict[str, str]:
        opts = {
            "hoodie.table.name": self.table_name,
            "hoodie.datasource.write.table.type": self.table_type,
            "hoodie.datasource.write.recordkey.field": self.record_key,
            "hoodie.datasource.write.precombine.field": self.pre_combine,
            "hoodie.datasource.write.operation": self.operation,
            "hoodie.datasource.meta.sync.enable": str(self.hive_sync).lower(),
            "hoodie.datasource.hive_sync.enable": str(self.hive_sync).lower(),
            "hoodie.datasource.hive_sync.database": self.hive_database,
            "hoodie.datasource.hive_sync.table": self.table_name,
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.hive_sync.mode": "hms",
            "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
            "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
            "hoodie.cleaner.commits.retained": "10",
            "hoodie.keep.min.commits": "20",
            "hoodie.keep.max.commits": "30",
            "hoodie.datasource.write.schema.allow.key.field.schema.changes": "true",
        }
        if self.partition_field:
            opts["hoodie.datasource.write.partitionpath.field"] = self.partition_field
            opts["hoodie.datasource.write.hive_style_partitioning"] = "true"
            opts["hoodie.datasource.hive_sync.partition_fields"] = self.partition_field
            opts["hoodie.datasource.hive_sync.partition_extractor_class"] = (
                "org.apache.hudi.hive.MultiPartKeysValueExtractor"
            )
        return opts


FACULTY_PROFILES_HUDI = HudiTableConfig(
    table_name="faculty_profiles",
    base_path="s3a://hudi-curated/faculty_profiles",
)

COURSE_CATALOG_HUDI = HudiTableConfig(
    table_name="course_catalog",
    base_path="s3a://hudi-curated/course_catalog",
)

UNIVERSITY_NEWS_HUDI = HudiTableConfig(
    table_name="university_news",
    base_path="s3a://hudi-curated/university_news",
)

RESEARCH_PUBLICATIONS_HUDI = HudiTableConfig(
    table_name="research_publications",
    base_path="s3a://hudi-curated/research_publications",
)

DOCUMENTS_REGISTRY_HUDI = HudiTableConfig(
    table_name="documents_registry",
    base_path="s3a://hudi-curated/documents_registry",
)
