CREATE EXTERNAL TABLE course_catalog_hive
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3a://hudi-curated/course_catalog'
TBLPROPERTIES (
  'table_type' = 'HUDI',
  'hoodie.table.name' = 'course_catalog'
);
