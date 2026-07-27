# configs/spark_config.py
from .minio_config import MINIO_CONFIG

SPARK_CONFIG = {
    'app_name': 'UniversityLakehouse',
    'master': 'local[*]',
    'packages': [
        'org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0',
        'org.apache.spark:spark-sql_2.12:3.5.0'
    ],
    'config': {
        'spark.sql.extensions': 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.hudi.catalog.HoodieCatalog',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.hadoop.fs.s3a.endpoint': f"http://{MINIO_CONFIG['endpoint']}",
        'spark.hadoop.fs.s3a.access.key': MINIO_CONFIG['access_key'],
        'spark.hadoop.fs.s3a.secret.key': MINIO_CONFIG['secret_key'],
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        'spark.sql.catalog.hive': 'org.apache.spark.sql.hive.HiveCatalog',
    }
}

HUDI_CONFIG = {
    'base_path': 's3a://university-lakehouse/',
    'table_prefix': 'hudi_',
    'partition_fields': ['year', 'month', 'day'],
    'default_ts': 'updated_at'
}