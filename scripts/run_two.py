import sys
sys.path.insert(0, '/opt/spark/work-dir')
from src.transformations.config.spark_config import SparkConfig
from src.transformations.etl.documents_registry_etl import run_documents_registry_etl
config = SparkConfig(app_name='DocsRegistryETL')
spark = config.build()
r2 = run_documents_registry_etl(spark)
print(f'documents_registry result: {r2}')
spark.stop()
