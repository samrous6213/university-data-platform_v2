import sys
sys.path.insert(0, '/opt/spark/work-dir')
from src.transformations.config.spark_config import SparkConfig
from src.transformations.etl.research_publications_etl import run_research_publications_etl
from src.transformations.etl.documents_registry_etl import run_documents_registry_etl
config = SparkConfig(app_name='FixTwoETLs')
spark = config.build()
r1 = run_research_publications_etl(spark)
print(f'research_publications result: {r1}')
r2 = run_documents_registry_etl(spark)
print(f'documents_registry result: {r2}')
spark.stop()
