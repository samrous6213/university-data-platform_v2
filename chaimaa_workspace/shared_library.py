"""
Bibliothèque partagée pour les 7 personnes
Chaque personne importe ces fonctions
"""

from minio import Minio
from pyspark.sql import SparkSession
import hashlib
import json
from datetime import datetime
from io import BytesIO

class DataPlatformUtils:
    """Utilitaires communs pour tout le monde"""
    
    @staticmethod
    def get_minio_client():
        """Connexion MinIO pour tout le monde"""
        return Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
    
    @staticmethod
    def get_spark_session(app_name):
        """Spark session configurée pour Hudi"""
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
            .enableHiveSupport() \
            .getOrCreate()
    
    @staticmethod
    def save_to_minio(bucket, path, data, content_type='application/json'):
        """Sauvegarde standardisée dans MinIO"""
        client = DataPlatformUtils.get_minio_client()
        
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        
        json_data = json.dumps(data, indent=2, default=str)
        json_bytes = BytesIO(json_data.encode('utf-8'))
        
        client.put_object(
            bucket, path, data=json_bytes,
            length=len(json_data), content_type=content_type
        )
        return f"s3://{bucket}/{path}"
    
    @staticmethod
    def generate_record_id(source, unique_id):
        """Génère un ID unique pour chaque record"""
        return hashlib.md5(f"{source}_{unique_id}_{datetime.now()}".encode()).hexdigest()
    
    @staticmethod
    def standardize_faculty_record(raw_data, source_name):
        """Convertit n'importe quelle source au format faculty_profiles"""
        return {
            "record_id": DataPlatformUtils.generate_record_id(source_name, raw_data.get('id', '')),
            "name": raw_data.get('name', raw_data.get('display_name', 'Unknown')),
            "title": raw_data.get('title', 'Researcher'),
            "department": raw_data.get('department', raw_data.get('institution', 'Unknown')),
            "email": raw_data.get('email', ''),
            "research_interests": raw_data.get('interests', raw_data.get('topics', '')),
            "source_system": source_name,
            "source_url": raw_data.get('url', raw_data.get('source_url', '')),
            "crawl_timestamp": datetime.now().isoformat(),
            "year": datetime.now().year
        }

print("✓ Bibliothèque partagée chargée")
