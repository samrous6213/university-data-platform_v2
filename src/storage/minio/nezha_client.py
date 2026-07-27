# src/storage/minio/nezha_client.py

from minio import Minio
from minio.error import S3Error
import io
import json
import logging

logger = logging.getLogger(__name__)

# ==============================================================
# CLASSE MinIOClient (pour la compatibilité avec ton scraper)
# ==============================================================

class MinIOClient:
    """Wrapper class for MinIO operations"""
    
    def __init__(self, endpoint="localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        # Test connection
        try:
            self.client.list_buckets()
            print("✅ MinIO client connected successfully")
        except Exception as e:
            print(f"❌ Failed to connect to MinIO: {e}")
            raise
    
    def ensure_bucket(self, bucket_name: str) -> bool:
        """Create bucket if it doesn't exist"""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"📦 Created bucket: {bucket_name}")
            return True
        except S3Error as e:
            logger.error(f"Error ensuring bucket '{bucket_name}': {e}")
            return False
    
    def upload_binary(self, bucket_name: str, object_name: str, data: bytes, content_type: str = None):
        """Upload binary data to MinIO"""
        try:
            self.ensure_bucket(bucket_name)
            data_stream = io.BytesIO(data)
            self.client.put_object(
                bucket_name,
                object_name,
                data_stream,
                len(data),
                content_type=content_type
            )
            logger.debug(f"✅ Uploaded {object_name} to {bucket_name}")
        except S3Error as e:
            logger.error(f"❌ Error uploading {object_name}: {e}")
            raise
    
    def upload_json(self, bucket_name: str, object_name: str, data: dict):
        """Upload JSON data to MinIO"""
        try:
            json_str = json.dumps(data, ensure_ascii=False, indent=2)
            self.upload_binary(
                bucket_name,
                object_name,
                json_str.encode('utf-8'),
                content_type="application/json"
            )
        except Exception as e:
            logger.error(f"❌ Error uploading JSON to {object_name}: {e}")
            raise
    
    def download_binary(self, bucket_name: str, object_name: str) -> bytes:
        """Download binary data from MinIO"""
        try:
            response = self.client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error(f"❌ Error downloading {object_name}: {e}")
            raise
    
    def download_json(self, bucket_name: str, object_name: str) -> dict:
        """Download JSON data from MinIO"""
        try:
            data = self.download_binary(bucket_name, object_name)
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"❌ Error downloading JSON from {object_name}: {e}")
            raise

# ==============================================================
# FONCTIONS EXISTANTES (gardées pour compatibilité)
# ==============================================================

def get_minio_client():
    """Create and return MinIO client connection (retourne la classe MinIOClient)"""
    return MinIOClient()

def bucket_exists(bucket_name):
    """Check if bucket exists"""
    client = get_minio_client()
    return client.client.bucket_exists(bucket_name)

def create_bucket(bucket_name):
    """Create bucket if not exists"""
    client = get_minio_client()
    return client.ensure_bucket(bucket_name)