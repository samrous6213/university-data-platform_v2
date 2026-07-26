"""
Script Spark minimal - lit les données JSON de MinIO et les sauvegarde en Parquet
"""

from pyspark.sql import SparkSession
import sys
import os

print("="*60)
print("🚀 TRANSFORMATION SPARK MINIMALE")
print("="*60)

# ===== SPARK SESSION =====
spark = SparkSession.builder \
    .appName("UniversityTransform") \
    .master("local[2]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("✅ Spark session créée")

try:
    # ===== CHARGEMENT DES DONNÉES =====
    print("\n📥 Chargement des données depuis MinIO...")
    
    # Essayer de charger le JSON consolidé
    try:
        df = spark.read.json("s3a://raw-json/source=*/year=*/month=*/day=*/*.json")
        print(f"✅ Données chargées: {df.count()} lignes")
        
        if df.count() > 0:
            # Sauvegarder en Parquet
            output_path = "s3a://curated/all_data_parquet/"
            df.write.mode("overwrite").parquet(output_path)
            print(f"✅ Données sauvegardées en Parquet: {output_path}")
        else:
            print("ℹ️ Aucune donnée trouvée dans raw-json")
            
    except Exception as e:
        print(f"ℹ️ Aucune donnée trouvée ou erreur: {e}")
        
    print("\n✅ Transformation terminée !")

except Exception as e:
    print(f"❌ Erreur: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    spark.stop()