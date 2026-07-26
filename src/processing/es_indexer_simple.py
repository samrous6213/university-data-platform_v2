"""
Indexation Elasticsearch - Version LÉGÈRE (sans PySpark)
"""

import requests
import json
from minio import Minio
import sys
import time

print("="*60)
print("🚀 INDEXATION ELASTICSEARCH (SANS SPARK)")
print("="*60)

# ===== CONNEXION MINIO =====
try:
    minio_client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    print("✅ Connexion MinIO OK")
except Exception as e:
    print(f"❌ Erreur MinIO: {e}")
    sys.exit(1)

# ===== CONNEXION ELASTICSEARCH =====
try:
    # Vérifier qu'Elasticsearch est accessible
    r = requests.get("http://university-elasticsearch:9200", timeout=60)
    if r.status_code == 200:
        print("✅ Connexion Elasticsearch OK")
    else:
        print(f"⚠️ Elasticsearch répond mais code: {r.status_code}")
except Exception as e:
    print(f"❌ Elasticsearch inaccessible: {e}")
    print("   Vérifie que le conteneur est démarré: docker-compose up -d elasticsearch")
    sys.exit(1)

elastic_url = "http://university-elasticsearch:9200/university_data/_doc/"
headers = {"Content-Type": "application/json"}

# ===== LIRE LES FICHIERS JSON DEPUIS MINIO =====
print("\n📥 Lecture des fichiers depuis MinIO...")

bucket = "raw-json"
success = 0
errors = 0
total = 0

try:
    objects = minio_client.list_objects(bucket, recursive=True)
    json_files = [obj.object_name for obj in objects if obj.object_name.endswith('.json')]
    print(f"✅ {len(json_files)} fichiers JSON trouvés")

    # Limiter à 50 fichiers pour la vitesse
    for file_path in json_files[:5]:
        try:
            response = minio_client.get_object(bucket, file_path)
            data = json.loads(response.read())
            response.close()

            # Si c'est une liste
            if isinstance(data, list):
                for doc in data:
                    doc_id = doc.get('record_id', f"doc_{total}")
                    if 'record_id' in doc:
                        del doc['record_id']
                    r = requests.post(f"{elastic_url}{doc_id}", headers=headers,
                                    data=json.dumps(doc), timeout=60)
                    if r.status_code in [200, 201]:
                        success += 1
                    else:
                        errors += 1
                    total += 1
            else:
                # Document unique
                doc_id = data.get('record_id', f"doc_{total}")
                if 'record_id' in data:
                    del data['record_id']
                r = requests.post(f"{elastic_url}{doc_id}", headers=headers,
                                data=json.dumps(data), timeout=10)
                if r.status_code in [200, 201]:
                    success += 1
                else:
                    errors += 1
                total += 1

            if total % 10 == 0:
                print(f"✅ {total} documents traités...")

        except Exception as e:
            errors += 1
            print(f"⚠️ Erreur sur {file_path}: {e}")

except Exception as e:
    print(f"❌ Erreur: {e}")

print(f"\n✅ Indexation terminée !")
print(f"   📄 Total: {total}")
print(f"   ✅ Succès: {success}")
print(f"   ❌ Erreurs: {errors}")

if success > 0:
    print("\n📊 Vérifier sur: http://localhost:9200/university_data/_search?size=5")