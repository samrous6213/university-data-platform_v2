# Runbook — University Data Platform

Ce document explique comment démarrer, surveiller, et récupérer le pipeline en cas de panne. Destiné à toute personne opérant la plateforme, y compris en dehors de l'équipe de développement.

## 1. Démarrage complet (from scratch)

```bash
cd university-data-platform_v2

docker build -t university-spark:custom -f Dockerfile.spark .
docker compose up -d
```

Attendre ~1 minute que tous les services soient sains :
```bash
docker ps
```
Tous les conteneurs doivent afficher `Up` (et `healthy` pour ceux qui ont un healthcheck).

### Initialisation unique (seulement au tout premier démarrage)
```bash
docker exec hive-metastore /opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType postgres -initSchema
```
Vérifier :
```bash
docker exec hive-metastore /opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType postgres -info
```
Doit afficher `Metastore schema version: 3.0.0` sans erreur.

## 2. Lancer une exécution du pipeline

### Via Airflow (méthode recommandée)
1. Ouvrir `http://localhost:8081`
2. DAG `nezha_pipeline` → **Trigger DAG**
3. Suivre la progression dans la vue **Grid**

### En ligne de commande
```bash
docker exec -it airflow-scheduler airflow dags trigger nezha_pipeline
```

### Manuellement, étape par étape (sans Airflow)
```bash
# 1. Ingestion (peut être lancée en parallèle)
docker exec spark-master bash -c "cd /workspace && PYTHONPATH=/workspace python3 src/ingestion/api/crossref.py"
docker exec spark-master bash -c "cd /workspace && PYTHONPATH=/workspace python3 src/ingestion/web/usms_vf/usms.py"
docker exec spark-master bash -c "cd /workspace && PYTHONPATH=/workspace python3 src/ingestion/docs/mit_ocw_pdf_scraper.py"

# 2. Transformation + écriture Hudi
docker exec spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /workspace/src/transformations/spark/write_hudi.py

# 3. Export vers Postgres (BI)
docker exec spark-master python3 /workspace/src/transformations/spark/export_to_postgres.py

# 4. Indexation Elasticsearch (lit Postgres, indexe dans Elasticsearch)
docker exec spark-master python3 /workspace/src/search/elasticsearch/index.py

# 5. Démarrer l'API de recherche (à refaire après chaque redémarrage de spark-master)
docker exec -d spark-master python3 /workspace/src/search/elasticsearch/query.py --serve
```

## 3. Monitoring

### État du DAG Airflow
```bash
docker exec -it airflow-scheduler airflow dags list-runs -d nezha_pipeline
```

### État détaillé d'un run précis
```bash
docker exec -it airflow-scheduler airflow tasks states-for-dag-run nezha_pipeline "<RUN_ID>"
```

### Logs d'une tâche précise
```bash
docker exec airflow-scheduler bash -c "find '/opt/airflow/logs/dag_id=nezha_pipeline/run_id=<RUN_ID>/task_id=<TASK_ID>' -name '*.log' | sort | tail -1 | xargs cat"
```

### Vérifier les tables Hudi directement
```bash
docker exec -it spark-master /opt/spark/bin/spark-sql \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
```
```sql
USE university_lakehouse;
SHOW TABLES;
SELECT COUNT(*) FROM faculty_profiles;
```

### Vérifier l'API de recherche
```bash
curl http://localhost:5001/health
```

Si l'API ne répond pas (souvent après un redémarrage de `spark-master`), la relancer :
```bash
docker exec spark-master ps aux | grep query.py
# si aucun process trouvé :
docker exec -d spark-master python3 /workspace/src/search/elasticsearch/query.py --serve
```

## 4. Résilience et sécurité des ré-exécutions (rerun safety)

C'est le point testé en live par le jury (10 minutes de stress-test). Le pipeline est conçu pour supporter une ré-exécution sans corruption de données :

- **Hudi upsert** : chaque table utilise `record_id` (hash SHA-256 déterministe basé sur les champs métier) comme clé d'enregistrement. Relancer `write_hudi` sur les mêmes données brutes met à jour les lignes existantes au lieu de créer des doublons.
- **Idempotence de l'ingestion** : les scripts de scraping USMS/MIT OCW utilisent des caches (`_image_cache`, `_url_cache`) pour éviter de re-télécharger des ressources déjà collectées.
- **Retries Airflow** : chaque tâche du DAG dispose de tentatives automatiques (`retries`, `retry_delay`) en cas d'échec transitoire (réseau, ressources).
- **Reprise partielle** : en cas d'échec d'une tâche, seules les tâches en aval sont bloquées — les tâches déjà réussies (`success`) ne sont pas relancées lors d'un `Clear` ciblé dans Airflow.

### Simuler une panne / rerun pour démonstration

```bash
# Arrêter Spark en pleine exécution
docker stop spark-master

# Relancer
docker start spark-master

# Relancer uniquement la tâche échouée depuis Airflow
docker exec -it airflow-scheduler airflow tasks clear nezha_pipeline -t write_hudi -y
```
Le pipeline reprend sans dupliquer les données déjà écrites (vérifiable via `SELECT COUNT(*)` avant/après, qui doit rester stable si les données sources n'ont pas changé).

## 5. Procédures de récupération après incident

### Le scheduler Airflow ne répond plus (heartbeat perdu)
```bash
docker restart airflow-scheduler
```
Cause fréquente : charge machine élevée. Vérifier les ressources avec `docker stats --no-stream`.

### Un conteneur Postgres a planté
```bash
docker ps -a | grep postgres
docker logs <nom-du-conteneur> --tail 50
docker start <nom-du-conteneur>
```
Redémarrer ensuite Airflow dans l'ordre : Postgres → scheduler → webserver.

### Erreur `HoodieMetaSyncException` / `Version information not found in metastore`
Le schéma Hive Metastore n'est pas initialisé :
```bash
docker exec hive-metastore /opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType postgres -initSchema
```

### `ModuleNotFoundError` dans une tâche Spark
Dépendance Python manquante dans l'image `spark-master`. Vérifier/reconstruire :
```bash
docker exec spark-master python3 -c "import pandas, psycopg2, elasticsearch, requests, bs4, minio, dotenv, flask; print('OK')"
docker build -t university-spark:custom -f Dockerfile.spark .
docker compose up -d --force-recreate spark-master spark-worker
```

### L'API de recherche répond mais renvoie une erreur 500 / incompatibilité de version Elasticsearch
Le client Python `elasticsearch` installé peut être plus récent que le serveur (8.11). Vérifier :
```bash
docker exec spark-master python3 -c "import elasticsearch; print(elasticsearch.__version__)"
```
Doit afficher une version `8.x` (pas `9.x`). Si besoin, corriger la contrainte dans `Dockerfile.spark` (`"elasticsearch>=8.11,<9.0"`) et reconstruire :
```bash
docker build -t university-spark:custom -f Dockerfile.spark .
docker compose up -d --force-recreate spark-master spark-worker
docker exec -d spark-master python3 /workspace/src/search/elasticsearch/query.py --serve
```

### L'API de recherche ne répond plus après un redémarrage de `spark-master`
Normal : le process Flask (`query.py --serve`) est lancé manuellement dans le conteneur, il ne redémarre pas automatiquement. Relancer :
```bash
docker exec -d spark-master python3 /workspace/src/search/elasticsearch/query.py --serve
```

### Le dashboard Metabase a disparu après un redémarrage
Vérifier que le volume de persistance est bien monté :
```bash
docker volume ls | grep metabase
```
Si absent, voir la configuration `volumes:` du service `metabase` dans `docker-compose.yml` — doit pointer vers un volume nommé, pas vers le système de fichiers interne du conteneur.

### Docker/WSL2 ne répond plus du tout
```bash
wsl --shutdown
```
Puis relancer Docker Desktop. Si le problème est récurrent, allouer plus de ressources à WSL2 via `%UserProfile%\.wslconfig` :
```ini
[wsl2]
memory=8GB
processors=4
```

## 6. Arrêt propre de la plateforme

```bash
docker compose down
```
Les données persistent dans les volumes Docker (MinIO, Postgres, Metabase) — un `docker compose up -d` ultérieur retrouve l'état précédent sans perte.

Pour tout réinitialiser (⚠️ perte de données) :
```bash
docker compose down -v
```