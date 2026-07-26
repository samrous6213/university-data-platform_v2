# University Data Platform v2 — RUNBOOK (Guide d'exploitation)

**Version :** 1.0

> Ce runbook décrit les étapes **opérationnelles** pour démarrer la plateforme, exécuter les scrapers, lancer la transformation Spark, indexer Elasticsearch, vérifier les données, surveiller les logs, récupérer en cas d'erreur et arrêter l'ensemble.

---

## 0) Prérequis

### Matériel / logiciels
- Docker Desktop (ou Docker Engine + Docker Compose)
- Accès au projet : `C:\Users\hp\university-data-platform_v2`

### Avertissement
- Les identifiants sont ceux définis dans `docker-compose.yml` (MinIO/PostgreSQL/Airflow).
- Les scripts Spark sont déclenchés via `spark-submit` (dans le conteneur spark-master).
- Le DAG Airflow exécute les ingestions depuis le conteneur `airflow-webserver`.

---

## 1) Démarrer la plateforme

Depuis la racine du projet :

```bash
docker compose up -d
```

### Vérifications
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

**13 conteneurs attendus :**
- `university-minio` — Stockage S3
- `university-postgres` — PostgreSQL Hive
- `hive-metastore` — Hive Metastore (port 9083)
- `university-hive-server` — HiveServer2 (ports 10000, 10002)
- `spark-master` — Spark Master (ports 7077, 8080)
- `spark-worker` — Spark Worker (port 8082)
- `university-elasticsearch` — Elasticsearch (ports 9200, 9300)
- `university-qdrant` — Qdrant vector DB (ports 6333, 6334)
- `university-metabase` — Metabase BI (port 3000)
- `airflow-postgres` — Airflow metadata DB (port 5433)
- `airflow-init` — Initialisation (s'exécute puis s'arrête)
- `airflow-webserver` — Airflow UI (port 8081)
- `airflow-scheduler` — Airflow scheduler

---

## 2) Accès aux services

| Service | URL | Port | Credentials |
|---|---:|---:|---|
| MinIO Console | http://localhost:9001 | 9001 | `minioadmin` / `minioadmin` |
| Spark Master UI | http://localhost:8080 | 8080 | - |
| Metabase | http://localhost:3000 | 3000 | Config. 1er lancement |
| Airflow Webserver | http://localhost:8081 | 8081 | `admin` / `admin` |
| Elasticsearch | http://localhost:9200 | 9200 | désactivé |
| Qdrant REST | http://localhost:6333 | 6333 | - |
| PostgreSQL (Hive) | localhost:5432 | 5432 | `hive` / `hive` |
| PostgreSQL (Airflow) | localhost:5433 | 5433 | `airflow` / `airflow` |

> Metabase : au premier lancement, créer un compte admin. Se connecter ensuite à Hive via JDBC (`thrift://hive-server:10000`).

---

## 3) Exécuter les scrapers individuellement

### Depuis le conteneur Airflow

```bash
docker compose exec airflow-webserver bash
```

#### 3.1 OpenAlex API (auteurs + publications)
```bash
python -m src.ingestion.api.chaimae_openalex
```

#### 3.2 UCA / FSSM / ENSA / ENCG Web (faculty + news + cours)
```bash
python -m src.ingestion.web.chaimae_uca_faculty
```

#### 3.3 IMIST PDFs (documents)
```bash
python -m src.ingestion.docs.chaimae_imist
```

### Depuis le host (Python direct, si dépendances installées)
```bash
python -m src.ingestion.api.chaimae_openalex
python -m src.ingestion.web.chaimae_uca_faculty
python -m src.ingestion.docs.chaimae_imist
```

---

## 4) Exécuter la transformation Spark

La transformation lit depuis MinIO (`raw-json`), transforme et écrit dans `hudi-curated` + indexe Elasticsearch.

### Commande (sur le host)
```powershell
docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master /opt/spark/bin/spark-submit `
  --master local[1] --driver-memory 4g `
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir `
  /opt/spark/work-dir/src/transformations/run_all_etl.py
```

### Résultat attendu
```
faculty_profiles         OK    ~180 records
course_catalog           OK     ~56 records
university_news          OK     ~15 records
research_publications    OK     ~20 records
documents_registry       OK    ~175 records
```

### Écritures
- Hudi : `s3a://hudi-curated/{table_name}/`
- Elasticsearch : index `faculty_profiles`, `course_catalog`, `university_news`, `research_publications`, `documents_registry`
- Hive Metastore : tables synchronisées automatiquement

---

## 5) Vérifier les données

### 5.1 MinIO (buckets + contenu)
1. Ouvrir : http://localhost:9001
2. Identifiants : `minioadmin` / `minioadmin`
3. Vérifier les buckets : `raw-json`, `raw-web-html`, `raw-images`, `raw-documents`, `raw-logs`, `hudi-curated`
4. Naviguer dans `raw-json/source=.../` pour vérifier les fichiers récents

### 5.2 Elasticsearch
```bash
# Statut cluster
curl -s http://localhost:9200

# Lister les indexes
curl -s http://localhost:9200/_cat/indices?v

# Compter les documents
curl -s http://localhost:9200/faculty_profiles/_count?pretty

# Recherche full-text
curl -s 'http://localhost:9200/faculty_profiles/_search?q=physique&pretty'
```

### 5.3 Hive / PostgreSQL
```bash
docker exec -it university-postgres psql -U hive -d metastore
```

Ou via HiveServer2 (beeline) :
```bash
docker exec -it university-hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
```

### 5.4 Vérifier l'état du DAG Airflow
1. Ouvrir : http://localhost:8081
2. Identifiants : `admin` / `admin`
3. Cliquer sur le DAG `chaimae_pipeline`
4. Vérifier que toutes les tâches sont en **vert** (succès)
5. Consulter les logs : cliquer sur une tâche → onglet "Log"

---

## 6) Lancer le pipeline complet (Airflow)

### Depuis l'UI Airflow
1. Ouvrir : http://localhost:8081
2. Cliquer sur le DAG `chaimae_pipeline`
3. Cliquer sur ▶ **"Trigger DAG"**

### Depuis le CLI
```bash
docker compose exec airflow-webserver airflow dags trigger chaimae_pipeline
```

### Suivi
```bash
docker compose exec airflow-webserver airflow dags list-runs -d chaimae_pipeline
```

---

## 7) Monitoring & logs

### 7.1 Logs Docker
```bash
docker compose logs -f --tail=200 airflow-webserver
docker compose logs -f --tail=100 spark-master
docker compose logs -f --tail=100 university-elasticsearch
docker compose logs -f --tail=100 hive-metastore
```

### 7.2 Airflow UI
- DAG `chaimae_pipeline` → **Graph** : vue des dépendances
- **Tree View** : historique des exécutions
- Chaque tâche → **Log** : sortie détaillée

### 7.3 Spark UI
- http://localhost:8080 : voir les workers, les jobs actifs/terminés

---

## 8) Recovery (redémarrage / reprise)

### 8.1 Redémarrer un service en échec
```bash
docker compose restart <service>
# Exemple:
docker compose restart elasticsearch
docker compose restart spark-master
```

### 8.2 Réexécuter un scraper
```bash
docker compose exec airflow-webserver python -m src.ingestion.api.chaimae_openalex
```

### 8.3 Réexécuter la transformation + indexation
```bash
docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master /opt/spark/bin/spark-submit \
  --master local[1] --driver-memory 4g \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
  /opt/spark/work-dir/src/transformations/run_all_etl.py
```

### 8.4 Réinitialiser complètement les données
```bash
# Arrêter et supprimer volumes
docker compose down -v

# Redémarrer
docker compose up -d

# Réexécuter scrapers + ETL
```

---

## 9) Problèmes courants & solutions rapides

### 9.1 Spark ne peut pas lire MinIO (S3A)
**Symptômes :** `java.nio.file.NoSuchFileException: s3a://raw-json/...`
**Actions :**
- Vérifier que MinIO tourne : `docker ps | grep minio`
- Vérifier les credentials S3A dans `spark_config.py`
- Tester l'accès : `curl http://localhost:9000/minio/health/live`

### 9.2 Hudi duplicate key exception
**Symptômes :** `HoodieDuplicateKeyException`
**Solution :** déduplication automatique dans `hudi_writer.py` (dropDuplicates). Si persiste, vider la table Hudi :
```bash
# Supprimer le dossier dans MinIO (via console)
```

### 9.3 Elasticsearch index non créé / zéro documents
**Actions :**
- Vérifier que le job d'indexation a terminé sans erreur
- Vérifier le count : `curl http://localhost:9200/_cat/indices?v`
- Vérifier les logs Spark pour `write_to_elasticsearch`
- Relancer l'ETL si nécessaire

### 9.4 OOM (Out Of Memory) dans Spark
**Symptômes :** `java.lang.OutOfMemoryError`
**Solutions :**
- Augmenter `--driver-memory` (ex: 4g → 8g)
- Augmenter `spark.sql.shuffle.partitions` (ex: 4 → 8)
- Vérifier le cache LRU dans `minio_reader.py`

### 9.5 Hive Metastore injoignable
**Actions :**
```bash
docker compose logs hive-metastore
docker compose logs university-postgres
```
Vérifier que postgres est healthy : `docker compose ps postgres`

### 9.6 IMIST crawler bloqué
**Symptômes :** le crawler tourne indéfiniment
**Solution :** le checkpoint file `imist_crawl_checkpoint.txt` permet la reprise. Supprimer le fichier pour reprendre du début, ou vérifier son contenu.

### 9.7 Timeout BFS du scraper UCA
**Symptômes :** tâche `uca_to_minio` prend > 30 min
**Solution :** un timeout BFS de 120s est intégré dans `chaimae_uca_faculty.py`. Si nécessaire, réduire `MAX_DURATION` dans le code.

---

## 10) Problèmes de schéma Hudi

Les transformations Hudi peuvent échouer si le schéma change entre exécutions. Solutions :
- `hoodie.schema.allow.key.field.schema.changes=true` (déjà configuré)
- `hoodie.merge.allow.duplicate.on.inserts=true`
- Si conflit persistant : supprimer la partition concernée dans MinIO et relancer

---

## 11) Arrêter la plateforme

### Arrêt simple (sans perte de données)
```bash
docker compose down
```

### Arrêt + suppression des volumes (perte des données)
```bash
docker compose down -v
```

---

## 12) Référence : DAG Airflow

### DAG : `chaimae_pipeline`
- Fichier : `dags/chaimae_pipeline.py`
- Schedule : `@daily`
- Retries : 3 (intervalle 2 min)
- Timeout ETL : 30 min

### Flux
```
                 ┌─────────────────────┐
                 │ openalex_to_minio   │
                 └──────────┬──────────┘
                            │
                 ┌──────────▼──────────┐
                 │    uca_to_minio     │
                 └──────────┬──────────┘
                            │
                 ┌──────────▼────────────┐
                 │ imist_pdfs_to_minio   │
                 └──────────┬────────────┘
                            │
                 ┌──────────▼──────────────────────┐
                 │ spark_etl_to_elasticsearch       │
                 └─────────────────────────────────┘
```

### Correctifs appliqués
1. **Timeout BFS** — `max_duration=120s` sur le crawler web
2. **Dédoublonnage** — `.dropDuplicates(["record_id"])` avant écriture ES et Hudi
3. **Cache Spark** — `df.cache().count()` pour éviter re-lecture S3
4. **Shuffle partitions** — réduites de 800 à 4 (mode local)
5. **Évolution de schéma Hudi** — `schema.allow.key.field.schema.changes=true`
6. **Capture d'erreurs Docker** — `exec_run(stderr=True)` dans Airflow
7. **Socket Docker** — monté dans Airflow pour contrôler les conteneurs
