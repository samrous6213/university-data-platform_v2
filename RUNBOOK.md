# University Data Platform — RUNBOOK (Guide d’exploitation) 

**Version :** 1.0

> Ce runbook décrit les étapes **opérationnelles** pour démarrer la plateforme, exécuter les scrapers, lancer la transformation Spark, indexer Elasticsearch, vérifier les données, surveiller les logs, récupérer en cas d’erreur et arrêter l’ensemble.

---

## 0) Prérequis 

### Matériel / logiciels
- Docker Desktop (ou Docker Engine)
- Docker Compose
- Accès au projet : `/Users/mac/university-data-platform_v2`

### Avertissement (configuration)
- Les identifiants sont ceux définis dans `docker-compose.yml` (MinIO/PostgreSQL/Airflow).
- Les scripts Spark sont déclenchés via `spark-submit` (dans le conteneur Airflow).

---

## 1) Démarrer la plateforme ▶

Depuis la racine du projet :

```bash
docker compose up -d
```

### Vérifications
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

Attendus (conteneurs) :
- `university-minio`
- `university-postgres`
- `hive-metastore`
- `spark-master` + `spark-worker`
- `university-elasticsearch`
- `university-metabase`
- `airflow-postgres`, `airflow-webserver`, `airflow-scheduler`

---

## 2) Accès aux services 

| Service | URL | Port | Credentials |
|---|---:|---:|---|
| MinIO (Console) | http://localhost:9001 | 9001 | `minioadmin` / `minioadmin` |
| Metabase | http://localhost:3000 | 3000 |  |
| Airflow (Web) | http://localhost:8081 | 8081 | `admin` / `admin` |
| Elasticsearch | http://localhost:9200 | 9200 | désactivé (sécurité désactivée dans compose) |
| PostgreSQL (Metastore) | localhost:5435 | 5435 | `hive` / `hive` |

> Les identifiants Metabase sont configurés lors du premier lancement de l'application.

---

## 3) Exécuter les scrapers individuellement 

⚠️ **Important :** Toutes les commandes suivantes doivent être exécutées **depuis le conteneur Airflow**. Utilisez d'abord :

```bash
docker compose exec airflow-webserver bash
```

Les scrapers écrivent dans **MinIO** via `src/storage/minio/sara_client.py`.

### Préparer l’environnement Airflow (recommandé)
Les scripts Spark/ingestion sont exécutés dans le conteneur Airflow (`apache/airflow:2.10.0-python3.11`) via la convention des autres composants.

Commande pratique :

```bash
docker compose exec airflow-webserver bash
```

Ensuite, exécuter depuis `/opt/airflow` (comme dans le DAG).

> Si votre conteneur n’a pas l’image exactement, utilisez le conteneur Airflow qui a accès aux sources (`src/`, `dags/`).

### 3.1 Crossref (publications)
```bash
python -m src.ingestion.api.crossref
```

### 3.2 IMIST / Toubkal (thèses, PDFs)
```bash
python -m src.ingestion.docs.toubkal
```

### 3.3 UM5 Web (faculty + news + médias)
```bash
python -m src.ingestion.web.um5
```

### 3.4 Lancer le pipeline complet (via Airflow)

Depuis l'UI Airflow :
1. Ouvrir http://localhost:8081
2. Cliquer sur le DAG `sara_university_pipeline`
3. Cliquer sur ▶ **"Trigger DAG"**

**OU en ligne de commande :**
```bash
docker compose exec airflow-webserver airflow dags trigger sara_university_pipeline
```

---

## 4) Exécuter la transformation Spark 

La transformation lit des données depuis MinIO et écrit dans `curated` + crée les tables Hive.

### Commande (script orchestrateur)
Dans le conteneur Airflow :

```bash
python -m src.processing.spark_transform
```

> Ce script déclenche `spark-submit` avec les paramètres MinIO (S3A).

### Attendus
- Lecture depuis `s3a://curated/faculty_profiles` et `s3a://curated/university_news` (ou les zones alimentées en amont)
- Écriture Hudi dans :
  - `s3a://curated/faculty_profiles_hudi`
  - `s3a://curated/university_news_hudi`
- Création/refresh de tables Hive :
  - `faculty_profiles`
  - `university_news`

---

## 5) Indexer dans Elasticsearch 

L’indexation prépare des documents puis les envoie dans l’index **`university_data`**.

### Commande
Dans le conteneur Airflow :

```bash
python -m src.processing.index_to_elasticsearch
```

> Ce script déclenche `spark-submit` avec `src/processing/es_indexer_script.py`.

### Vérification rapide côté Elasticsearch
```bash
curl -s http://localhost:9200/university_data/_count?pretty
```

---

## 6) Vérifier les données (étapes étape-par-étape) 

### 6.1 Vérifier MinIO (buckets + contenu)
1. Ouvrir : http://localhost:9001
2. Vérifier les buckets :
   - `raw-json`, `raw-documents`, `raw-web-html`, `raw-images`, `raw-logs`, `curated`

**Objectif opérationnel :** confirmer que les scrapers ont écrit des objets récents.

### 6.2 Vérifier PostgreSQL / Hive Metastore
- PostgreSQL exposé : `localhost:5435`
- Credentials : `hive` / `hive`

En pratique, vérifiez surtout les tables Hive via Spark SQL :

```sql
SHOW TABLES;
SELECT COUNT(*) FROM faculty_profiles;
SELECT COUNT(*) FROM university_news;
```

(à exécuter dans un contexte Spark si disponible)

### 6.3 Vérifier Elasticsearch
- Attendu : environ 606 documents indexés

```bash
curl -s http://localhost:9200/university_data/_count?pretty
```

- Vérifier un sample :

```bash
curl -s 'http://localhost:9200/university_data/_search?size=3' -H 'Content-Type: application/json' | python -m json.tool
```

### 6.4 Vérifier l'état du DAG Airflow

1. Ouvrir http://localhost:8081
2. Vérifier que toutes les tâches sont en **vert** ✅
3. Vérifier les logs : cliquer sur une tâche → onglet "Logs"
4. Vérifier l'historique : les DAG runs récents doivent être en succès

---

## 7) Monitoring & logs 

### 7.1 Logs Docker

```bash
docker compose logs -f --tail=200 airflow-webserver
```

Autres services (à adapter) :
- `minio`
- `spark-worker`
- `airflow-scheduler`
- `university-elasticsearch`

### 7.2 Airflow UI
1. Ouvrir : http://localhost:8081
2. Aller sur DAG : `sara_university_pipeline`
3. Vérifier l’historique : tâches `scrape_*` → `transform_spark` → `index_elasticsearch`

---

## 8) Recovery (redémarrage / reprise) 

### 8.1 Redémarrer un service en échec
Exemple : redémarrer Elasticsearch

```bash
docker compose restart elasticsearch
```

### 8.2 Réexécuter un scraper
- Relancer la commande correspondante (Crossref/IMIST/UM5)

### 8.3 Réexécuter transformation + indexation
Procéder dans cet ordre :
1. Transformation Spark :
   ```bash
   python -m src.processing.spark_transform
   ```
2. Indexation Elasticsearch :
   ```bash
   python -m src.processing.index_to_elasticsearch
   ```

---

## 9) Problèmes courants & solutions rapides 

### 9.1 MinIO inaccessible depuis Spark
Symptômes : transformation échoue sur S3A (endpoint/credentials).

Actions :
- Vérifier que le conteneur MinIO tourne :
  ```bash
  docker ps | grep minio
  ```
- Vérifier endpoint utilisé par Spark : `http://minio:9000`

### 9.2 Hive / Metastore DB non joignable
- Vérifier la santé du conteneur Postgres :
  ```bash
  docker ps | grep university-postgres
  ```
- Vérifier `airflow-postgres` si lié aux métadonnées Airflow.

### 9.3 Elasticsearch index non créé / zéro documents
- Vérifier que le job d’indexation a terminé (Airflow)
- Vérifier le count :
  ```bash
  curl -s http://localhost:9200/university_data/_count?pretty
  ```
- Si besoin : relancer `index_to_elasticsearch`.

---

## 10) Arrêter la plateforme 

Arrêt des conteneurs (sans supprimer volumes) :

```bash
docker compose down
```

Arrêt + suppression des conteneurs/ réseaux (sans effacer volumes par défaut) :
- Utilisez `docker compose down -v` **uniquement si vous acceptez de perdre les données**.

---

## 11) Preuves de test 🧪

Toutes les captures d'écran sont disponibles dans `docs/screenshots/`. Elles montrent :
- ✅ UI Airflow avec toutes les tâches en vert
- ✅ Logs des tâches
- ✅ Données dans MinIO
- ✅ Documents indexés dans Elasticsearch (606)
- ✅ Dashboard Metabase (8 KPIs)

---

## Annexe — Référence : DAG Airflow

DAG : `sara_university_pipeline` (`dags/sara_pipeline.py`)

Chaîne :
1. `scrape_um5`
2. `scrape_toubkal`
3. `scrape_crossref`
4. `transform_spark`
5. `index_elasticsearch`

