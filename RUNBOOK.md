# University Data Platform — RUNBOOK (Guide d’exploitation) 

**Version :** 1.0

> Ce runbook décrit les étapes **opérationnelles** pour démarrer la plateforme, exécuter les scrapers, lancer la validation Spark/Hudi/Hive, vérifier Elasticsearch, vérifier Metabase, surveiller les logs, récupérer en cas d’erreur et arrêter l’ensemble.

---

## 0) Prérequis 

### Matériel / logiciels
- Docker Desktop (ou Docker Engine)
- Docker Compose
- Accès au projet : `C:\Users\User\Documents\university-data-platform_v2`

### Avertissement (configuration)
- Les identifiants sont ceux définis dans `docker-compose.yml` (MinIO/PostgreSQL/Airflow).
- Les scripts Spark sont déclenchés via `spark-submit` dans le conteneur Spark.
- Les tâches Airflow exécutent des commandes Docker via `BashOperator`.

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
- `spark-master`
- `university-elasticsearch`
- `university-metabase`
- `airflow-postgres`, `airflow-webserver`, `airflow-scheduler`
- `safaa-hive-mysql`, `safaa-hive-metastore`

---

## 2) Accès aux services 

| Service | URL | Port | Credentials |
|---|---:|---:|---|
| MinIO (Console) | http://localhost:9001 | 9001 | `minioadmin` / `minioadmin` |
| Metabase | http://localhost:3000 | 3000 |  |
| Airflow (Web) | http://localhost:8081 | 8081 | `admin` / `admin` |
| Elasticsearch | http://localhost:9200 | 9200 | désactivé (sécurité désactivée dans compose) |
| PostgreSQL | localhost:5432 | 5432 | `hive` / `hive` |

> Les identifiants Metabase sont configurés lors du premier lancement de l'application.

---

## 3) Exécuter les scrapers individuellement 

⚠️ **Important :** Toutes les commandes suivantes peuvent être exécutées **depuis le conteneur Airflow**.

Les scrapers écrivent dans **MinIO** via `src/storage/minio/safaa_client.py`.

### Préparer l’environnement Airflow (recommandé)

Commande pratique :

```bash
docker exec airflow-webserver bash
```

Ensuite, exécuter depuis `/opt/airflow` comme dans le DAG.

> Utilisez le conteneur Airflow qui a accès aux sources `src/` et `dags/`.

### 3.1 ORCID API (publications)
```bash
python -m src.ingestion.api.safaa_orcid
```

### 3.2 Khan Academy (source documentaire)
```bash
python -m src.ingestion.docs.safaa_khan_academy
```

### 3.3 UIZ Web (faculty + news + médias)
```bash
python -m src.ingestion.web.safaa_uiz
```

### 3.4 Lancer le pipeline complet (via Airflow)

Depuis l'UI Airflow :
1. Ouvrir http://localhost:8081
2. Cliquer sur le DAG `safaa_end_to_end_pipeline`
3. Cliquer sur ▶ **"Trigger DAG"**

**OU en ligne de commande :**
```bash
docker exec airflow-webserver airflow dags trigger safaa_end_to_end_pipeline
```

---

## 4) Exécuter la transformation Spark 

La transformation produit les données curated et les tables Hudi.

### Commandes Spark principales

Dans le conteneur Spark :

```bash
/opt/spark/bin/spark-submit /opt/spark/work-dir/safaa_transform_faculty_profiles.py
```

```bash
/opt/spark/bin/spark-submit /opt/spark/work-dir/safaa_transform_university_news.py
```

```bash
/opt/spark/bin/spark-submit /opt/spark/work-dir/safaa_transform_research_publications.py
```

### Attendus
- Écriture curated dans :
  - `/opt/spark/work-dir/data/curated/safaa/faculty_profiles`
  - `/opt/spark/work-dir/data/curated/safaa/university_news`
  - `/opt/spark/work-dir/data/curated/safaa/research_publications`
- Écriture Hudi dans :
  - `/opt/spark/work-dir/data/hudi/safaa/faculty_profiles`
  - `/opt/spark/work-dir/data/hudi/safaa/university_news`
  - `/opt/spark/work-dir/data/hudi/safaa/research_publications`
- Création/refresh des tables Hive via :
  - `safaa_final_register_hive.py`

---

## 5) Indexer dans Elasticsearch 

L’indexation prépare des documents puis les envoie dans les indices Elasticsearch.

### Commande
Dans le conteneur Spark :

```bash
/opt/spark/bin/spark-submit /opt/spark/work-dir/safaa_index_elasticsearch.py
```

### Vérification rapide côté Elasticsearch
```bash
curl -s http://localhost:9200/safaa_faculty_profiles/_count?pretty
curl -s http://localhost:9200/safaa_university_news/_count?pretty
curl -s http://localhost:9200/safaa_research_publications/_count?pretty
```

---

## 6) Vérifier les données (étapes étape-par-étape) 

### 6.1 Vérifier MinIO (buckets + contenu)
1. Ouvrir : http://localhost:9001
2. Vérifier les buckets :
   - `raw-json`
   - `raw-documents`
   - `raw-web-html`
   - `raw-images`
   - `raw-logs`
   - `curated`

**Objectif opérationnel :** confirmer que les scrapers ont écrit des objets récents.

### 6.2 Vérifier PostgreSQL / Hive Metastore
- PostgreSQL exposé : `localhost:5432`
- Credentials : `hive` / `hive`

En pratique, vérifiez surtout les tables utilisées par Metabase dans PostgreSQL :

```sql
SELECT COUNT(*) FROM safaa_dashboard.faculty_profiles;
SELECT COUNT(*) FROM safaa_dashboard.university_news;
SELECT COUNT(*) FROM safaa_dashboard.research_publications;
```

### 6.3 Vérifier Elasticsearch
- Attendu :
  - `safaa_faculty_profiles` : 79 documents
  - `safaa_university_news` : 151 documents
  - `safaa_research_publications` : 969 documents

```bash
curl -s http://localhost:9200/safaa_faculty_profiles/_count?pretty
curl -s http://localhost:9200/safaa_university_news/_count?pretty
curl -s http://localhost:9200/safaa_research_publications/_count?pretty
```

- Vérifier un sample :

```bash
curl -s 'http://localhost:9200/safaa_faculty_profiles/_search?q=Agadir&pretty=true'
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
docker logs -f --tail=200 airflow-webserver
```

Autres services :
- `university-minio`
- `spark-master`
- `airflow-scheduler`
- `university-elasticsearch`

### 7.2 Airflow UI
1. Ouvrir : http://localhost:8081
2. Aller sur DAG : `safaa_end_to_end_pipeline`
3. Vérifier l’historique : tâches `ingest_*` → `transform_spark` → `write_hudi` → `register_hive` → `index_elasticsearch` → `load_metabase`

---

## 8) Recovery (redémarrage / reprise) 

### 8.1 Redémarrer un service en échec
Exemple : redémarrer Elasticsearch

```bash
docker restart university-elasticsearch
```

### 8.2 Réexécuter un scraper
- Relancer la commande correspondante :
  - ORCID
  - Khan Academy
  - UIZ Web

### 8.3 Réexécuter transformation + indexation
Procéder dans cet ordre :

1. Transformation Spark :
   ```bash
   /opt/spark/bin/spark-submit /opt/spark/work-dir/safaa_transform_faculty_profiles.py
   /opt/spark/bin/spark-submit /opt/spark/work-dir/safaa_transform_university_news.py
   /opt/spark/bin/spark-submit /opt/spark/work-dir/safaa_transform_research_publications.py
   ```

2. Indexation Elasticsearch :
   ```bash
   /opt/spark/bin/spark-submit /opt/spark/work-dir/safaa_index_elasticsearch.py
   ```

---

## 9) Problèmes courants & solutions rapides 

### 9.1 MinIO inaccessible depuis Airflow
Symptômes : la tâche `store_raw_minio` échoue.

Actions :
- Vérifier que le conteneur MinIO tourne :
  ```bash
  docker ps | grep minio
  ```
- Vérifier les endpoints utilisés :
  - `university-minio:9000`
  - `minio:9000`
  - `host.docker.internal:9000`

### 9.2 Hive / Metastore DB non joignable
- Vérifier la santé des conteneurs :
  ```bash
  docker ps | grep hive
  ```
- Vérifier `safaa-hive-mysql` et `safaa-hive-metastore`.

### 9.3 Elasticsearch index non créé / zéro documents
- Vérifier que le job d’indexation a terminé.
- Vérifier le count :
  ```bash
  curl -s http://localhost:9200/safaa_faculty_profiles/_count?pretty
  curl -s http://localhost:9200/safaa_university_news/_count?pretty
  curl -s http://localhost:9200/safaa_research_publications/_count?pretty
  ```
- Si besoin : relancer `safaa_index_elasticsearch.py`.

---

## 10) Arrêter la plateforme 

Arrêt des conteneurs sans supprimer les volumes :

```bash
docker stop airflow-postgres airflow-webserver airflow-scheduler spark-master university-minio university-elasticsearch university-postgres university-metabase safaa-hive-mysql safaa-hive-metastore
```

Arrêt + suppression des conteneurs/réseaux :

- Utilisez `docker compose down -v` **uniquement si vous acceptez de perdre les données**.

---

## 11) Preuves de test 🧪

Toutes les captures d'écran sont disponibles dans `docs/screenshots/`. Elles montrent :
- ✅ UI Airflow avec toutes les tâches en vert
- ✅ Logs des tâches
- ✅ Données dans MinIO
- ✅ Documents indexés dans Elasticsearch
- ✅ Dashboard Metabase
- ✅ Tous les conteneurs en cours

---

## Annexe — Référence : DAG Airflow

DAG : `safaa_end_to_end_pipeline` (`dags/safaa_end_to_end_pipeline.py`)

Chaîne :
1. `check_services`
2. `ingest_web_uiz`
3. `ingest_api_orcid`
4. `ingest_documents`
5. `store_raw_minio`
6. `transform_spark`
7. `check_curated_outputs`
8. `write_hudi`
9. `register_hive`
10. `index_elasticsearch`
11. `load_metabase`
12. `check_metabase`