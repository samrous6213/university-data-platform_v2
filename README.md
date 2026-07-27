# University Data Platform — MVP

Plateforme de données universitaire complète : ingestion multi-source → lakehouse versionné (Hudi/Hive) → BI (Metabase) → recherche full-text (Elasticsearch), orchestrée par Apache Airflow.

Réalisé dans le cadre du challenge "University Data Platform — Best-of-9 MVP Competition".

## 1. Aperçu du pipeline

```
Sources (API + Web + Fichiers)
        │
        ▼
   MinIO (zone brute, avec métadonnées)
        │
        ▼
   Spark (nettoyage, normalisation, transformation)
        │
        ▼
   Apache Hudi (zone curated, tables versionnées, upsert)
        │
        ▼
   Hive Metastore (catalogue SQL partagé)
        │
   ┌────┴────┐
   ▼         ▼
Postgres   (export BI)
   │
   ▼
Metabase (dashboard, 8 KPIs)          Elasticsearch (recherche full-text + API HTTP)

Orchestration de bout en bout : Apache Airflow (DAG `nezha_pipeline`)
```

Voir [`ARCHITECTURE.md`](./ARCHITECTURE.md) pour le diagramme détaillé et le rôle de chaque composant.

## 2. Sources de données (3 types requis)

| Type | Source | Description |
|---|---|---|
| API | Crossref | Publications académiques (DOI, titre, auteurs, journal, année) |
| Web scraping | USMS (4 établissements : FLSH, FST, ENSAK, ESTKH) | Profils professeurs, pages institutionnelles, actualités |
| Fichiers | MIT OCW | Documents PDF de cours (métadonnées + fichiers) |

## 3. Tables curated (Hudi / Hive)

Base Hive : `university_lakehouse`

| Table | Lignes | Clé | Partition |
|---|---|---|---|
| `faculty_profiles` | 442 | `record_id` | `faculty` |
| `course_catalog` | 1042 | `record_id` | `department` |
| `research_publications` | 50 | `record_id` | `publication_year` |
| `university_news` | 282 | `record_id` | `publication_year` |

(2 tables minimum étaient requises — 4 livrées)

## 4. Stack technique

| Composant | Rôle |
|---|---|
| MinIO | Stockage objet S3-compatible, zone brute |
| Apache Spark 3.5.1 | ETL, transformation, écriture Hudi |
| Apache Hudi 0.15.0 | Tables analytiques versionnées, upsert |
| Hive Metastore | Catalogue SQL partagé (Postgres en backend) |
| PostgreSQL | Backend Hive Metastore + backend Airflow + export BI |
| Metabase | Dashboard BI (8 KPIs) |
| Elasticsearch 8.11 | Index de recherche full-text + API HTTP intégrée à `query.py` (mode `--serve`, Flask) |
| Apache Airflow 2.10 | Orchestration du pipeline complet (DAG `nezha_pipeline`) |
| Docker Compose | Déploiement de l'ensemble des services |

## 5. Installation

### Prérequis
- Docker Desktop avec WSL2 activé (Windows) — au moins 8 Go de RAM alloués à WSL2
- Docker Compose v2

### Étapes

```bash
git clone <url-du-repo>
cd university-data-platform_v2

# Construire l'image Spark personnalisée (dépendances Python pré-installées)
docker build -t university-spark:custom -f Dockerfile.spark .

# Lancer l'ensemble des services
docker compose up -d
```

Vérifier que tous les services sont démarrés :
```bash
docker ps
```

Services attendus : `minio`, `hive-metastore`, `university-postgres`, `airflow-postgres`, `spark-master`, `spark-worker`, `university-metabase`, `university-elasticsearch`, `airflow-webserver`, `airflow-scheduler`.

### Initialisation (première installation uniquement)

```bash
# Initialiser le schéma Hive Metastore
docker exec hive-metastore /opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType postgres -initSchema
```

Voir [`RUNBOOK.md`](./RUNBOOK.md) pour le détail complet du démarrage, du monitoring et de la récupération après incident.

## 6. Lancer le pipeline

### Option A — via Airflow (recommandé, orchestration complète)
```
http://localhost:8081
```
DAG `nezha_pipeline` → bouton "Trigger DAG".

### Option B — étape par étape, manuellement
Voir [`RUNBOOK.md`](./RUNBOOK.md) section "Exécution manuelle".

## 7. Accès aux interfaces

| Interface | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin (à adapter) |
| Metabase | http://localhost:3000 | compte créé à la première connexion |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| API de recherche Elasticsearch | http://localhost:5001 | — |
| Spark Master UI | http://localhost:8080 | — |

## 8. Recherche full-text (endpoint HTTP)

L'API de recherche est intégrée directement dans `src/search/elasticsearch/query.py` (mode `--serve`), pas dans un fichier séparé. Elle tourne dans le conteneur `spark-master`, exposée sur le port hôte `5001`.

### Démarrer l'API (à refaire après chaque redémarrage de `spark-master`)
```bash
docker exec -d spark-master python3 /workspace/src/search/elasticsearch/query.py --serve
```

### Endpoints disponibles
```bash
curl "http://localhost:5001/health"
curl "http://localhost:5001/search?q=informatique"
curl "http://localhost:5001/search?q=informatique&index=university_news"
curl "http://localhost:5001/search/filter?index=university_news&field=institution&value=flsh"
curl "http://localhost:5001/facets?index=university_news&field=category"
```

### Mode CLI (alternatif, sans serveur HTTP)
```bash
docker exec spark-master python3 /workspace/src/search/elasticsearch/query.py "informatique"
```

⚠️ Le client Python `elasticsearch` doit être épinglé à une version compatible avec le serveur Elasticsearch 8.11 (`elasticsearch>=8.11,<9.0` dans `Dockerfile.spark`), sinon les requêtes échouent avec une erreur de version d'API incompatible.

## 9. Documentation complète

- [`ARCHITECTURE.md`](./ARCHITECTURE.md) — diagramme détaillé et rôle de chaque composant
- [`RUNBOOK.md`](./RUNBOOK.md) — démarrage, monitoring, récupération après panne
- [`DEMO_SCRIPT.md`](./DEMO_SCRIPT.md) — script de démonstration (15 minutes)