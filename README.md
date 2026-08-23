# University Data Platform

![Status](https://img.shields.io/badge/status-mvp-blue)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Spark](https://img.shields.io/badge/Spark-in--container-orange)
![Docker](https://img.shields.io/badge/docker-compose-blue)
![Airflow](https://img.shields.io/badge/Airflow-in--container-blue)

> Plateforme data **end-to-end** pour un environnement universitaire : ingestion multi-sources, lakehouse, orchestration, BI et recherche — entièrement conteneurisée.

---

## Contexte

Ce projet est né dans le cadre du **University Data Platform Challenge** (compétition Best-of-9 MVP, 3 semaines, 9 équipes) : construire une plateforme data complète en respectant un cahier des charges strict — fiabilité et rejouabilité comme critère le plus pondéré, deux tables Hudi obligatoires, exposition SQL via Hive, dashboard BI, recherche full-text, et orchestration Airflow.

Après la compétition, j'ai continué à faire évoluer le projet à titre personnel : migration complète de l'orchestration vers une architecture Docker de bout en bout (voir section Architecture), nettoyage du repo, et documentation pour en faire une pièce de portfolio présentable et reproductible par n'importe qui, sur n'importe quel OS.

**Sources ingérées :** OpenAlex (API), data.gov.ma / CKAN (documents), sites d'établissements universitaires marocains (web scraping statique).

---

## Ce que ça fait

- **Ingère** des données depuis 3 types de sources hétérogènes (API REST, documents CKAN, HTML statique) avec retry/backoff, respect de `robots.txt`, et traçabilité complète (checksum, horodatage, source exacte pour chaque enregistrement)
- **Transforme** avec Spark : normalisation de schéma, déduplication, contrôle qualité avec quarantaine des enregistrements invalides
- **Stocke** en lakehouse avec Apache Hudi (upsert idempotent, versionné), catalogué dans Hive Metastore pour un accès SQL direct
- **Restitue** via un dashboard Metabase (KPIs) et une API de recherche full-text (FastAPI + Elasticsearch, filtres par type d'entité et établissement)
- **Orchestre** tout le pipeline avec Airflow, dont chaque tâche tourne directement dans le conteneur — aucune dépendance à l'OS hôte au-delà de Docker

---

## Architecture

```text
[OpenAlex API] [data.gov.ma / CKAN] [Sites Web (BeautifulSoup)]
                        |
                        v
                [MinIO S3 - raw zone]
                        |
                        v
     [Spark - transformation, dans le conteneur airflow-scheduler]
                        |
                        v
              [Apache Hudi - curated zone]
                        |
                        v
              [Hive Metastore - catalogue SQL]
                 /                       \
                v                         v
     [PostgreSQL] --> [Metabase]   [Elasticsearch] --> [FastAPI search_api]
```

### Tout tourne dans Docker, y compris Spark

Airflow, l'ingestion et les jobs Spark s'exécutent tous **dans le même conteneur** `airflow-scheduler`, via une image custom (`Dockerfile.airflow`) qui embarque Java 17, PySpark et toutes les dépendances Python du projet. Le cache Ivy (jars Hudi/hadoop-aws) est pré-résolu **au build de l'image**, pas à chaque run — pour que le pipeline reste rapide et fiable même en cas de rerun répété.

Le code (`src/`, `configs/`) est monté en volume dans le conteneur. `configs/spark_config.py` définit des valeurs par défaut `localhost`/`127.0.0.1` (utilisables hors Docker si besoin), que `docker-compose.yml` surcharge avec les noms de service Docker (`minio`, `hive-metastore`, `elasticsearch`, `postgres`) pour l'exécution normale.

Résultat : le projet se clone et se lance à l'identique sur Windows, macOS ou Linux — seul Docker est requis.

---

## Stack technique

| Couche | Techno |
|---|---|
| Orchestration | Apache Airflow (conteneurisé) |
| Ingestion | Python (`requests`, `BeautifulSoup`, client MinIO) |
| Data lake (raw) | MinIO (S3-compatible) |
| Transformation | Apache Spark (PySpark 3.4.4) |
| Lakehouse (curated) | Apache Hudi (Copy-on-Write, upsert) |
| Catalogue SQL | Hive Metastore |
| Données structurées | PostgreSQL |
| Recherche | Elasticsearch + FastAPI |
| BI | Metabase |
| Conteneurisation | Docker Compose |

---

## Structure du projet

```text
university-data-platform/
├── dags/
│   └── university_pipeline_dag.py       # DAG : ingestion -> Spark, tout in-container
├── src/
│   ├── api/search_api.py                # API de recherche (FastAPI + Elasticsearch)
│   ├── ingestion/
│   │   ├── api/                         # Source 1 : API OpenAlex
│   │   ├── docs/                        # Source 2 : documents CKAN (data.gov.ma)
│   │   └── web/generic_crawler.py       # Source 3 : web statique (BeautifulSoup)
│   ├── storage/minio/                # Client MinIO partagé
│   ├── lakehouse/
│   │   ├── hudi/hudi_writer.py
│   │   ├── postgres/postgres_writer.py
│   │   └── elasticsearch/es_writer.py
│   └── transformations/spark/
│       ├── config/spark_session.py
│       ├── jobs/                        # faculty_profiles_job.py, course_catalog_job.py
│       ├── pipelines/
│       ├── readers/
│       ├── schemas/
│       ├── transforms/                  # nettoyage, qualité, normalisation
│       └── utils/
├── configs/
│   ├── schools_config.json              # Établissements à crawler
│   └── spark_config.py                  # Endpoints MinIO/Hive/ES/Postgres
├── debug/                                # Scripts de vérification manuelle par composant
├── scripts/legacy_win/               # .bat historiques (debug hors Docker, non requis)
├── jdbc/                                 # Driver JDBC Postgres
├── Dockerfile.airflow                    # Image Airflow custom (Java + PySpark + deps)
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── README.md
└── RUNBOOK.md
```

---

## Démarrage rapide

**Prérequis :** Docker Desktop (Windows/macOS) ou Docker Engine (Linux). Rien d'autre.

```bash
git clone <ton-repo>
cd university-data-platform

cp .env.example .env

docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d

# Vérification
docker compose exec airflow-scheduler airflow dags list-import-errors   # doit être vide
```

Interface Airflow : http://localhost:8081 (`admin` / `admin`) → DAG `university_data_platform_daily` → **Trigger DAG**.

Procédures détaillées (vérification des données, monitoring, recovery) : voir [`RUNBOOK.md`](./RUNBOOK.md).

---

## Traçabilité et idempotence

- Chaque enregistrement curated porte un `content_hash`, une `source_url` et un `raw_object_path` : traçable jusqu'à l'objet brut d'origine dans MinIO.
- Les objets MinIO sont nommés d'après le hash de leur contenu → un rerun sans changement de données ne crée pas de doublon.
- Chaque run d'ingestion écrit un log JSON dans `raw-logs/` (source, volume, erreurs, `ingestion_id`).
- Hudi gère l'upsert nativement : rejouer un job Spark ne duplique jamais les enregistrements déjà présents.

---

## Points d'attention connus

- Le taux de déduplication observé lors des runs de test est élevé (le crawler web revisite parfois les mêmes pages sur plusieurs runs) — piste d'amélioration : affiner la profondeur/fréquence de crawl.
- L'API de recherche n'est pas encore un service Docker à part entière ; elle se lance manuellement dans le conteneur `airflow-scheduler` (voir RUNBOOK section 5).
- Metabase reste hors du DAG automatisé : le dashboard se consulte manuellement, pas de refresh programmé.

---

## Ce que ce projet démontre

- Conception d'un pipeline batch reproductible avec orchestration Airflow
- Ingestion multi-sources hétérogènes avec gestion d'erreurs différenciée (échec critique vs erreur mineure isolée)
- Architecture lakehouse (raw → curated) avec Hudi et upserts idempotents
- Conteneurisation complète d'un stack data (Spark + Java dans Airflow, pas juste de l'orchestration légère)
- Documentation opérationnelle (runbook) pensée pour qu'un tiers puisse reprendre le projet