# Architecture — University Data Platform

## 1. Diagramme complet
==> Voir [`university_data_platform_architecture_v2.svg`](https://github.com/samrous6213/university-data-platform_v2/blob/nezha/docs/university_data_platform_architecture_v2.svg) pour l'architecture détaillé. 

## 2. Rôle de chaque composant

### Zone brute — MinIO
Stocke les données exactement telles que collectées, avec métadonnées de traçabilité (`source_system`, `source_url`, `content_hash`, `crawl_timestamp`). Permet de rejouer l'ingestion sans re-scraper les sources externes.

Buckets utilisés :
- `raw-json` — API Crossref, métadonnées MIT OCW, données consolidées USMS (faculty, news)
- `raw-web-html` — pages HTML brutes crawlées
- `raw-images` — images extraites (pages génériques + actualités, séparées par catégorie)
- `raw-docs` — fichiers PDF MIT OCW
- `curated-zone` — zone curated : contient `hudi_warehouse/`, où Spark écrit les 4 tables Hudi (`faculty_profiles`, `course_catalog`, `research_publications`, `university_news`). C'est le seul bucket MinIO utilisé en dehors de la zone brute ; toutes les tables curated y résident physiquement, même si elles sont exposées en SQL via Hive Metastore.

### Traitement — Apache Spark
Lit les données brutes, applique nettoyage, déduplication, génération de `record_id` (hash SHA-256), normalisation de texte, puis écrit au format Hudi.

Un fichier de transformation dédié par source (`transform_faculty.py`, `transform_courses.py`, `transform_publications.py`, `transform_news.py`) + un module commun (`clean_data.py`) pour éviter la duplication de logique.

### Zone curated — Apache Hudi
Format de table transactionnel supportant l'upsert : chaque exécution du pipeline met à jour les enregistrements existants (par `record_id`) plutôt que de dupliquer les données. C'est ce qui garantit la sécurité des ré-exécutions (voir section Resilience du Runbook).

Type de table : `COPY_ON_WRITE`, optimisé pour la lecture (adapté à un usage BI).

Les 4 tables curated sont écrites et synchronisées avec Hive Metastore. **3 des 4 tables** (`faculty_profiles`, `research_publications`, `university_news`) sont exportées avec succès vers la couche de service PostgreSQL/Elasticsearch (voir ci-dessous) ; `course_catalog` reste disponible et interrogeable en SQL via Hive/Spark SQL, mais son export vers Postgres rencontre une limitation technique connue (voir `RUNBOOK.md`, section Limitations connues).

### Catalogue — Hive Metastore
Rend les tables Hudi interrogeables en SQL standard depuis Spark SQL, sans dépendre du format de stockage sous-jacent. Base `university_lakehouse`.

### Serving — PostgreSQL + Metabase
Les données curated sont exportées vers PostgreSQL (via Pandas/psycopg2, pour contourner une limitation du connecteur JDBC Spark rencontrée dans cet environnement). Metabase se connecte à Postgres pour construire le dashboard. 3 des 4 tables curated sont exportées et disponibles dans Postgres (voir ci-dessus).

### Recherche — Elasticsearch + API Flask
Chaque table curated **effectivement exportée vers Postgres** est indexée dans Elasticsearch (un index dédié par table, soit 3 index : `faculty_profiles`, `research_publications`, `university_news`). Une API Flask légère, intégrée à `src/search/elasticsearch/query.py` (mode `--serve`), expose des endpoints HTTP pour interroger ces index sans connaissance d'Elasticsearch côté utilisateur final.

### Orchestration — Apache Airflow
DAG `nezha_pipeline` définissant les dépendances entre tâches :

```
ingest_crossref  ┐
ingest_usms      ├──▶ clean_data ──▶ write_hudi ──▶ export_to_postgres ──▶ index_elasticsearch
ingest_mit_ocw   ┘
```

Chaque tâche dispose de retries configurés côté Airflow ; les échecs sont visibles et relançables individuellement sans redémarrer tout le pipeline.

## 3. Choix d'architecture justifiés

- **MinIO plutôt qu'un vrai S3** : compatible S3 API, déployable localement sans coût cloud, suffisant pour un MVP.
- **Hudi plutôt que Parquet simple** : upsert natif indispensable pour des ré-exécutions quotidiennes sans dupliquer les données ni tout recharger.
- **Export vers Postgres pour le BI** : Metabase n'a pas de connecteur Hudi natif fiable dans cet environnement ; Postgres est une cible BI standard, stable, et rapide à interroger.
- **API Flask dédiée pour la recherche** : découple Elasticsearch (technique) de l'usage final (HTTP simple), conforme à l'exigence du cahier des charges d'un "basic search endpoint".
