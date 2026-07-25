# CHAIME — Projet University Data Platform v2

## Objectif
Pipeline Spark ETL complet qui lit les données depuis MinIO (`raw-json` bucket), les transforme via 5 extracteurs, et écrit dans 5 tables Hudi curées sur MinIO (`hudi-curated` bucket).

---

## Architecture

```
docker-compose : spark-master (1 worker local) + MinIO + Hive Metastore
```

| Couche | Technologie | Rôle |
|--------|-------------|------|
| Stockage brut | MinIO `raw-json` | 5 prefixes `source=*/` : fssm (842 JSON-LD), ensa (1 news), encg (96 JSON-LD), openalex (2), imist_docs (281) |
| Moteur ETL | Apache Spark 3.5 + Hudi 0.15 | Traitement distribué |
| Curated | MinIO `hudi-curated` | 5 tables Hudi COW partitionnées |
| Metastore | Hive Metastore (thrift) | (désactivé pour les écritures) |

## Structure du code

```
src/transformations/
├── config/
│   ├── spark_config.py          # Configuration Spark (S3A, MinIO, Hive, Hudi)
│   └── hudi_config.py           # 5 HudiTableConfig (table_name, record_key, partition_field, etc.)
├── readers/
│   └── minio_reader.py          # Lecture MinIO (read_json, read_raw_records, discover_source_prefixes, cache LRU)
├── transformers/
│   ├── base_transformer.py      # Utilitaires : drop_nulls, fill_defaults, normalize_string, deduplicate_by
│   ├── faculty_transformer.py   # Faculty → full_name, first_name, last_name, email, etc.
│   ├── course_transformer.py    # Course → course_code, course_name, credits, etc.
│   ├── news_transformer.py      # News → title, content, publication_date, etc.
│   ├── publications_transformer.py  # Publications → title, abstract, doi, authors, etc.
│   └── documents_transformer.py # Documents → document_name, file_size, storage_path, etc.
├── writers/
│   └── hudi_writer.py           # write_hudi_table avec retry (upsert Hudi)
├── utils/
│   ├── logger.py
│   ├── metadata.py              # add_processing_timestamp
│   └── schema_validator.py
├── etl/
│   ├── faculty_profiles_etl.py
│   ├── course_catalog_etl.py
│   ├── university_news_etl.py
│   ├── research_publications_etl.py
│   └── documents_registry_etl.py
├── run_all_etl.py               # Entry point, exécute les 5 ETLs séquentiellement
```

---

## Fonctionnement (Data Flow)

1. **Découverte automatique** : `discover_source_prefixes()` liste tous les dossiers `source=*/` via Hadoop S3A `FileSystem.listStatus()`
2. **Lecture** : `read_json()` lit chaque prefixe, cache le résultat en mémoire Spark
3. **Explosion** : chaque ETL définit une liste de `array_fields` ; pour chaque source, si un champ tableau correspond, il est explosé via `inline_outer()` ; sinon la source est ignorée (sauf documents_registry qui passe tout en raw)
4. **Transformation** : chaque ETL appelle son transformateur sur les données explosées (une source à la fois pour faculty/course, union après transformation)
5. **Écriture Hudi** : `write_hudi_table()` upsert en COPY_ON_WRITE avec déduplication par `record_id`

## Tables Hudi

| Table | Nombre d'enregistrements | Partition | Clé |
|-------|--------------------------|-----------|-----|
| faculty_profiles | 20 | faculty | record_id |
| course_catalog | 35 | faculty | record_id |
| university_news | 15 | faculty | record_id |
| research_publications | 20 | publication_year | record_id |
| documents_registry | 175 | document_type | record_id |

## Sources MinIO et leurs champs tableau

| Source | Fichiers | Champ tableau | Utilisé par |
|--------|----------|---------------|-------------|
| source=fssm/ | 842 JSON-LD | `faculty_items` | faculty_profiles |
| source=ensa/ | 1 fichier | `news_items` | course_catalog, university_news |
| source=encg/ | 96 JSON-LD | `faculty_members` | faculty_profiles |
| source=openalex/ | 2 fichiers | `results` | faculty_profiles, course_catalog, research_publications |
| source=imist_docs/ | 281 métadonnées | (aucun, raw pass-through) | documents_registry |

## Commandes d'exécution

```powershell
# Lancer tous les ETLs
docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master /opt/spark/bin/spark-submit `
  --master local[1] --driver-memory 4g `
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir `
  /opt/spark/work-dir/src/transformations/run_all_etl.py
```

## Problèmes résolus

1. **Hudi write fail** : `HoodieDuplicateKeyException` → ajout de `dropDuplicates([record_key])` dans `hudi_writer.py`
2. **OOM driver** : 842 fichiers JSON-LD → `--driver-memory 4g`
3. **Path not found** : `read_json()` retourne DataFrame vide au lieu de crasher
4. **Colonnes manquantes** : `drop_nulls()` et `fill_defaults()` ignorent les colonnes absentes
5. **Noms de colonnes dupliqués** : `_map_fields()` exclut les noms cibles du passthrough, utilise `COALESCE` pour les mappings multi-sources
6. **Perte de données OpenAlex dans course_catalog** : Les sources hétérogènes (ENSA news + OpenAlex) mélangées avant transformation → transformation par source individuelle puis union
7. **Double comptage OpenAlex dans faculty_profiles** : Appels redondants aux mêmes sources → découverte unique par source, transformation isolée
8. **Timeouts longs** : Cache Spark des lectures brutes avec `df.cache().count()` pour éviter les re-lectures de 842 fichiers
