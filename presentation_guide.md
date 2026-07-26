# Guide de Présentation — Centré sur Airflow

## Structure de la Présentation (10–15 min)

---

### 1. Introduction (1 min)

Montrez le schéma d'architecture global (slide ou Mermaid).

```
Sources → Airflow → MinIO → Spark ETL → Hudi → Hive / ES → Metabase
```

**Phrase clé :** *"Tout part d'Airflow. Une seule plateforme orchestre l'ingestion, la transformation, et l'indexation de A à Z."*

---

### 2. Live : L'interface Airflow (2 min)

Ouvrez `http://localhost:8080`

Montrez :
- La liste des DAGs → cliquez sur `chaimae_pipeline`
- La vue **Graph** → montre les 4 tâches et leurs dépendances
- La vue **Code** → montre le code Python du DAG (défilez rapidement)
- La vue **Tree View** → montre l'historique des exécutions

**Phrase clé :** *"Airflow me donne une visibilité totale : je vois chaque tâche, son état, sa durée, et ses logs."*

---

### 3. Les 3 Sources d'Ingestion (3 min)

Expliquez chaque tâche en montrant son code et son output dans Airflow :

#### Tâche 1 : `openalex_to_minio`
- Cliquez sur la tâche → **Log**
- Montrez les logs : *"Fetching page 1... Fetching page 2..."*
- Say : *"L'API OpenAlex nous donne ~180 auteurs marocains. Les données brutes tombent dans MinIO, bucket `raw-json`."*

#### Tâche 2 : `uca_to_minio`
- Montrez les logs du BFS crawl
- Insistez sur le **timeout de 2 min** ajouté
- Say : *"Scraping des sites UCA et ENSA. ~1100 pages visitées, 280 profs et 170 cours extraits. Sans le timeout que j'ai ajouté, cette tâche tournait indéfiniment."*

#### Tâche 3 : `imist_pdfs_to_minio`
- Montrez les logs rapides
- Say : *"25 documents PDF extraits par mots-clés."*

**Phrase clé :** *"Trois sources complètement différentes — API, Web scraping, PDF — unifiées par Airflow dans un seul lac MinIO."*

---

### 4. La Transformation Spark (2 min)

Montrez la tâche 4 : `spark_etl_to_elasticsearch`

- Cliquez sur la tâche → **Log**
- Montrez la sortie du spark-submit :

```
faculty_profiles    OK    180 records
course_catalog      OK     56 records
university_news     OK     15 records
research_publications OK  20 records
documents_registry  OK     25 records
```

- Say : *"Airflow lance spark-submit dans le conteneur spark-master via Docker. Spark lit depuis MinIO, transforme, écrit dans Hudi (5 tables ACID sur S3), et indexe dans Elasticsearch."*
- Mentionnez les optimisations : *"Le job mettait >20 min. Après optimisation (cache, partitions de shuffle, mémoire), il tourne en ~4 min."*

**Phrase clé :** *"Une seule commande orchestre toute la transformation : `spark-submit run_all_etl.py`."*

---

### 5. Résultat : Metabase + Elasticsearch (2 min)

#### Metabase — `http://localhost:3000`

Montrez le dashboard :
- Compteurs (enseignants, cours, actus, publications)
- Graphiques de répartition par source
- Timeline des publications

Say : *"Metabase interroge Hive via JDBC. Les données viennent des tables Hudi. Aucun chargement manuel — Airflow a tout mis à jour."*

#### Elasticsearch — Démo rapide

```bash
curl http://localhost:9200/faculty_profiles/_search?q=physique
```

Say : *"Elasticsearch indexe les mêmes données pour la recherche full-text. Chaque document a un ID unique pour l'upsert."*

**Phrase clé :** *"Le pipeline complet : du scraping à la visualisation, sans intervention manuelle."*

---

### 6. Démonstration Live (si possible, 3 min)

1. Ouvrez Airflow → **Trigger DAG** → montrez les tâches qui s'exécutent
2. Ouvrez les logs d'`openalex_to_minio` en direct
3. Laissez tourner et passez à Metabase pour montrer que les données précédentes sont déjà visibles
4. Revenez à Airflow pour montrer la fin des tâches

---

### 7. Questions-Réponses (préparez ces points)

| Question possible | Réponse |
|------------------|---------|
| *Pourquoi Hudi ?* | "ACID sur S3, upsert, snapshot isolation, Hive Sync intégré." |
| *Pourquoi pas tout dans Spark ?* | "Hudi donne des tables persistantes queryables en SQL. Spark seul ne stocke pas." |
| *Problème le plus difficile ?* | "Le schema Hudi qui changeait entre exécutions. Résolu avec `schema.allow.key.field.schema.changes=true`." |
| *Scalabilité ?* | "Le DAG passe de SequentialExecutor à CeleryExecutor pour du parallélisme. Spark passe de local[*] à cluster." |
| *Pourquoi Elasticsearch en plus de Hive ?* | "Hive = analytique SQL lent. ES = recherche full-text <100ms." |

---

### Slides à préparer

1. **Schéma architectural** — flèche haute en couleur
2. **Capture d'écran Airflow** — DAG graph view
3. **Logs de chaque tâche** — preuve du succès
4. **Metabase dashboard** — capture des KPIs
5. **Elasticsearch query** — résultat d'une recherche

Tout montrer sur le live si possible — c'est plus impressionnant que des slides.
