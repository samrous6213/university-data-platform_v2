# Demo Script — University Data Platform
## Soutenance – 15 minutes

---

## 1. Introduction (1 min)

"Bonjour, je vous présente notre plateforme data universitaire. Notre objectif est de démontrer un pipeline complet d'ingestion, transformation, stockage versionné et visualisation de données pour les universités marocaines, orchestré de bout en bout par Apache Airflow."

---

## 2. Architecture (1 min)

- Montrer le diagramme d'architecture (disponible dans `ARCHITECTURE.md`, section 1 — diagramme Mermaid)
- Expliquer rapidement les composants :
  - **MinIO** pour le stockage brut (zone `raw-*`)
  - **Apache Spark** pour la transformation et le nettoyage
  - **Apache Hudi + Hive Metastore** pour le lakehouse versionné (upsert)
  - **PostgreSQL** comme couche de service pour le BI
  - **Metabase** pour la visualisation (dashboard, 8 KPIs)
  - **Elasticsearch** pour la recherche full-text
  - **Apache Airflow** pour l'orchestration complète (DAG `nezha_pipeline`)

---

## 3. Airflow — Orchestration (3 min)

1. Ouvrir `http://localhost:8081`
2. Se connecter : `admin` / `admin`
3. Montrer le DAG `nezha_pipeline`
4. Expliquer les 7 tâches :
   1. `ingest_usms` — scraping web USMS (4 établissements : FLSH, FST, ENSAK, ESTKH)
   2. `ingest_mit_ocw` — lecture de documents PDF MIT OCW
   3. `ingest_crossref` — appel API Crossref (publications académiques)
   4. `clean_data` — nettoyage et normalisation commune
   5. `write_hudi` — transformation + écriture des 4 tables Hudi + synchronisation Hive
   6. `export_to_postgres` — export vers PostgreSQL pour le BI
   7. `index_elasticsearch` — indexation dans Elasticsearch
5. Montrer les logs d'une tâche (ex : `write_hudi`, avec les messages `✅ Table '...' mise à jour et synchronisée avec Hive`)
6. Montrer l'historique des runs (Grid View, tâches en vert)

📸 Captures suggérées :
- Vue d'ensemble du DAG (Graph View)
- Logs de `write_hudi`
- Grid View avec plusieurs runs réussis

---

## 4. MinIO — Stockage (2 min)

1. Ouvrir `http://localhost:9001`
2. Se connecter : `minioadmin` / `minioadmin`
3. Montrer les buckets :
   - `raw-json` (données structurées : Crossref, USMS, MIT OCW)
   - `raw-web-html` (pages HTML brutes crawlées)
   - `raw-images` (images extraites)
   - `raw-docs` (fichiers PDF MIT OCW)
   - `curated-zone` (contient `hudi_warehouse/`, les 4 tables Hudi)
4. Ouvrir un fichier JSON brut dans `raw-json`, montrer les métadonnées de traçabilité (`source_url`, `crawl_timestamp`, `content_hash`)

📸 Captures suggérées :
- Liste des buckets
- Contenu d'un fichier JSON brut (métadonnées visibles)

---

## 5. Spark → Hudi → Hive (2 min)

1. Exécuter dans le conteneur `spark-master` :
```bash
docker exec -it spark-master /opt/spark/bin/spark-sql \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  -e "USE university_lakehouse; SHOW TABLES; SELECT COUNT(*) FROM faculty_profiles;"
```
2. Montrer les 4 tables : `faculty_profiles` (442 lignes), `course_catalog` (1042 lignes), `research_publications` (50 lignes), `university_news` (282 lignes)
3. Expliquer le mécanisme d'upsert Hudi (clé `record_id`) : chaque ré-exécution met à jour les enregistrements existants plutôt que de les dupliquer

📸 Capture suggérée :
- Résultat de `SHOW TABLES` et du `COUNT(*)`

---

## 6. Elasticsearch — Recherche (2 min)

1. Vérifier que l'API de recherche tourne (à démarrer si besoin) :
```bash
docker exec -d spark-master python3 /workspace/src/search/elasticsearch/query.py --serve
curl "http://localhost:5001/health"
```
2. Effectuer une recherche simple :
```bash
curl "http://localhost:5001/search?q=informatique"
```
→ Montrer un résultat avec les champs pertinents (`title`, `department`/`institution`, etc.)

3. Montrer un filtre par facette :
```bash
curl "http://localhost:5001/search/filter?index=university_news&field=institution&value=flsh"
curl "http://localhost:5001/facets?index=university_news&field=category"
```

📸 Captures suggérées :
- Résultat JSON de `/search`
- Résultat de `/facets`

---

## 7. Metabase — Dashboard (3 min)

1. Ouvrir `http://localhost:3000`
2. Se connecter avec les identifiants configurés
3. Montrer le dashboard "University Analytics Overview" avec les 8 KPIs, par exemple :
   - Total professeurs (`faculty_profiles`)
   - Total publications (`research_publications`)
   - Total actualités (`university_news`)
   - Professeurs par département / faculté
   - Publications par année / journal
   - Actualités par catégorie / institution
4. Montrer un filtre en action (ex : filtrer par département ou par année)
5. Montrer un graphique interactif (drill-down si disponible)

📸 Capture suggérée :
- Vue complète du dashboard

---

## 8. Résilience — preuve de rerun (1.5 min)

1. Relancer manuellement une tâche déjà réussie dans Airflow (`Clear` sur `write_hudi` ou `export_to_postgres`) → repasse au vert
2. Refaire un `SELECT COUNT(*)` sur `faculty_profiles` avant/après → montrer que le compte reste stable (pas de doublons grâce à l'upsert Hudi)
3. Mentionner que le pipeline a déjà survécu en conditions réelles à un crash Docker complet en plein milieu d'exécution, avec reprise automatique par Airflow

---

## 9. Conclusion (1 min)

Récapitulatif des données traitées :
- ✅ 3 types de sources : API (Crossref), web scraping (USMS), fichiers (MIT OCW PDF)
- ✅ 442 profils professeurs
- ✅ 1042 cours (catalogue MIT OCW)
- ✅ 50 publications de recherche
- ✅ 282 actualités universitaires
- ✅ 4 tables curated Hudi, synchronisées Hive (2 minimum requises — 4 livrées)
- ✅ Dashboard Metabase (8 KPIs)
- ✅ Recherche full-text via API HTTP Elasticsearch

Points forts :
- Pipeline robuste et reproductible (upsert Hudi, retry logic Airflow)
- Stack moderne et cohérente (Airflow, Spark, Hudi, Hive, Elasticsearch, Metabase)
- Architecture modulaire, traçabilité de bout en bout (raw → curated → BI/recherche)

Pistes d'amélioration :
- Finaliser l'export complet de `course_catalog` vers la couche BI (actuellement limité par un contournement technique documenté dans `RUNBOOK.md`)
- Enrichissement des données (NLP, catégorisation automatique)
- Ajout de sources supplémentaires

---

## 10. Q&A (1.5 min)

- Répondre aux questions du jury
- Montrer la documentation si nécessaire (`README.md`, `ARCHITECTURE.md`, `RUNBOOK.md`)

---

## ⚠️ Points à anticiper pour le stress-test du jury

Le jury peut demander :
1. **Un rerun complet du DAG** → `docker exec airflow-scheduler airflow dags trigger nezha_pipeline`
2. **Une panne simulée** (ex : `docker stop spark-master` pendant l'exécution) → montrer la procédure de récupération du `RUNBOOK.md`
3. **Une requête de recherche différente** → utiliser directement l'API sans préparation (`/search`, `/facets`, `/search/filter`)
4. **Une question sur les doublons après ré-exécution** → montrer que `SELECT COUNT(*)` reste stable grâce à l'upsert Hudi (clé `record_id`)

---

## ✅ Checklist avant la démo

- [ ] Tous les conteneurs `Up` (`docker ps`)
- [ ] Un DAG run récent en `success` visible dans Airflow (filet de sécurité si le run en direct est lent)
- [ ] Dashboard Metabase accessible et à jour
- [ ] API de recherche démarrée et qui répond (`curl http://localhost:5001/health`)
- [ ] `RUNBOOK.md` ouvert dans un onglet, prêt en cas de besoin

---

## Notes importantes

- ⏱ Ne pas dépasser 15 minutes
- 🔄 Si une tâche est en cours au moment de la démo, expliquer que le pipeline est conçu pour être repris/relancé sans risque (upsert, retries)
- 🐛 En cas d'erreur en direct, montrer les logs Airflow et expliquer la procédure de récupération du `RUNBOOK.md`