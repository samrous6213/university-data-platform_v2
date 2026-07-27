# University Data Platform — RUNBOOK

> **Stack** : Spark 3.5.1 · Hive Metastore · Hudi 0.15.0 · PostgreSQL · Elasticsearch 8.11 · Metabase · MinIO
> **Dernière mise à jour** : Juillet 2026

---

## Table des matières

1. [Installation](#1-installation)
2. [Vérification des containers](#2-vérification-des-containers)
3. [Spark](#3-spark)
4. [ETL Hudi](#4-etl-hudi)
5. [Vérification Hudi](#5-vérification-hudi)
6. [Elasticsearch — Indexation](#6-elasticsearch--indexation)
7. [Vérification Elasticsearch](#7-vérification-elasticsearch)
8. [Metabase](#8-metabase)
9. [Vérifications finales](#9-vérifications-finales)
10. [Dépannage](#10-dépannage)

---

## 1. Installation

### 1.1 Installer Docker Desktop

**Objectif** : Installer l'environnement Docker sur Windows.

**Commandes** :

```powershell
# Télécharger Docker Desktop depuis https://www.docker.com/products/docker-desktop/
# Puis installer et redémarrer la machine
```

**Explication** : Docker Desktop embarque le moteur Docker, Docker Compose et le plug-in WSL2. C'est l'outil principal pour faire tourner l'ensemble des services.

**Vérification** :

```powershell
docker --version
docker compose version
```

**Résultat attendu** :

```
Docker version 27.x.x, build xxxxxxx
Docker Compose version v2.x.x
```

---

### 1.2 Installer / Activer WSL2

**Objectif** : Activer le sous-système Windows pour Linux nécessaire au fonctionnement de Docker Desktop.

**Commandes** :

```powershell
# Depuis PowerShell (admin)
wsl --install
# Redémarrer la machine, puis :
wsl --set-default-version 2
```

**Explication** : WSL2 fournit un noyau Linux réel pour Docker Desktop, offrant de meilleures performances que la VM Hyper-V classique.

**Vérification** :

```powershell
wsl --status
```

**Résultat attendu** :

```
Default Version: 2
```

---

### 1.3 Cloner le projet

**Objectif** : Récupérer le code source du projet.

**Commandes** :

```powershell
git clone https://github.com/VOTRE_UTILISATEUR/university-data-platform.git
cd university-data-platform
```

**Vérification** :

```powershell
ls
```

**Résultat attendu** :

```
Dockerfile.airflow/
Dockerfile.hive/
Dockerfile.spark/
docker-compose.yml
dags/
data/
src/
conf/
requirements-airflow.txt
```

---

### 1.4 Lancer les services

**Objectif** : Démarrer tous les conteneurs Docker du projet.

**Commandes** :

```powershell
docker compose up -d --build
```

**Explication** :

| Flag | Rôle |
|------|------|
| `up` | Crée et démarre les containers |
| `-d` | Détache les containers (mode arrière-plan) |
| `--build` | Force la reconstruction des images (Dockerfile.hive, Dockerfile.spark, Dockerfile.airflow) |

**Vérification** :

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

**Résultat attendu** :

```
NAMES                    STATUS
university-spark-master  Up (healthy)
university-spark-worker  Up
university-hive-metastore Up (healthy)
university-postgres      Up (healthy)
university-elasticsearch Up (healthy)
university-metabase      Up
university-minio         Up (healthy)
```

---

### 1.5 Consulter les logs

**Objectif** : Vérifier qu'aucune erreur ne remonte au démarrage.

**Commandes** :

```powershell
# Logs de tous les services
docker compose logs --tail=50

# Logs d'un service spécifique
docker compose logs --tail=50 spark-master
docker compose logs --tail=50 hive-metastore
```

**Explication** : `--tail=50` affiche les 50 dernières lignes de logs, suffisant pour détecter les erreurs de démarrage.

---

## 2. Vérification des containers

### 2.1 Spark Master

**Commandes** :

```powershell
docker ps --filter "name=spark-master" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker logs university-spark-master --tail=30
```

**Résultat attendu** :

```
NAMES               STATUS        PORTS
university-spark-master  Up (healthy)  0.0.0.0:18080->18080/tcp, 0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
```

Le port `8080` est l'interface Web UI de Spark Master, `7077` le port de communication interne.

**Vérification** :

```powershell
Invoke-RestMethod http://localhost:8080/json/ | ConvertTo-Json -Depth 2
```

**Résultat attendu** : Un JSON contenant `"status":"ALIVE"`, `"workers": [...]`, `"activeWorkers": 1`.

---

### 2.2 Spark Worker

**Commandes**:

```powershell
docker ps --filter "name=spark-worker" --format "table {{.Names}}\t{{.Status}}"
docker logs university-spark-worker --tail=20
```

**Résultat attendu** : Le worker doit afficher `"CONNECTED"` et apparaître dans la liste des workers du master.

---

### 2.3 Hive Metastore

**Commandes** :

```powershell
docker ps --filter "name=hive-metastore" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker logs university-hive-metastore --tail=30
```

**Résultat attendu** :

```
NAMES                   STATUS         PORTS
university-hive-metastore  Up (healthy)   0.0.0.0:9083->9083/tcp
```

Le port `9083` est le port Thrift du Metastore Hive, utilisé par Spark pour découvrir les tables.

**Vérification** :

```powershell
docker exec university-hive-metastore beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"
```

**Résultat attendu** :

```
+----------------+
| database_name  |
+----------------+
| default        |
| university_data_platform |
+----------------+
```

---

### 2.4 PostgreSQL

**Commandes** :

```powershell
docker ps --filter "name=postgres" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker logs university-postgres --tail=20
```

**Résultat attendu** :

```
NAMES             STATUS         PORTS
university-postgres  Up (healthy)   0.0.0.0:5432->5432/tcp
```

**Vérification** :

```powershell
docker exec university-postgres psql -U hive -d metastore -c "\dt"
```

**Résultat attendu** : Liste des tables Hudi synchronisées par Hive (`DBS`, `TBLS`, `SDS`, etc.).

---

### 2.5 Elasticsearch

**Commandes** :

```powershell
docker ps --filter "name=elasticsearch" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker logs university-elasticsearch --tail=30
```

**Résultat attendu** :

```
NAMES                    STATUS         PORTS
university-elasticsearch  Up (healthy)   0.0.0.0:9200->9200/tcp, 0.0.0.0:9300->9300/tcp
```

**Vérification** :

```powershell
Invoke-RestMethod http://localhost:9200
```

**Résultat attendu** : JSON avec `"version" : { "number" : "8.11.x" }` et `"cluster_name" : "university-cluster"`.

---

### 2.6 MinIO

**Commandes** :

```powershell
docker ps --filter "name=minio" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker logs university-minio --tail=20
```

**Résultat attendu** :

```
NAMES          STATUS         PORTS
university-minio  Up (healthy)   0.0.0.0:9000->9000/tcp, 0.0.0.0:9001->9001/tcp
```

Le port `9001` est l'interface Web Console MinIO, `9000` l'API S3.

**Vérification** :

```powershell
Invoke-RestMethod http://localhost:9001 -ErrorAction SilentlyContinue
```

**Résultat attendu** : Page de connexion MinIO Console.

---

### 2.7 Metabase

**Commandes** :

```powershell
docker ps --filter "name=metabase" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker logs university-metabase --tail=30
```

**Résultat attendu** :

```
NAMES             STATUS    PORTS
university-metabase  Up        0.0.0.0:3000->3000/tcp
```

**Vérification** : Ouvrir `http://localhost:3000` dans un navigateur. L'assistant de configuration s'affiche.

---

## 3. Spark

### 3.1 Connexion au Spark Master

**Objectif** : Accéder à l'intérieur du conteneur Spark Master pour lancer des commandes spark-submit.

**Commandes** :

```bash
docker exec -it spark-master bash
```

**Explication** : `docker exec -it` lance un terminal interactif dans le conteneur `spark-master`.

### 3.2 Répertoire de travail

**Commandes** :

```bash
cd /opt/spark/work-dir
```

**Explication** : C'est le répertoire contenant le code source du projet (`src/`), monté via le volume Docker. Les scripts ETL et d'indexation s'exécutent depuis ce répertoire.

**Vérification** :

```bash
ls /opt/spark/work-dir/src/transformations/spark/
```

**Résultat attendu** :

```
faculty_profiles_etl.py
research_publications_etl.py
university_news_etl.py
documents_registry_etl.py
```

---

## 4. ETL Hudi

Chaque script ETL lit des données JSON brutes depuis MinIO, applique des transformations (nettoyage, validation, normalisation des noms de champs), puis écrit les résultats dans une table Hudi stockée sur MinIO et synchronisée avec le Hive Metastore.

### 4.1 Faculty Profiles

**Objectif** : Extraire et transformer les profils de professeurs (noms, départements, emails, intérêts de recherche) depuis plusieurs sources hétérogènes.

**Commande** :

```bash
PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
--conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
--conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
src/transformations/spark/faculty_profiles_etl.py
```

**Ce que fait le script** :

1. **Découverte automatique** : Parcourt les prefixes du bucket `raw-json` pour trouver toutes les sources de données
2. **Lecture JSON** : Lit les fichiers JSON bruts depuis MinIO via `s3a://raw-json/<prefix>/`
3. **Flattening** : Explose les arrays imbriqués (`faculty_items`, `faculty_members`, `profiles`, `items`, `results`, `courses`) via `inline_outer`
4. **Normalisation** : Coalesce les variantes de noms de champs (EN/FR), trim les chaînes, valide la présence du champ `name`
5. **Génération de clé** : Crée un `record_id` MD5 à partir de `name::department::source`
6. **Écriture Hudi** : Upsert dans `university_data_platform.faculty_profiles` avec synchronisation Hive

**Résultat attendu** : Logs Spark se terminant par `Process exited with exit code 0`. La table `university_data_platform.faculty_profiles` contient les données.

---

### 4.2 Research Publications

**Objectif** : Extraire et transformer les publications scientifiques (titres, auteurs, DOI, résumés, mots-clés) depuis les sources académiques.

**Commande** :

```bash
PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
--conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
--conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
src/transformations/spark/research_publications_etl.py
```

**Ce que fait le script** :

1. **Lecture** : Lit les enregistrements bruts depuis MinIO
2. **Flattening** : Explose l'array `results`
3. **Normalisation** : Aplati les arrays `authors` et `keywords` en chaînes virgulées (`"Author1, Author2"`), valide la présence de `title`, parse l'année depuis des formats variés (ex: `"2024-01-15"` → `2024`)
4. **Écriture Hudi** : Upsert dans `university_data_platform.research_publications`

---

### 4.3 University News

**Objectif** : Extraire et transformer les actualités universitaires (titres, résumés, catégories, auteurs).

**Commande** :

```bash
PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
--conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
--conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
src/transformations/spark/university_news_etl.py
```

**Ce que fait le script** :

1. **Lecture** : Lit les enregistrements bruts depuis MinIO
2. **Flattening** : Explose l'array `news_items`
3. **Normalisation** : Valide la présence de `headline`, génère un `record_id` MD5 basé sur `headline::source_url-or-source`
4. **Écriture Hudi** : Upsert dans `university_data_platform.university_news`

---

### 4.4 Documents Registry

**Objectif** : Extraire et transformer le registre de documents universitaires (noms, types, tailles, départements).

**Commande** :

```bash
PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
--conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
--conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
src/transformations/spark/documents_registry_etl.py
```

**Ce que fait le script** :

1. **Lecture** : Lit les enregistrements plats depuis MinIO (pas de flattening nécessaire)
2. **Normalisation** : Cast `file_size` en Long, `year` en Int, valide la présence de `document_name`
3. **Écriture Hudi** : Upsert dans `university_data_platform.documents_registry`

---

## 5. Vérification Hudi

### 5.1 Vérifier l'existence des tables

**Objectif** : Confirmer que le Hive Metastore contient les 4 tables Hudi.

**Commandes** :

```bash
docker exec -it spark-master bash
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN university_data_platform;"
```

**Résultat attendu** :

```
+-------------------------------+---------------------+
|           tab_name            |     database_name   |
+-------------------------------+---------------------+
| faculty_profiles              | university_data_platform |
| research_publications         | university_data_platform |
| university_news               | university_data_platform |
| documents_registry            | university_data_platform |
+-------------------------------+---------------------+
```

---

### 5.2 Compter les lignes

**Commandes** :

```bash
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM university_data_platform.faculty_profiles;"

/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM university_data_platform.research_publications;"

/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM university_data_platform.university_news;"

/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM university_data_platform.documents_registry;"
```

**Résultat attendu** : Chaque requête retourne un nombre entier (≥ 0).

---

### 5.3 Afficher quelques lignes

**Commandes** :

```bash
/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT * FROM university_data_platform.faculty_profiles LIMIT 3;"

/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT * FROM university_data_platform.research_publications LIMIT 3;"

/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT * FROM university_data_platform.university_news LIMIT 3;"

/opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT * FROM university_data_platform.documents_registry LIMIT 3;"
```

**Résultat attendu** : Un tableau avec les colonnes de chaque table (record_id, name/title/headline/document_name, department, source_system, crawl_timestamp, year, etc.).

---

## 6. Elasticsearch — Indexation

### 6.1 Principe

Chaque script d'indexation :

1. **Lit** une table Hudi depuis le filesystem local (`/opt/spark/work-dir/data/lakehouse/hudi/<table>`)
2. **Convertit** chaque ligne en JSON
3. **Envoie** le document dans Elasticsearch via l'API REST, en utilisant `record_id` comme `_id`

### 6.2 Connexion au Spark Master

```bash
docker exec -it spark-master bash
cd /opt/spark/work-dir/src/search
```

### 6.3 Indexer Faculty Profiles

**Commande** :

```bash
/opt/spark/bin/spark-submit index_faculty_profiles.py
```

**Ce que fait le script** : Lit `/opt/spark/work-dir/data/lakehouse/hudi/faculty_profiles`, itère sur chaque ligne, envoie le JSON dans l'index `faculty_profiles` sur Elasticsearch.

---

### 6.4 Indexer Research Publications

**Commande** :

```bash
/opt/spark/bin/spark-submit index_research_publications.py
```

**Ce que fait le script** : Lit les données Hudi de `research_publications`, envoie chaque document dans l'index `research_publications` sur Elasticsearch.

---

### 6.5 Indexer Documents Registry

**Commande** :

```bash
/opt/spark/bin/spark-submit index_documents_registry.py
```

**Ce que fait le script** : Lit les données Hudi de `documents_registry`, envoie chaque document dans l'index `documents_registry` sur Elasticsearch.

---

### 6.6 Indexer University News

**Commande** :

```bash
/opt/spark/bin/spark-submit index_university_news.py
```

**Ce que fait le script** : Lit les données Hudi de `university_news`, envoie chaque document dans l'index `university_news` sur Elasticsearch.

---

## 7. Vérification Elasticsearch

### 7.1 Lister les index

**Commande** (depuis PowerShell) :

```powershell
Invoke-RestMethod http://localhost:9200/_cat/indices?v
```

**Résultat attendu** :

```
health status index                    uuid                   pri rep docs.count docs.deleted store.size pri.store.size
green  open   faculty_profiles         xxxxxxxxxxxxxxxxxxxx   1   0       1234            0      1.2mb          1.2mb
green  open   research_publications    xxxxxxxxxxxxxxxxxxxx   1   0        567            0    890.5kb        890.5kb
green  open   documents_registry       xxxxxxxxxxxxxxxxxxxx   1   0         89            0    234.1kb        234.1kb
green  open   university_news          xxxxxxxxxxxxxxxxxxxx   1   0         45            0    123.4kb        123.4kb
```

---

### 7.2 Compter les documents par index

**Commandes** :

```powershell
Invoke-RestMethod http://localhost:9200/faculty_profiles/_count
Invoke-RestMethod http://localhost:9200/research_publications/_count
Invoke-RestMethod http://localhost:9200/documents_registry/_count
Invoke-RestMethod http://localhost:9200/university_news/_count
```

**Résultat attendu** :

```json
{ "count" : 1234, "_shards" : { "total" : 1, "successful" : 1, "skipped" : 0, "failed" : 0 } }
```

Chaque count doit correspondre au COUNT(*) obtenu à l'étape 5.2.

---

### 7.3 Recherche de test

```powershell
Invoke-RestMethod "http://localhost:9200/faculty_profiles/_search?q=*&pretty"
Invoke-RestMethod "http://localhost:9200/university_news/_search?q=*&pretty"
```

---

## 8. Metabase

### 8.1 Accès

Ouvrir `http://localhost:3000` dans un navigateur.

### 8.2 Assistant de configuration initiale

Lors de la première connexion, Metabase affiche un assistant de configuration. Choisir :

| Champ | Valeur |
|-------|--------|
| Base de données | PostgreSQL |
| Host | `university-postgres` (ou `postgres`) |
| Port | `5432` |
| Nom de la base | `metastore` |
| Utilisateur | `hive` |
| Mot de passe | `hive` |

### 8.3 Connexion à la base de données Hive

Metabase peut se connecter directement au Hive Metastore via :

- **Via Spark Thrift** (si démarré) : Host `spark-thrift`, Port `10000`
- **Via PostgreSQL** : Host `university-postgres`, DB `metastore`

### 8.4 Création des dashboards

1. Cliquer **"Nouvelle question"** → **"Matérialiser une nouvelle question"**
2. Sélectionner la source de données (Hive via Spark Thrift ou PostgreSQL)
3. Écrire une requête SQL ou utiliser l'éditeur visuel
4. Cliquer **"Enregistrer"** → **"Ajouter à un tableau de bord"**

---

## 9. Vérifications finales

### Checklist

```
✅ Docker Desktop lancé et WSL2 activé
✅ docker compose up -d --build exécuté sans erreur
✅ Tous les containers en status UP ou HEALTHY
✅ spark-master : http://localhost:8080 → "ALIVE"
✅ spark-worker connecté au master
✅ hive-metastore : port 9083 accessible
✅ postgres : port 5432 accessible
✅ elasticsearch : http://localhost:9200 → version 8.11.x
✅ minio : http://localhost:9001 → console accessible
✅ 4 tables Hudi créées dans university_data_platform
✅ Données chargées dans les 4 tables (COUNT > 0)
✅ 4 index Elasticsearch créés (faculty_profiles, research_publications, documents_registry, university_news)
✅ Nombre de documents Elasticsearch = nombre de lignes Hudi
✅ Metabase : http://localhost:3000 → assistant de configuration
```

### Vérification rapide globale

```powershell
# Tous les containers actifs
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Elasticsearch
Invoke-RestMethod http://localhost:9200/_cat/indices?v

# Spark Master
Invoke-RestMethod http://localhost:8080/json/ | Select-Object status

# Metabase
Invoke-RestMethod http://localhost:3000 -ErrorAction SilentlyContinue
```

---

## 10. Dépannage

### 10.1 Erreur `JAVA_HOME is not set`

**Symptôme** :

```
Exception in thread "main" java.lang.RuntimeException: JAVA_HOME is not set
```

**Cause** : Java n'est pas installé dans le conteneur Airflow (utilisé par `SparkSubmitOperator`).

**Solution** : Vérifier que `Dockerfile.airflow` contient l'installation d'OpenJDK 17 :

```dockerfile
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Puis rebuilder :

```powershell
docker compose up -d --build airflow-webserver airflow-scheduler
```

---

### 10.2 Erreur `PYTHONPATH` manquant

**Symptôme** :

```
ModuleNotFoundError: No module named 'src'
```

**Cause** : Le module `src` n'est pas trouvable par Python lors de l'exécution de spark-submit.

**Solution** : Ajouter le flag PYTHONPATH à chaque spark-submit :

```bash
PYTHONPATH=/opt/spark/work-dir \
/opt/spark/bin/spark-submit \
--conf spark.driverEnv.PYTHONPATH=/opt/spark/work-dir \
--conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
src/transformations/spark/faculty_profiles_etl.py
```

---

### 10.3 Erreur `ModuleNotFoundError: No module named 'minio'`

**Symptôme** :

```
ModuleNotFoundError: No module named 'minio'
```

**Cause** : La bibliothèque Python `minio` n'est pas installée dans le conteneur.

**Solution** : Installer dans le conteneur :

```bash
pip install minio --quiet
```

Ou ajouter à `requirements.txt` du projet et rebuilder l'image.

---

### 10.4 Erreur `localhost:9200 inaccessible depuis Spark`

**Symptôme** :

```
ConnectionRefusedError: [Errno 111] Connection refused
```

**Cause** : Depuis l'intérieur d'un conteneur Docker, `localhost` ne pointe pas vers la machine hôte.

**Solution** : Utiliser le nom Docker du service :

```python
# Incorrect
es = Elasticsearch("http://localhost:9200")

# Correct
es = Elasticsearch("http://university-elasticsearch:9200")
```

**Vérification** :

```bash
# Depuis le conteneur spark-master
curl http://university-elasticsearch:9200
```

---

### 10.5 Erreur Elasticsearch version 9 vs 8 (compatibility headers)

**Symptôme** :

```
elastic_indexer_elastic.py: elastic.ElasticsearchWarning: ...
```

Ou erreurs de type inattendu lors de l'indexation.

**Cause** : La version du client Python `elasticsearch` est incompatible avec le serveur Elasticsearch 8.

**Solution** :

```bash
pip uninstall elasticsearch -y
pip install elasticsearch==8.11.1
```

---

### 10.6 Spark Thrift occupant tous les executors

**Symptôme** : Les scripts ETL ne s'exécutent pas ou échouent avec un timeout.

**Cause** : Spark Thrift Server consomme toutes les ressources disponibles, laissant 0 executor aux autres jobs.

**Solution** : Arrêter Spark Thrift avant l'indexation, le relancer après :

```bash
# Arrêter Spark Thrift
docker stop university-spark-thrift

# Exécuter les ETL et l'indexation
# ...

# Relancer Spark Thrift
docker start university-spark-thrift
```

---

### 10.7 Liquibase corrompt la base Hive

**Symptôme** :

```
ERROR: relation "completed_txn_components" already exists
```

**Cause** : Metabase utilise la même base PostgreSQL que Hive Metastore, et Liquibase (gestionnaire de schéma de Metabase) crée des tables qui entrent en conflit.

**Solution** : Séparer les bases PostgreSQL. Modifier `docker-compose.yml` :

1. Ajouter un service `metabase-postgres` dédié
2. Faire pointer `metabase` vers `metabase-postgres`
3. Hive continue d'utiliser `postgres`

```yaml
metabase-postgres:
  image: postgres:13
  environment:
    POSTGRES_DB: metabase
    POSTGRES_USER: metabase
    POSTGRES_PASSWORD: metabase
  ports:
    - "5434:5432"
  volumes:
    - metabase_postgres_data:/var/lib/postgresql/data
```

---

### 10.8 `Permission denied` lors du `pip install`

**Symptôme** :

```
ERROR: Could not install packages due to an EnvironmentError: [Errno 13] Permission denied
```

**Solution** :

```bash
# Utiliser --user ou sudo
sudo pip install <package>
```

Ou plus proprement, ajouter le package au `requirements.txt` et rebuilder l'image Docker.

---

### 10.9 Table Hudi vide après l'ETL

**Symptôme** : `SELECT COUNT(*) FROM ...` retourne 0.

**Vérifications** :

1. Vérifier que les données brutes existent dans MinIO :
   ```bash
   docker exec -it minio bash
   mc ls local/raw-json/
   ```
2. Vérifier les logs Spark pour les erreurs silencieuses
3. Vérifier que le bucket `hudi` existe dans MinIO :
   ```bash
   mc ls local/hudi/
   ```

---

### 10.10 Elasticsearch jaune (yellow status)

**Symptôme** : `health status` = `yellow` au lieu de `green`.

**Cause** : Le cluster a 1 seul nœud mais la réplication est configurée sur 1 replica.

**Solution** : C'est normal en mode single-node. Pour corriger :

```powershell
curl -X PUT "http://localhost:9200/_all/_settings" -H "Content-Type: application/json" -d '{"number_of_replicas": 0}'
```

---

### 10.11 Container ne démarre pas

**Diagnostic** :

```powershell
docker ps -a --filter "status=exited"
docker logs <container_name> --tail=50
```

**Causes fréquentes** :

- Port déjà utilisé (vérifier avec `netstat -ano | findstr :<port>`)
- Volume corrompu (`docker volume prune`)
- Image non reconstruite (`docker compose up -d --build`)

---


