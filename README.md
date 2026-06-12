# University Data Platform

## 1. Cloner le projet

```bash
git clone <repository-url>
cd university-data-platform
```

---

## 2. Démarrer Docker Desktop

Vérifier que Docker est lancé :

```bash
docker ps
```

---

## 3. Lancer l'infrastructure

```bash
docker compose up -d
```

Vérifier les conteneurs :

```bash
docker ps
```

---

## 4. Accès aux services

### MinIO

```text
http://localhost:9001
```

```text
Username: minioadmin
Password: minioadmin
```

### Spark

```text
http://localhost:8080
```

### Metabase

```text
http://localhost:3000
```

### Airflow

```text
http://localhost:8081
```

```text
Username: admin
Password: admin
```

### Elasticsearch

```text
http://localhost:9200
```

---

## 5. Exécuter l'ingestion OpenAlex

```bash
python -m src.ingestion.apis.openalex
```

---

## 6. Exécuter le Web Scraping UCA

```bash
python -m src.ingestion.scrapers.uca
```

---

## 7. Vérifier les données dans MinIO

1. Ouvrir MinIO
2. Aller dans le bucket raw
3. Vérifier les fichiers JSON

---

## 8. Exécuter les transformations Spark

```bash
spark-submit src/transformations/faculty_profiles.py
```

```bash
spark-submit src/transformations/course_catalog.py
```

---

## 9. Vérifier Elasticsearch

```bash
curl http://localhost:9200
```

---

## 10. Vérifier PostgreSQL

```bash
docker exec -it university-postgres psql -U hive -d metastore
```

Puis :

```sql
SELECT version();
```

---

## 11. Consulter les logs

### Airflow

```bash
docker logs airflow-webserver
```

### Spark

```bash
docker logs spark-master
docker logs spark-worker
```

### Hive

```bash
docker logs hive-metastore
```

### MinIO

```bash
docker logs university-minio
```

---

## 12. Arrêter la plateforme

```bash
docker compose down
```

---

## Architecture

```text
OpenAlex API
UCA Website
        ↓
      MinIO
   (Raw Data)
        ↓
      Spark
(Transformations)
        ↓
      Hudi
 (Curated Data)
        ↓
      Hive
    (SQL)
        ↓
    Metabase

        ↓
 Elasticsearch

        ↓
    Airflow
```
