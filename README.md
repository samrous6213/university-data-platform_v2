# University Data Platform V2

This repo contains an Airflow/Spark/MinIO/Hudi data platform with **7 parallel workspaces** (one per team member).

At the root you will find the orchestration stack (Docker Compose) and helper scripts, and each workspace contains its own:
- `dags/` (Airflow DAGs)
- `data/raw/` and `data/processed/`
- `notebooks/`
- `spark_jobs/`

---

## Tech Stack (via `docker-compose.yml`)
- **MinIO** (object storage) at `http://localhost:9001`
- **Airflow** (webserver/scheduler) at `http://localhost:8082`
- **PostgreSQL** (metastore / dashboard storage)
- **Metabase** at `http://localhost:3000`
- **Elasticsearch** at `http://localhost:9200`
- **Kibana** at `http://localhost:5601` (optional profile)

---

## Services Setup

### 1) Start everything
```bash
docker-compose up -d
```

### 2) Check services
```bash
./check_services.sh
```

### 3) Access UIs
- MinIO Console: http://localhost:9001  (minioadmin/minioadmin)
- Airflow UI:      http://localhost:8082 (admin/admin)
- Metabase:       http://localhost:3000
- Elasticsearch:  http://localhost:9200
- Kibana (opt):   http://localhost:5601

---

## Repo Structure

```text
university-data-platform_v2/
├── docker-compose.yml
├── check_services.sh
├── README.md
├── TEAM_WORK_PLAN.md
├── .gitignore
├── ayoub_workspace/
│   ├── shared_library.py
│   ├── dags/
│   ├── data/raw/
│   ├── data/processed/
│   ├── notebooks/
│   └── spark_jobs/
├── chaimaa_workspace/
│   └── ... (same structure)
├── fahd_workspace/
│   └── ... (same structure)
├── hiba_workspace/
│   └── ... (same structure)
├── nezha_workspace/
│   └── ... (same structure)
├── safaa_workspace/
│   └── ... (same structure)
└── sara_workspace/
    └── ... (same structure)
```

---

## Airflow DAGs
Each workspace defines its own DAG(s) under `*/workspace/dags/`.

These DAGs are expected to use the shared utilities found in the same workspace via `shared_library.py`.

---

## Shared Utilities
Inside each workspace there is a `shared_library.py` containing common helpers, for example:
- MinIO client creation (`get_minio_client`)
- Spark session builder with Hudi extensions (`get_spark_session`)
- Standardized record formatting and MinIO upload helpers

---

## Git Workflow
1. Pull before working: `git pull origin main`
2. You don't need to create your branch: already created 
3. Modify your own workspace only
4. Push and open PR: `git push origin <name>-update`

---

## Helpful Commands
```bash
# Docker logs
docker-compose logs -f airflow-webserver

# List running containers
docker-compose ps
```

