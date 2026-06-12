# Changelog / TODO

- [x] Fix MinIO endpoint configuration so Airflow container can reach MinIO (was hardcoded to localhost:9001).
- [x] Update `src/storage/minio/chaimae_client.py` to read `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_SECURE` from env (docker-compose defaults).
- [ ] Re-run `docker exec airflow-webserver airflow tasks test chaimae_pipeline openalex_to_minio 2026-06-12`.
- [ ] If still failing, ensure env vars are wired into `airflow-webserver` in `docker-compose.yml` and restart.


