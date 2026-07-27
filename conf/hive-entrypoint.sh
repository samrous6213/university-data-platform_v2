#!/bin/bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════════
#  Hive Metastore entrypoint
#
#  Lifecycle:  1. Wait for PostgreSQL
#              2. Initialize / upgrade schema (idempotent)
#              3. Start Hive Metastore Server on METASTORE_PORT
#
#  All configuration is passed via environment variables
#  (see docker-compose.yml for the full list).
# ═══════════════════════════════════════════════════════════════════

DB_HOST="${METASTORE_DB_HOST:-postgres}"
DB_PORT="${METASTORE_DB_PORT:-5432}"
DB_NAME="${METASTORE_DB_NAME:-metastore}"
DB_USER="${METASTORE_DB_USER:-hive}"
DB_PASSWORD="${METASTORE_DB_PASSWORD:-hive}"
MS_PORT="${METASTORE_PORT:-9083}"

JDBC_URL="jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}"

# ─────────────────────────────────────────────────────────────────
#  1. Wait for PostgreSQL to be ready
# ─────────────────────────────────────────────────────────────────
echo "[hive-metastore] Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."
while ! (echo > /dev/tcp/"${DB_HOST}"/"${DB_PORT}") 2>/dev/null; do
  sleep 2
done
echo "[hive-metastore] PostgreSQL is ready."

# ─────────────────────────────────────────────────────────────────
#  2. Build Hadoop CLIENT_OPTS with JDBC + S3A configuration
# ─────────────────────────────────────────────────────────────────
#    These -D properties override any hive-site.xml / core-site.xml
#    values.  schematool, Hive, and Hadoop all inherit
#    HADOOP_CLIENT_OPTS, so the config reaches every process.
# ─────────────────────────────────────────────────────────────────
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx1G"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Djavax.jdo.option.ConnectionURL=${JDBC_URL}"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Djavax.jdo.option.ConnectionUserName=${DB_USER}"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Djavax.jdo.option.ConnectionPassword=${DB_PASSWORD}"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Dhive.metastore.warehouse.dir=s3a://hudi/warehouse"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Dhive.metastore.schema.evolution=true"

# ─────────────────────────────────────────────────────────────────
#  S3A / MinIO filesystem configuration
# ─────────────────────────────────────────────────────────────────
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Dfs.s3a.endpoint=http://university-minio:9000"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Dfs.s3a.access.key=minioadmin"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Dfs.s3a.secret.key=minioadmin"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Dfs.s3a.path.style.access=true"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Dfs.s3a.connection.ssl.enabled=false"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Dfs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"

# ─────────────────────────────────────────────────────────────────
#  3. Initialize or upgrade Hive Metastore schema
#     - initOrUpgradeSchema is idempotent:
#         * no schema  → CREATE
#         * old schema → UPGRADE
#         * up-to-date → NO-OP
# ─────────────────────────────────────────────────────────────────
echo "[hive-metastore] Initializing Hive Metastore schema (dbType=postgres)..."
"${HIVE_HOME}/bin/schematool" -dbType postgres -initOrUpgradeSchema
echo "[hive-metastore] Schema initialization completed."

# ─────────────────────────────────────────────────────────────────
#  4. Start Hive Metastore Server
#     exec replaces the shell so the process receives SIGTERM
# ─────────────────────────────────────────────────────────────────
echo "[hive-metastore] Starting Hive Metastore Server on port ${MS_PORT}..."
exec "${HIVE_HOME}/bin/hive" \
  --skiphadoopversion \
  --skiphbasecp \
  --service metastore
