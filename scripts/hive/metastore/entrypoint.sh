#!/bin/bash
set -e

set -a
METASTORE_DB_HOSTNAME=${METASTORE_DB_HOSTNAME:-postgres}
METASTORE_DB_PORT=${METASTORE_DB_PORT:-5432}
METASTORE_DB_NAME=${METASTORE_DB_NAME:-metastore}
METASTORE_DB_USER=${METASTORE_DB_USER:-hive}
METASTORE_DB_PASSWORD=${METASTORE_DB_PASSWORD:-hive}
METASTORE_WAREHOUSE_DIR=${METASTORE_WAREHOUSE_DIR:-/user/hive/warehouse}
HIVE_METASTORE_PORT=${HIVE_METASTORE_PORT:-9083}
set +a

envsubst < /opt/hive/conf/hive-site.xml > /tmp/hive-site.xml
mv /tmp/hive-site.xml /opt/hive/conf/hive-site.xml

envsubst < /opt/hadoop/etc/hadoop/core-site.xml > /tmp/core-site.xml
mv /tmp/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

echo "Waiting for PostgreSQL at ${METASTORE_DB_HOSTNAME}:${METASTORE_DB_PORT}..."
for i in $(seq 1 30); do
    if nc -z "${METASTORE_DB_HOSTNAME}" "${METASTORE_DB_PORT}" 2>/dev/null; then
        echo "PostgreSQL is ready."
        break
    fi
    echo "Attempt $i: not ready yet..."
    sleep 2
done

echo "Checking Hive Metastore schema..."
if ! schematool -dbType postgres -info > /dev/null 2>&1; then
    echo "Initializing Hive Metastore schema..."
    schematool -dbType postgres -initSchema
    echo "Schema initialized."
else
    echo "Schema already initialized."
fi

echo "Starting Hive Metastore on port ${HIVE_METASTORE_PORT}..."
mkdir -p /tmp/hive && chmod 777 /tmp/hive
export HADOOP_OPTS="-Dhive.metastore.port=${HIVE_METASTORE_PORT}"
exec hive --service metastore -p "${HIVE_METASTORE_PORT}"
