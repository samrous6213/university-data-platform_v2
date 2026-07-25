#!/bin/bash
set -e

HIVE_CONF_DIR="${HIVE_CONF_DIR:-/opt/hive/conf}"

# ------------------------------------------------------------------
# Generate hive-site.xml from HIVE_SITE_CONF_* environment variables
# ------------------------------------------------------------------
HIVE_SITE_FILE="${HIVE_CONF_DIR}/hive-site.xml"

cat > "${HIVE_SITE_FILE}" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
EOF

# Override S3A credential provider to use env vars (avoids HiveConf clone issue)
export HIVE_SITE_CONF_fs_s3a_aws_credentials_provider="com.amazonaws.auth.EnvironmentVariableCredentialsProvider"

for var in $(env | grep -E "^HIVE_SITE_CONF_" | sort); do
    name="${var%%=*}"
    value="${var#*=}"
    prop_name=$(echo "$name" | sed 's/^HIVE_SITE_CONF_//' | tr '_' '.')
    echo "  <property>" >> "${HIVE_SITE_FILE}"
    echo "    <name>${prop_name}</name>" >> "${HIVE_SITE_FILE}"
    echo "    <value>${value}</value>" >> "${HIVE_SITE_FILE}"
    echo "  </property>" >> "${HIVE_SITE_FILE}"
done

echo "</configuration>" >> "${HIVE_SITE_FILE}"

echo "Generated hive-site.xml with $(grep -c '<property>' "${HIVE_SITE_FILE}" || true) properties."

# ------------------------------------------------------------------
# Generate core-site.xml from S3A_* environment variables
# ------------------------------------------------------------------
CORE_SITE_FILE="/opt/hadoop/etc/hadoop/core-site.xml"
envsubst < /opt/hadoop/etc/hadoop/core-site.xml > /tmp/core-site.xml
cp /tmp/core-site.xml "${CORE_SITE_FILE}"
echo "Generated core-site.xml with S3A configuration at ${CORE_SITE_FILE}."

# ------------------------------------------------------------------
# Metastore detection
# If a remote Thrift metastore is configured, skip local Derby init.
# ------------------------------------------------------------------
METASTORE_URIS=$(grep -A1 'hive.metastore.uris' "${HIVE_SITE_FILE}" 2>/dev/null | grep '<value>' | sed 's|.*<value>\(.*\)</value>.*|\1|')

if echo "${METASTORE_URIS}" | grep -q "^thrift://"; then
    echo "Remote metastore detected: ${METASTORE_URIS}. Skipping local Derby schema init."
else
    echo "No remote metastore configured. Checking local Derby schema..."
    if ! schematool -dbType derby -info > /dev/null 2>&1; then
        echo "Initializing Derby schema..."
        schematool -dbType derby -initSchema
        echo "Schema initialized."
    else
        echo "Derby schema already initialized. Skipping init."
    fi
fi

# ------------------------------------------------------------------
# Hadoop & Hive classpath exports required for S3A
# ------------------------------------------------------------------
export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}"
export HIVE_AUX_JARS_PATH="${HIVE_AUX_JARS_PATH:-/opt/hive/auxlib}"

# ------------------------------------------------------------------
# Start HiveServer2
# ------------------------------------------------------------------
mkdir -p /tmp/hive && chmod 777 /tmp/hive
echo "Starting HiveServer2..."
exec hive --service hiveserver2
