#!/bin/bash

echo "========================================="
echo "Checking University Data Platform Services"
echo "========================================="
echo ""

# Check Docker services
echo "📦 Docker Services Status:"
docker-compose ps

echo ""
echo "🌐 Service URLs:"
echo "  - MinIO Console:      http://localhost:9001 (minioadmin/minioadmin)"
echo "  - Spark Master UI:    http://localhost:8080"
echo "  - Airflow UI:         http://localhost:8082 (admin/admin)"
echo "  - Metabase:           http://localhost:3000"
echo "  - Elasticsearch:      http://localhost:9200"
echo "  - Kibana:             http://localhost:5601"
echo ""

echo "🔍 Testing Connections:"

# Test MinIO
if curl -s http://localhost:9000/minio/health/ready > /dev/null; then
    echo "  ✓ MinIO is running"
else
    echo "  ✗ MinIO is not running"
fi

# Test Elasticsearch
if curl -s http://localhost:9200 > /dev/null; then
    echo "  ✓ Elasticsearch is running"
else
    echo "  ✗ Elasticsearch is not running"
fi

# Test PostgreSQL
if docker exec university-postgres pg_isready -U hive > /dev/null 2>&1; then
    echo "  ✓ PostgreSQL is running"
else
    echo "  ✗ PostgreSQL is not running"
fi

echo ""
echo "📊 Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
