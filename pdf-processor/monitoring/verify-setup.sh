#!/bin/bash

# PDF Processing Monitoring Verification Script
# Run this on EC2 to verify Prometheus and Grafana setup

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}🔍 PDF Processing Monitoring Verification${NC}"
echo "=========================================="

# Check if Docker is running
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed${NC}"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo -e "${RED}❌ Docker is not running${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker is running${NC}"

# Check if monitoring directory exists
if [ ! -d "/home/ubuntu/pdf-processor/monitoring" ]; then
    echo -e "${RED}❌ Monitoring directory not found${NC}"
    exit 1
fi

cd /home/ubuntu/pdf-processor/monitoring

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}❌ docker-compose.yml not found${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Configuration files found${NC}"

# Start monitoring services
echo -e "${YELLOW}🚀 Starting monitoring services...${NC}"
docker-compose up -d

# Wait for services to start
sleep 10

# Check service status
echo -e "${YELLOW}📊 Checking service status...${NC}"
services=$(docker-compose ps --services)
for service in $services; do
    if docker-compose ps $service | grep -q "Up"; then
        echo -e "${GREEN}✅ $service is running${NC}"
    else
        echo -e "${RED}❌ $service is not running${NC}"
    fi
done

# Check Prometheus
PROMETHEUS_URL="http://localhost:9090"
if curl -s "$PROMETHEUS_URL/api/v1/query?query=up" | grep -q '"status":"success"'; then
    echo -e "${GREEN}✅ Prometheus is responding${NC}"
    echo -e "${YELLOW}📍 Prometheus: $PROMETHEUS_URL${NC}"
else
    echo -e "${RED}❌ Prometheus is not responding${NC}"
fi

# Check Grafana
GRAFANA_URL="http://localhost:3000"
if curl -s -o /dev/null -w "%{http_code}" "$GRAFANA_URL/api/health" | grep -q "200"; then
    echo -e "${GREEN}✅ Grafana is responding${NC}"
    echo -e "${YELLOW}📍 Grafana: $GRAFANA_URL (admin/admin123)${NC}"
else
    echo -e "${RED}❌ Grafana is not responding${NC}"
fi

# Check Node Exporter
NODE_EXPORTER_URL="http://localhost:9100/metrics"
if curl -s "$NODE_EXPORTER_URL" | grep -q "node_"; then
    echo -e "${GREEN}✅ Node Exporter is responding${NC}"
    echo -e "${YELLOW}📍 Node Exporter: $NODE_EXPORTER_URL${NC}"
else
    echo -e "${RED}❌ Node Exporter is not responding${NC}"
fi

# Check Prometheus targets
PROMETHEUS_TARGETS="$PROMETHEUS_URL/targets"
echo -e "${YELLOW}📍 Check Prometheus targets: $PROMETHEUS_TARGETS${NC}"

# Get public IP for external access
PUBLIC_IP=$(curl -s https://checkip.amazonaws.com || curl -s https://api.ipify.org)
echo -e "${GREEN}🌐 Public URLs:${NC}"
echo -e "${YELLOW}  Prometheus: http://$PUBLIC_IP:9090${NC}"
echo -e "${YELLOW}  Grafana: http://$PUBLIC_IP:3000${NC}"
echo -e "${YELLOW}  Node Exporter: http://$PUBLIC_IP:9100${NC}"

echo ""
echo -e "${GREEN}🎉 Monitoring verification complete!${NC}"
echo -e "${YELLOW}📊 Access Grafana dashboard at: http://$PUBLIC_IP:3000${NC}"
