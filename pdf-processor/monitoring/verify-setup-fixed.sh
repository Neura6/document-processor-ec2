#!/bin/bash

# PDF Processing Monitoring Verification Script - Fixed Version
# Run this from the monitoring directory

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

# Check current directory and navigate to docker if needed
CURRENT_DIR=$(pwd)
if [[ "$CURRENT_DIR" == *"monitoring"* ]]; then
    if [ -d "docker" ]; then
        cd docker
        echo -e "${GREEN}✅ Changed to docker directory${NC}"
    fi
fi

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}❌ docker-compose.yml not found${NC}"
    echo -e "${YELLOW}Current directory: $(pwd)${NC}"
    echo -e "${YELLOW}Contents: $(ls -la)${NC}"
    exit 1
fi

echo -e "${GREEN}✅ docker-compose.yml found${NC}"

# Check configuration files
echo -e "${YELLOW}📋 Checking configuration files...${NC}"

# Check Prometheus config
if [ -f "../config/prometheus/prometheus.yml" ]; then
    echo -e "${GREEN}✅ Prometheus config found${NC}"
else
    echo -e "${RED}❌ Prometheus config not found${NC}"
fi

# Check Grafana configs
if [ -f "../config/grafana/provisioning/datasources/prometheus.yml" ]; then
    echo -e "${GREEN}✅ Grafana datasource config found${NC}"
else
    echo -e "${RED}❌ Grafana datasource config not found${NC}"
fi

if [ -f "../config/grafana/provisioning/dashboards/dashboard.yml" ]; then
    echo -e "${GREEN}✅ Grafana dashboard config found${NC}"
else
    echo -e "${RED}❌ Grafana dashboard config not found${NC}"
fi

# Start monitoring services
echo -e "${YELLOW}🚀 Starting monitoring services...${NC}"
docker-compose up -d

# Wait for services to start
sleep 15

# Check service status
echo -e "${YELLOW}📊 Checking service status...${NC}"
services=$(docker-compose ps --services 2>/dev/null || echo "prometheus grafana node-exporter")

for service in $services; do
    if docker-compose ps | grep -q "$service.*Up"; then
        echo -e "${GREEN}✅ $service is running${NC}"
    else
        echo -e "${RED}❌ $service is not running${NC}"
    fi
done

# Get public IP
PUBLIC_IP=$(curl -s https://checkip.amazonaws.com || curl -s https://api.ipify.org 2>/dev/null || echo "localhost")

echo ""
echo -e "${GREEN}🌐 Access URLs:${NC}"
echo -e "${YELLOW}  Prometheus: http://$PUBLIC_IP:9090${NC}"
echo -e "${YELLOW}  Grafana: http://$PUBLIC_IP:3000${NC}"
echo -e "${YELLOW}  Node Exporter: http://$PUBLIC_IP:9100${NC}"

echo ""
echo -e "${GREEN}🎉 Verification complete!${NC}"
echo -e "${YELLOW}📊 Check Grafana dashboard at: http://$PUBLIC_IP:3000${NC}"
echo -e "${YELLOW}📝 Login: admin/admin123${NC}"
