#!/bin/bash

# Script to fix Docker ContainerConfig KeyError issues
echo "🐳 Fixing Docker ContainerConfig issues..."

# Stop all running containers
echo "Stopping all running containers..."
docker stop $(docker ps -aq) 2>/dev/null || true

# Remove all containers
echo "Removing all containers..."
docker rm $(docker ps -aq) 2>/dev/null || true

# Remove all volumes (this will fix the ContainerConfig issue)
echo "Removing all volumes..."
docker volume prune -f

# Remove all images related to this project
echo "Removing project images..."
docker rmi pdf-processor_pdf-processor 2>/dev/null || true
docker rmi prom/prometheus:latest 2>/dev/null || true
docker rmi grafana/grafana:latest 2>/dev/null || true

# Clean up Docker system
echo "Cleaning Docker system..."
docker system prune -f

# Rebuild and start services
echo "Rebuilding services..."
docker-compose build --no-cache
docker-compose up -d

echo "✅ Docker issues fixed! Services should be starting now..."
echo "Check status with: docker-compose ps"
