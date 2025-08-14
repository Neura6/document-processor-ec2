#!/bin/bash

# EC2 Final Deployment Commands - Run these exactly as shown

echo "🚀 PDF Processing Monitoring - Final Deployment"
echo "=============================================="

# Navigate to correct directory
echo "📍 Current directory: $(pwd)"
echo "📁 Contents: $(ls -la)"

# Fix permissions
echo "🔧 Fixing permissions..."
chmod +x verify-setup-fixed.sh

# Deploy monitoring stack
echo "🐳 Starting Docker monitoring..."
cd docker
docker-compose up -d

# Wait for services to start
echo "⏳ Waiting for services to start..."
sleep 10

# Check what's running
echo "📊 Service status:"
docker-compose ps

# Get public IP
PUBLIC_IP=$(curl -s https://checkip.amazonaws.com)
echo "🌐 Public IP: $PUBLIC_IP"

# Check service accessibility
echo "🔗 Service URLs:"
echo "  Grafana: http://$PUBLIC_IP:3000"
echo "  Prometheus: http://$PUBLIC_IP:9090"
echo "  Node Exporter: http://$PUBLIC_IP:9100"

# Quick health check
echo "🔍 Quick health check..."
if curl -s http://localhost:3000/api/health | grep -q "ok"; then
    echo "✅ Grafana is responding"
else
    echo "❌ Grafana check failed"
fi

if curl -s http://localhost:9090/api/v1/query?query=up | grep -q "success"; then
    echo "✅ Prometheus is responding"
else
    echo "❌ Prometheus check failed"
fi

echo ""
echo "🎉 Deployment complete!"
echo "📊 Access Grafana at: http://$PUBLIC_IP:3000"
echo "🔑 Login: admin/admin123"
