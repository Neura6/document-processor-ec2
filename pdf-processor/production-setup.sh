#!/bin/bash

# Complete EC2 Production Setup Script
# This sets up both monitoring and SQS worker for full production use

set -e

echo "🚀 Setting up complete EC2 production environment..."

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')] $1${NC}"
}

# 1. Start monitoring stack
log "Starting monitoring stack..."
cd /opt/monitoring/docker
sudo docker compose up -d

# 2. Create SQS worker systemd service
log "Creating SQS worker service..."
sudo tee /etc/systemd/system/pdf-processor-worker.service > /dev/null << 'EOF'
[Unit]
Description=PDF Processor SQS Worker
After=network.target docker.service
Requires=docker.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/pdf-processor
Environment=AWS_REGION=us-east-1
Environment=PYTHONPATH=/home/ubuntu/pdf-processor
ExecStart=/usr/bin/python3 /home/ubuntu/pdf-processor/services/sqs_worker_metrics.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# 3. Create metrics collection service
log "Creating metrics collection service..."
sudo tee /etc/systemd/system/pdf-metrics.service > /dev/null << 'EOF'
[Unit]
Description=PDF Processing Metrics Collection
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/pdf-processor
Environment=PYTHONPATH=/home/ubuntu/pdf-processor
ExecStart=/usr/bin/python3 /home/ubuntu/pdf-processor/monitor-system.py
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF

# 4. Create production status script
log "Creating production status script..."
sudo tee /usr/local/bin/production-status > /dev/null << 'EOF'
#!/bin/bash
echo "=== PDF Processing Production Status ==="
echo ""

# Check monitoring
echo "📊 Monitoring Stack:"
cd /opt/monitoring/docker
sudo docker compose ps

echo ""
echo "🔄 SQS Worker:"
sudo systemctl is-active pdf-processor-worker

echo ""
echo "📈 Metrics Collection:"
sudo systemctl is-active pdf-metrics

echo ""
echo "🔗 Access URLs:"
PUBLIC_IP=$(curl -s https://checkip.amazonaws.com || curl -s https://api.ipify.org)
echo "  Grafana: http://$PUBLIC_IP:3000"
echo "  Prometheus: http://$PUBLIC_IP:9090"

echo ""
echo "📋 Service Commands:"
echo "  Start all: sudo systemctl start pdf-processor-worker pdf-metrics"
echo "  Stop all: sudo systemctl stop pdf-processor-worker pdf-metrics"
echo "  Logs: sudo journalctl -u pdf-processor-worker -f"
echo "  Status: production-status"
EOF

# 5. Create production start script
sudo tee /usr/local/bin/start-production > /dev/null << 'EOF'
#!/bin/bash
echo "🚀 Starting production environment..."

# Start monitoring
cd /opt/monitoring/docker
sudo docker compose up -d

# Start services
sudo systemctl daemon-reload
sudo systemctl enable --now pdf-processor-worker pdf-metrics

echo "✅ Production environment started!"
echo "Run 'production-status' to check everything"
EOF

# 6. Make scripts executable
sudo chmod +x /usr/local/bin/production-status /usr/local/bin/start-production

# 7. Reload systemd and start services
sudo systemctl daemon-reload
sudo systemctl enable --now pdf-processor-worker pdf-metrics

# 8. Create log rotation
sudo tee /etc/logrotate.d/pdf-processor > /dev/null << 'EOF'
/home/ubuntu/pdf-processor/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0644 ubuntu ubuntu
}
EOF

log "✅ Production setup complete!"
echo ""
echo "=== Production Commands ==="
echo "Start everything: start-production"
echo "Check status: production-status"
echo "View logs: sudo journalctl -u pdf-processor-worker -f"
echo ""
echo "=== Access Points ==="
echo "Grafana: http://$(curl -s https://checkip.amazonaws.com):3000"
echo "Prometheus: http://$(curl -s https://checkip.amazonaws.com):9090"
