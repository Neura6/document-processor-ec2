#!/bin/bash
# PDF Processing Monitoring Setup Script for EC2

set -e

echo "🚀 Starting PDF Processing Monitoring Setup..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   print_error "This script should not be run as root"
   exit 1
fi

# Update system
print_status "Updating system packages..."
sudo yum update -y

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    print_status "Installing Docker..."
    sudo amazon-linux-extras install docker -y
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo usermod -a -G docker $USER
    print_warning "Please log out and log back in for Docker group changes to take effect"
fi

# Install Docker Compose if not present
if ! command -v docker-compose &> /dev/null; then
    print_status "Installing Docker Compose..."
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
fi

# Create monitoring directories
print_status "Creating monitoring directories..."
sudo mkdir -p /opt/monitoring/{data,config,scripts}
sudo mkdir -p /opt/monitoring/data/{prometheus,grafana}
sudo mkdir -p /opt/monitoring/config/{prometheus,grafana/provisioning/{dashboards,datasources}}

# Set permissions
sudo chown -R $USER:$USER /opt/monitoring
sudo chmod -R 755 /opt/monitoring

# Create storage auto-scaling script
print_status "Creating storage auto-scaling script..."
sudo tee /opt/monitoring/scripts/auto-resize.sh > /dev/null << 'EOF'
#!/bin/bash
# Storage auto-resizing script

# Check disk usage
USAGE=$(df /opt/monitoring | tail -1 | awk '{print $5}' | sed 's/%//')
THRESHOLD=85

if [ $USAGE -gt $THRESHOLD ]; then
    echo "$(date): Disk usage at ${USAGE}%, triggering cleanup"
    
    # Clean Docker system
    docker system prune -f
    
    # Clean Prometheus old data (keep last 15 days)
    docker exec prometheus sh -c "find /prometheus -name '*.tmp' -mtime +7 -delete"
    
    # Log cleanup
    echo "$(date): Storage cleanup completed"
fi
EOF

sudo chmod +x /opt/monitoring/scripts/auto-resize.sh

# Create storage monitor service
print_status "Creating storage monitor service..."
sudo tee /etc/systemd/system/storage-monitor.service > /dev/null << 'EOF'
[Unit]
Description=Storage Auto-Scaling Monitor
After=docker.service

[Service]
Type=simple
ExecStart=/opt/monitoring/scripts/auto-resize.sh
Restart=always
RestartSec=300
User=ec2-user

[Install]
WantedBy=multi-user.target
EOF

# Create storage monitor timer
sudo tee /etc/systemd/system/storage-monitor.timer > /dev/null << 'EOF'
[Unit]
Description=Run storage monitor every 5 minutes
Requires=storage-monitor.service

[Timer]
OnCalendar=*:0/5
Persistent=true

[Install]
WantedBy=timers.target
EOF

# Create monitoring dashboard import script
print_status "Creating dashboard import script..."
sudo tee /opt/monitoring/scripts/import-dashboards.sh > /dev/null << 'EOF'
#!/bin/bash
# Import Grafana dashboards

GRAFANA_URL="http://localhost:3000"
GRAFANA_USER="admin"
GRAFANA_PASS="admin123"

# Wait for Grafana to be ready
echo "Waiting for Grafana to be ready..."
until curl -s "$GRAFANA_URL/api/health" > /dev/null; do
    sleep 5
done

# Import dashboards
for dashboard_file in /opt/monitoring/config/grafana/dashboards/*.json; do
    if [ -f "$dashboard_file" ]; then
        dashboard_name=$(basename "$dashboard_file" .json)
        echo "Importing dashboard: $dashboard_name"
        
        curl -X POST \
            -H "Content-Type: application/json" \
            -u "$GRAFANA_USER:$GRAFANA_PASS" \
            "$GRAFANA_URL/api/dashboards/db" \
            -d "@$dashboard_file"
    fi
done

echo "Dashboards imported successfully"
EOF

sudo chmod +x /opt/monitoring/scripts/import-dashboards.sh

# Create monitoring start script
print_status "Creating monitoring start script..."
sudo tee /opt/monitoring/start-monitoring.sh > /dev/null << 'EOF'
#!/bin/bash
# Start monitoring services

echo "🚀 Starting PDF Processing Monitoring..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Create necessary directories
mkdir -p /opt/monitoring/data/{prometheus,grafana}

# Copy configuration files
cp -r ./config/* /opt/monitoring/config/

# Start services
cd /opt/monitoring
docker-compose up -d

# Wait for services to start
echo "⏳ Waiting for services to start..."
sleep 30

# Check service health
services=("prometheus" "grafana")
for service in "${services[@]}"; do
    if docker-compose ps | grep -q "$service.*Up"; then
        echo "✅ $service is running"
    else
        echo "❌ $service failed to start"
    fi
done

# Import dashboards
echo "📊 Importing dashboards..."
/opt/monitoring/scripts/import-dashboards.sh

echo "🎉 Monitoring setup complete!"
echo "📊 Grafana: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):3000"
echo "🔍 Prometheus: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):9090"
echo ""
echo "📋 Default credentials:"
echo "   Username: admin"
echo "   Password: admin123"
EOF

sudo chmod +x /opt/monitoring/start-monitoring.sh

# Create monitoring stop script
print_status "Creating monitoring stop script..."
sudo tee /opt/monitoring/stop-monitoring.sh > /dev/null << 'EOF'
#!/bin/bash
# Stop monitoring services

echo "🛑 Stopping PDF Processing Monitoring..."

cd /opt/monitoring
docker-compose down

echo "✅ Monitoring services stopped"
EOF

sudo chmod +x /opt/monitoring/stop-monitoring.sh

# Enable and start services
print_status "Enabling services..."
sudo systemctl daemon-reload
sudo systemctl enable storage-monitor.timer
sudo systemctl start storage-monitor.timer

# Create symlink for easy access
sudo ln -sf /opt/monitoring/start-monitoring.sh /usr/local/bin/start-monitoring
sudo ln -sf /opt/monitoring/stop-monitoring.sh /usr/local/bin/stop-monitoring

print_status "✅ Monitoring setup complete!"
print_status "Next steps:"
print_status "1. Log out and log back in for Docker permissions"
print_status "2. Run: start-monitoring"
print_status "3. Access Grafana at: http://your-ec2-ip:3000"
print_status "4. Access Prometheus at: http://your-ec2-ip:9090"
