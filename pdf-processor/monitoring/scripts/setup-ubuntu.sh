#!/bin/bash

# PDF Processing Monitoring Setup Script for Ubuntu EC2
# This script sets up Prometheus, Grafana, and monitoring for PDF processing pipeline

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   error "This script should not be run as root"
   exit 1
fi

# Check Ubuntu version
if ! command -v lsb_release &> /dev/null; then
    error "lsb_release not found. Please install it."
    exit 1
fi

UBUNTU_VERSION=$(lsb_release -rs)
log "Setting up monitoring on Ubuntu $UBUNTU_VERSION"

# Update system
log "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install system dependencies
log "Installing system dependencies..."
sudo apt install -y \
    curl \
    wget \
    git \
    unzip \
    htop \
    jq \
    build-essential \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    apt-transport-https \
    ca-certificates \
    gnupg \
    lsb-release

# Install Docker
log "Installing Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt update
    sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    sudo usermod -aG docker $USER
    log "Docker installed. Please log out and back in for group changes to take effect."
else
    log "Docker already installed"
fi

# Install AWS CLI
log "Installing AWS CLI..."
if ! command -v aws &> /dev/null; then
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
    rm -rf awscliv2.zip aws
    log "AWS CLI installed"
else
    log "AWS CLI already installed"
fi

# Install Python dependencies
log "Installing Python dependencies..."
if [[ "$VIRTUAL_ENV" != "" ]]; then
    # Inside virtual environment
    pip3 install boto3 prometheus-client psutil PyPDF2 reportlab python-dotenv
else
    # System-wide installation
    pip3 install --user boto3 prometheus-client psutil PyPDF2 reportlab python-dotenv
fi

# Create monitoring directory structure
log "Creating monitoring directories..."
sudo mkdir -p /opt/monitoring/{config,scripts,logs,data}
sudo chown -R $USER:$USER /opt/monitoring

# Copy monitoring files
log "Setting up monitoring configuration..."
cp -r monitoring/* /opt/monitoring/

# Create systemd services
log "Creating systemd services..."
cat > /tmp/monitoring.service << EOF
[Unit]
Description=PDF Processing Monitoring Stack
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/monitoring
ExecStart=/usr/bin/docker compose -f /opt/monitoring/docker/docker-compose.yml up -d
ExecStop=/usr/bin/docker compose -f /opt/monitoring/docker/docker-compose.yml down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

sudo mv /tmp/monitoring.service /etc/systemd/system/
sudo systemctl daemon-reload

# Create convenience scripts
log "Creating convenience scripts..."
cat > /usr/local/bin/start-monitoring << 'EOF'
#!/bin/bash
cd /opt/monitoring/docker
sudo docker compose up -d
echo "Monitoring stack started"
EOF

cat > /usr/local/bin/stop-monitoring << 'EOF'
#!/bin/bash
cd /opt/monitoring/docker
sudo docker compose down
echo "Monitoring stack stopped"
EOF

cat > /usr/local/bin/monitoring-status << 'EOF'
#!/bin/bash
echo "=== Monitoring Stack Status ==="
cd /opt/monitoring/docker
sudo docker compose ps

echo -e "\n=== Service URLs ==="
PUBLIC_IP=$(curl -s https://checkip.amazonaws.com || curl -s https://api.ipify.org)
echo "Grafana: http://$PUBLIC_IP:3000"
echo "Prometheus: http://$PUBLIC_IP:9090"

echo -e "\n=== System Resources ==="
echo "CPU: $(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print 100 - $1"%"}')"
echo "Memory: $(free -h | awk '/^Mem:/ {print $3 "/" $2}')"
echo "Disk: $(df -h / | awk 'NR==2 {print $3 "/" $2 " (" $5 ")"}')"
EOF

chmod +x /usr/local/bin/start-monitoring /usr/local/bin/stop-monitoring /usr/local/bin/monitoring-status

# Create storage auto-resize script
log "Creating storage auto-resize script..."
cat > /opt/monitoring/scripts/auto-resize.sh << 'EOF'
#!/bin/bash
# Auto-resize storage for monitoring data

THRESHOLD=85
MOUNT_POINT="/"

usage=$(df -h "$MOUNT_POINT" | awk 'NR==2 {print $5}' | sed 's/%//')

if [ "$usage" -gt "$THRESHOLD" ]; then
    echo "Disk usage is ${usage}%, above threshold ${THRESHOLD}%"
    
    # Clean old Prometheus data (keep last 7 days)
    if [ -d "/opt/monitoring/data/prometheus" ]; then
        find /opt/monitoring/data/prometheus -type f -mtime +7 -delete
    fi
    
    # Clean old logs
    if [ -d "/opt/monitoring/logs" ]; then
        find /opt/monitoring/logs -type f -mtime +7 -delete
    fi
    
    echo "Cleanup completed"
else
    echo "Disk usage is ${usage}%, within threshold"
fi
EOF

chmod +x /opt/monitoring/scripts/auto-resize.sh

# Create systemd timer for auto-resize
log "Creating auto-resize timer..."
cat > /tmp/monitoring-cleanup.service << EOF
[Unit]
Description=Monitoring Storage Cleanup

[Service]
Type=oneshot
ExecStart=/opt/monitoring/scripts/auto-resize.sh
User=ubuntu
EOF

cat > /tmp/monitoring-cleanup.timer << EOF
[Unit]
Description=Run monitoring cleanup every hour
Requires=monitoring-cleanup.service

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
EOF

sudo mv /tmp/monitoring-cleanup.service /tmp/monitoring-cleanup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable monitoring-cleanup.timer
sudo systemctl start monitoring-cleanup.timer

# Enable services
log "Enabling services..."
sudo systemctl enable monitoring.service
sudo systemctl enable docker
sudo systemctl start docker

# Final instructions
log "Setup complete!"
echo ""
echo "=== Next Steps ==="
echo "1. If you just added your user to the docker group, log out and back in"
echo "2. Start monitoring: start-monitoring"
echo "3. Check status: monitoring-status"
echo "4. Access Grafana: http://YOUR-EC2-IP:3000 (admin/admin123)"
echo "5. Access Prometheus: http://YOUR-EC2-IP:9090"
echo ""
echo "=== Commands ==="
echo "Start monitoring: start-monitoring"
echo "Stop monitoring: stop-monitoring"
echo "Check status: monitoring-status"
echo "View logs: sudo journalctl -u monitoring.service -f"
