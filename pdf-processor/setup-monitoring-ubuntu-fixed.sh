#!/bin/bash
# PDF Processing Monitoring Setup Script for Ubuntu EC2 - VENV Compatible

set -e

echo "🚀 Starting PDF Processing Monitoring Setup for Ubuntu (VENV Compatible)..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   print_error "This script should not be run as root"
   exit 1
fi

# Detect if we're in a virtual environment
if [[ -n "$VIRTUAL_ENV" ]]; then
    print_status "Detected virtual environment: $VIRTUAL_ENV"
    PIP_CMD="pip3"
    PYTHON_CMD="python3"
    INSTALL_FLAGS=""
else
    print_status "No virtual environment detected, using --user flag"
    PIP_CMD="pip3"
    PYTHON_CMD="python3"
    INSTALL_FLAGS="--user"
fi

# Update system
print_header "Updating Ubuntu System"
sudo apt update && sudo apt upgrade -y

# Install required packages
print_header "Installing System Dependencies"
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

# Install Docker (if not already installed)
print_header "Installing Docker"
if ! command -v docker &> /dev/null; then
    print_status "Installing Docker..."
    
    # Add Docker's official GPG key
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    
    # Add Docker repository
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    sudo apt update
    sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    
    # Add user to docker group
    sudo usermod -aG docker $USER
    print_warning "Please log out and log back in for Docker permissions to take effect"
else
    print_status "Docker already installed"
fi

# Install Docker Compose v2 (standalone)
print_header "Installing Docker Compose"
if ! command docker compose version &> /dev/null; then
    print_status "Installing Docker Compose v2..."
    sudo apt install -y docker-compose-plugin
    echo 'alias docker-compose="docker compose"' >> ~/.bashrc
else
    print_status "Docker Compose already installed"
fi

# Install Python dependencies (VENV compatible)
print_header "Installing Python Dependencies"
print_status "Using $PIP_CMD with flags: $INSTALL_FLAGS"

# Install packages
$PIP_CMD install $INSTALL_FLAGS \
    boto3 \
    prometheus-client \
    psutil \
    PyPDF2 \
    reportlab \
    python-dotenv

# Verify installations
print_status "Verifying Python packages..."
$PYTHON_CMD -c "import boto3; print('✅ boto3 installed')"
$PYTHON_CMD -c "import prometheus_client; print('✅ prometheus-client installed')"
$PYTHON_CMD -c "import psutil; print('✅ psutil installed')"

# Install AWS CLI
print_header "Installing AWS CLI"
if ! command -v aws &> /dev/null; then
    print_status "Installing AWS CLI..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
    rm -rf awscliv2.zip aws
else
    print_status "AWS CLI already installed"
fi

# Create monitoring directories
print_header "Creating Monitoring Directories"
sudo mkdir -p /opt/monitoring/{data,config,scripts,logs}
sudo mkdir -p /opt/monitoring/data/{prometheus,grafana}
sudo mkdir -p /opt/monitoring/config/{prometheus,grafana/provisioning/{dashboards,datasources}}
sudo mkdir -p /opt/monitoring/logs/{prometheus,grafana}

# Set permissions
sudo chown -R $USER:$USER /opt/monitoring
sudo chmod -R 755 /opt/monitoring

# Create systemd service for storage monitoring
print_header "Creating Storage Monitoring Service"
sudo tee /etc/systemd/system/storage-monitor.service > /dev/null << 'EOF'
[Unit]
Description=Storage Auto-Scaling Monitor
After=docker.service

[Service]
Type=simple
ExecStart=/opt/monitoring/scripts/auto-resize.sh
Restart=always
RestartSec=300
User=ubuntu

[Install]
WantedBy=multi-user.target
EOF

# Create systemd timer for storage monitoring
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

# Create storage auto-scaling script
print_header "Creating Storage Auto-Scaling Script"
sudo tee /opt/monitoring/scripts/auto-resize.sh > /dev/null << 'EOF'
#!/bin/bash
# Storage auto-resizing script for Ubuntu

LOG_FILE="/opt/monitoring/logs/storage-monitor.log"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

# Check disk usage
USAGE=$(df /opt/monitoring 2>/dev/null | tail -1 | awk '{print $5}' | sed 's/%//')
if [ -z "$USAGE" ]; then
    USAGE=$(df / 2>/dev/null | tail -1 | awk '{print $5}' | sed 's/%//')
fi

THRESHOLD=85

log_message "Checking disk usage: ${USAGE}%"

if [ "$USAGE" -gt "$THRESHOLD" ]; then
    log_message "Disk usage at ${USAGE}%, triggering cleanup"
    
    # Clean Docker system
    if command -v docker &> /dev/null; then
        docker system prune -f >> "$LOG_FILE" 2>&1
        log_message "Docker cleanup completed"
    fi
    
    # Clean old logs (keep last 7 days)
    find /opt/monitoring/logs -name "*.log" -mtime +7 -delete 2>/dev/null || true
    log_message "Old logs cleaned"
    
    # Clean Prometheus old data (keep last 15 days)
    if docker ps | grep -q prometheus; then
        docker exec prometheus sh -c "find /prometheus -name '*.tmp' -mtime +7 -delete" 2>/dev/null || true
        log_message "Prometheus cleanup completed"
    fi
    
    # Check if we need to alert
    NEW_USAGE=$(df /opt/monitoring 2>/dev/null | tail -1 | awk '{print $5}' | sed 's/%//')
    if [ -z "$NEW_USAGE" ]; then
        NEW_USAGE=$(df / 2>/dev/null | tail -1 | awk '{print $5}' | sed 's/%//')
    fi
    
    log_message "Cleanup completed. New usage: ${NEW_USAGE}%"
    
    if [ "$NEW_USAGE" -gt 95 ]; then
        log_message "WARNING: Storage still critically high after cleanup"
    fi
fi
EOF

sudo chmod +x /opt/monitoring/scripts/auto-resize.sh

# Create monitoring start script
print_header "Creating Monitoring Start Script"
sudo tee /opt/monitoring/start-monitoring.sh > /dev/null << 'EOF'
#!/bin/bash
# Start monitoring services on Ubuntu (VENV Compatible)

set -e

echo "🚀 Starting PDF Processing Monitoring on Ubuntu..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first:"
    echo "   sudo systemctl start docker"
    exit 1
fi

# Create necessary directories
mkdir -p /opt/monitoring/data/{prometheus,grafana}
mkdir -p /opt/monitoring/logs/{prometheus,grafana}

# Copy configuration files if they exist in current directory
if [ -d "./config" ]; then
    cp -r ./config/* /opt/monitoring/config/
    echo "✅ Configuration files copied"
fi

# Copy docker-compose.yml if it exists
if [ -f "./docker-compose.yml" ]; then
    cp ./docker-compose.yml /opt/monitoring/
    echo "✅ Docker Compose file copied"
fi

# Start services
cd /opt/monitoring
if [ -f "docker-compose.yml" ]; then
    docker compose up -d
else
    echo "❌ docker-compose.yml not found in /opt/monitoring/"
    echo "   Please copy docker-compose.yml to /opt/monitoring/"
    exit 1
fi

# Wait for services to start
echo "⏳ Waiting for services to start..."
sleep 30

# Check service health
services=("prometheus" "grafana")
for service in "${services[@]}"; do
    if docker compose ps | grep -q "$service.*Up"; then
        echo "✅ $service is running"
    else
        echo "❌ $service failed to start"
        docker compose logs "$service" --tail=20
    fi
done

# Get public IP for access
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 || echo "localhost")

echo "🎉 Monitoring setup complete!"
echo ""
echo "📊 Access URLs:"
echo "   Grafana: http://$PUBLIC_IP:3000"
echo "   Prometheus: http://$PUBLIC_IP:9090"
echo ""
echo "📋 Default credentials:"
echo "   Username: admin"
echo "   Password: admin123"
echo ""
echo "🔧 Commands:"
echo "   View logs: docker compose logs -f"
echo "   Stop services: docker compose down"
echo "   Restart: docker compose restart"
EOF

sudo chmod +x /opt/monitoring/start-monitoring.sh

# Create monitoring stop script
print_header "Creating Monitoring Stop Script"
sudo tee /opt/monitoring/stop-monitoring.sh > /dev/null << 'EOF'
#!/bin/bash
# Stop monitoring services on Ubuntu

echo "🛑 Stopping PDF Processing Monitoring..."

if [ -f "/opt/monitoring/docker-compose.yml" ]; then
    cd /opt/monitoring
    docker compose down
    echo "✅ Monitoring services stopped"
else
    echo "⚠️  No docker-compose.yml found, checking running containers..."
    docker stop prometheus grafana node-exporter 2>/dev/null || true
    echo "✅ Containers stopped"
fi
EOF

sudo chmod +x /opt/monitoring/stop-monitoring.sh

# Create systemd service for system monitoring
print_header "Creating System Monitoring Service"
sudo tee /etc/systemd/system/system-monitor.service > /dev/null << 'EOF'
[Unit]
Description=System Metrics Collector
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 /opt/monitoring/system-monitor.py
Restart=always
RestartSec=60
User=ubuntu
WorkingDirectory=/opt/monitoring

[Install]
WantedBy=multi-user.target
EOF

# Create system monitoring script (VENV compatible)
sudo tee /opt/monitoring/system-monitor.py > /dev/null << 'EOF'
#!/usr/bin/env python3
"""
System Monitoring Script for Ubuntu (VENV Compatible)
Collects system metrics and updates Prometheus metrics
"""

import psutil
import time
import logging
import os
import sys

# Try to import metrics service, handle both venv and system paths
try:
    # Try current directory first
    sys.path.insert(0, '/home/ubuntu/pdf-processor')
    from services.metrics_service import metrics
    print("✅ Using services.metrics_service from pdf-processor")
except ImportError:
    try:
        # Try venv path
        import site
        venv_path = None
        for path in site.getsitepackages():
            if 'venv' in path:
                venv_path = path
                break
        
        if venv_path:
            sys.path.insert(0, os.path.dirname(venv_path))
            from services.metrics_service import metrics
            print("✅ Using services.metrics_service from venv")
        else:
            raise ImportError
    except ImportError:
        # Fallback - create basic metrics if services not available
        print("⚠️  services.metrics_service not found, using basic metrics")
        
        class BasicMetrics:
            def update_system_metrics(self, cpu, memory, mount, disk):
                print(f"System: CPU={cpu}%, Memory={memory}%, Disk={mount}={disk}%")
        
        metrics = BasicMetrics()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/monitoring/logs/system-monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UbuntuSystemMonitor:
    """Ubuntu system metrics collector"""
    
    def __init__(self, interval=30):
        self.interval = interval
        
    def collect_metrics(self):
        """Collect and update system metrics"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Disk usage for monitoring directory
            try:
                disk = psutil.disk_usage('/opt/monitoring')
                disk_percent = (disk.used / disk.total) * 100
                disk_mount = '/opt/monitoring'
            except:
                disk = psutil.disk_usage('/')
                disk_percent = (disk.used / disk.total) * 100
                disk_mount = '/'
            
            # Update metrics
            metrics.update_system_metrics(cpu_percent, memory_percent, disk_mount, disk_percent)
            
            # Log system status
            logger.info(f"System metrics - CPU: {cpu_percent}%, Memory: {memory_percent}%, Disk({disk_mount}): {disk_percent}%")
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'disk_percent': disk_percent,
                'disk_free_gb': disk.free / (1024**3),
                'memory_free_gb': memory.available / (1024**3),
                'load_avg': os.getloadavg()
            }
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return None
    
    def run(self):
        """Run continuous monitoring"""
        logger.info(f"Starting Ubuntu system monitoring (interval: {self.interval}s)")
        
        while True:
            try:
                self.collect_metrics()
                time.sleep(self.interval)
            except KeyboardInterrupt:
                logger.info("Stopping system monitoring...")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.interval)

if __name__ == "__main__":
    monitor = UbuntuSystemMonitor()
    monitor.run()
EOF

sudo chmod +x /opt/monitoring/system-monitor.py

# Create convenience aliases
print_header "Creating Convenience Commands"
sudo tee /usr/local/bin/start-monitoring > /dev/null << 'EOF'
#!/bin/bash
/opt/monitoring/start-monitoring.sh
EOF

sudo tee /usr/local/bin/stop-monitoring > /dev/null << 'EOF'
#!/bin/bash
/opt/monitoring/stop-monitoring.sh
EOF

sudo chmod +x /usr/local/bin/start-monitoring
sudo chmod +x /usr/local/bin/stop-monitoring

# Create monitoring status script
sudo tee /usr/local/bin/monitoring-status > /dev/null << 'EOF'
#!/bin/bash
echo "📊 PDF Processing Monitoring Status"
echo "=================================="
echo ""

# Check Docker
if command -v docker &> /dev/null; then
    echo "✅ Docker: $(docker --version)"
else
    echo "❌ Docker: Not installed"
fi

# Check Docker Compose
if command docker compose version &> /dev/null; then
    echo "✅ Docker Compose: $(docker compose version)"
else
    echo "❌ Docker Compose: Not installed"
fi

# Check system services
services=("storage-monitor.timer" "system-monitor.service")
for service in "${services[@]}"; do
    if systemctl is-active --quiet "$service"; then
        echo "✅ $service: Active"
    else
        echo "⚠️  $service: Inactive"
    fi
done

echo ""
echo "📁 Directory Structure:"
echo "   /opt/monitoring/"
echo "   ├── data/{prometheus,grafana}/"
echo "   ├── config/{prometheus,grafana}/"
echo "   ├── scripts/"
echo "   └── logs/"

echo ""
echo "🚀 To start monitoring:"
echo "   start-monitoring"
echo ""
echo "🛑 To stop monitoring:"
echo "   stop-monitoring"
EOF

sudo chmod +x /usr/local/bin/monitoring-status

# Create requirements file for Python dependencies
print_header "Creating Requirements File"
sudo tee /opt/monitoring/requirements.txt > /dev/null << 'EOF'
boto3==1.34.0
prometheus-client==0.19.0
psutil==5.9.8
PyPDF2==3.0.1
reportlab==4.0.7
python-dotenv==1.0.0
EOF

# Enable and start services
print_header "Enabling System Services"
sudo systemctl daemon-reload
sudo systemctl enable storage-monitor.timer
sudo systemctl enable system-monitor.service

# Create final summary
print_header "🎉 Ubuntu Monitoring Setup Complete!"
echo ""
echo "📋 Installation Summary:"
echo "   ✅ System packages updated"
echo "   ✅ Docker & Docker Compose installed"
echo "   ✅ Python dependencies installed (VENV compatible)"
echo "   ✅ AWS CLI installed"
echo "   ✅ Monitoring directories created"
echo "   ✅ System services configured"
echo "   ✅ Scripts created and executable"
echo ""
echo "🚀 Next Steps:"
echo "   1. Log out and log back in for Docker permissions"
echo "   2. Copy your monitoring files to /opt/monitoring/"
echo "   3. Run: start-monitoring"
echo ""
echo "📊 Commands Available:"
echo "   monitoring-status   - Check monitoring status"
echo "   start-monitoring    - Start all monitoring services"
echo "   stop-monitoring     - Stop all monitoring services"
echo ""
echo "📁 Important Paths:"
echo "   /opt/monitoring/    - Main monitoring directory"
echo "   /opt/monitoring/docker-compose.yml"
echo "   /opt/monitoring/start-monitoring.sh"
echo "   /opt/monitoring/stop-monitoring.sh"
echo ""
echo "🌐 After starting, access:"
echo "   Grafana: http://your-ubuntu-ec2-ip:3000"
echo "   Prometheus: http://your-ubuntu-ec2-ip:9090"
echo ""
echo "💡 VENV Note:"
echo "   Python packages installed in current environment"
echo "   If using venv, packages are in: $VIRTUAL_ENV"
