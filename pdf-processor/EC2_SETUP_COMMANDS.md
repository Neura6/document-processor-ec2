# EC2 Ubuntu Monitoring Setup Commands

## **🚀 Complete EC2 Setup Commands**

### **Step 1: System Update & Docker Installation**

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker dependencies
sudo apt install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    python3-pip \
    python3-venv \
    git

# Add Docker GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Add Docker repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER

# Install Python dependencies
pip3 install prometheus-client boto3 pypdf2 reportlab
```

### **Step 2: Create Directory Structure**

```bash
# Create directories
mkdir -p ~/pdf-processor/monitoring/{data,config,logs}
mkdir -p ~/pdf-processor/monitoring/data/{prometheus,grafana}
mkdir -p ~/pdf-processor/monitoring/grafana/provisioning/{dashboards,datasources}

# Set permissions
chmod 755 ~/pdf-processor/monitoring/data
chmod 755 ~/pdf-processor/monitoring/config
```

### **Step 3: Clone Repository**

```bash
# Navigate to home
cd ~

# Clone your repository (replace with your actual repo)
git clone <your-repo-url> pdf-processor
cd pdf-processor
```

### **Step 4: Start Monitoring Stack**

```bash
# Start monitoring services
cd ~/pdf-processor/monitoring
docker-compose up -d

# Check if services are running
docker-compose ps
```

### **Step 5: Verify Installation**

```bash
# Check Prometheus
curl http://localhost:9090

# Check Grafana
curl http://localhost:3000

# Check Node Exporter
curl http://localhost:9100/metrics
```

### **Step 6: Run SQS Worker with Monitoring**

```bash
# Start SQS worker in background
nohup python3 ~/pdf-processor/sqs_worker.py > ~/pdf-processor/logs/sqs_worker.log 2>&1 &

# Check worker logs
tail -f ~/pdf-processor/logs/sqs_worker.log
```

### **Step 7: Access Dashboards**

```bash
# Grafana URL
http://your-ec2-public-ip:3000
# Login: admin/admin123

# Prometheus URL
http://your-ec2-public-ip:9090
```

### **Step 8: System Service Setup**

```bash
# Create systemd service for monitoring
sudo tee /etc/systemd/system/pdf-processor.service > /dev/null <<EOF
[Unit]
Description=PDF Processor SQS Worker
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/pdf-processor
ExecStart=/usr/bin/python3 /home/ubuntu/pdf-processor/sqs_worker.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable pdf-processor
sudo systemctl start pdf-processor
```

### **Step 9: Storage Auto-Scaling Setup**

```bash
# Create monitoring script
sudo tee /opt/monitor-storage.sh > /dev/null <<EOF
#!/bin/bash
# Storage monitoring script

USAGE=$(df / | tail -1 | awk '{print $5}' | sed 's/%//')

if [ $USAGE -gt 80 ]; then
    echo "Storage usage high: ${USAGE}%" | logger
    # Send alert (implement your alert mechanism)
fi
EOF

sudo chmod +x /opt/monitor-storage.sh

# Add to crontab
echo "*/5 * * * * /opt/monitor-storage.sh" | crontab -
```

### **Step 10: Firewall Configuration**

```bash
# Allow HTTP traffic (adjust as needed)
sudo ufw allow 3000/tcp  # Grafana
sudo ufw allow 9090/tcp  # Prometheus
sudo ufw allow 9100/tcp  # Node Exporter
```

### **Step 11: Environment Setup**

```bash
# Create .env file
cat > ~/pdf-processor/.env <<EOF
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
SOURCE_BUCKET=your-source-bucket
CHUNKED_BUCKET=your-chunked-bucket
SQS_QUEUE_URL=your-sqs-queue-url
EOF

# Load environment variables
source ~/pdf-processor/.env
```

### **Step 12: Quick Health Check**

```bash
# Check all services
echo "=== Service Status ==="
sudo systemctl status pdf-processor
echo "=== Docker Status ==="
docker-compose -f ~/pdf-processor/monitoring/docker-compose.yml ps
echo "=== Storage Usage ==="
df -h
echo "=== Memory Usage ==="
free -h
```

### **Step 13: Log Monitoring**

```bash
# Monitor logs
tail -f ~/pdf-processor/logs/sqs_worker.log
tail -f /var/log/syslog | grep pdf-processor

# Docker logs
docker-compose -f ~/pdf-processor/monitoring/docker-compose.yml logs -f
```

### **Step 14: Backup & Recovery**

```bash
# Backup Grafana dashboards
docker exec grafana tar czf - /var/lib/grafana > ~/grafana-backup.tar.gz

# Backup Prometheus data
docker exec prometheus tar czf - /prometheus > ~/prometheus-backup.tar.gz
```

### **Step 15: Troubleshooting Commands**

```bash
# Check service logs
sudo journalctl -u pdf-processor -f

# Check Docker logs
docker logs prometheus
docker logs grafana

# Check metrics endpoint
curl http://localhost:8000/metrics

# Test SQS connection
aws sqs list-queues --region us-east-1
```

## **🎯 One-Command Setup**

```bash
# Run this single command to setup everything
curl -fsSL https://raw.githubusercontent.com/your-repo/main/monitoring/setup-monitoring.sh | bash
```

## **📊 Verification Commands**

```bash
# Check all services are running
sudo systemctl is-active pdf-processor
docker-compose -f ~/pdf-processor/monitoring/docker-compose.yml ps

# Check metrics are being collected
prometheus_targets_up=$(curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets | length')
echo "Prometheus targets: $prometheus_targets_up"

# Test processing
python3 -c "from services.orchestrator import Orchestrator; print('Orchestrator loaded successfully')"
```

## **🔧 Maintenance Commands**

```bash
# Restart services
sudo systemctl restart pdf-processor
docker-compose -f ~/pdf-processor/monitoring/docker-compose.yml restart

# Update code
cd ~/pdf-processor
git pull origin main
sudo systemctl restart pdf-processor

# Monitor processing
watch -n 5 'curl -s http://localhost:8000/metrics | grep pdf_files_processed'
```
