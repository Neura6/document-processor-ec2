# 🐧 Ubuntu EC2 Monitoring Deployment Guide

## 🚀 Quick Ubuntu Setup (5 minutes)

### **Step 1: Copy Files to Ubuntu EC2**
```bash
# From your local machine
scp -i your-key.pem -r pdf-processor/ ubuntu@your-ec2-ip:/home/ubuntu/
```

### **Step 2: Run Ubuntu Setup Script**
```bash
# SSH into Ubuntu EC2
ssh -i your-key.pem ubuntu@your-ec2-ip

# Navigate to directory
cd /home/ubuntu/pdf-processor

# Make setup script executable
chmod +x setup-monitoring-ubuntu.sh

# Run Ubuntu-specific setup
./setup-monitoring-ubuntu.sh
```

### **Step 3: Start Monitoring (After Re-login)**
```bash
# Logout and login again for Docker permissions
exit
ssh -i your-key.pem ubuntu@your-ec2-ip

# Start monitoring
cd /home/ubuntu/pdf-processor
start-monitoring
```

## 📦 Ubuntu Package Dependencies

### **System Packages (apt install)**
```bash
# Core system
sudo apt update && sudo apt upgrade -y

# Essential tools
sudo apt install -y curl wget git unzip htop jq build-essential

# Python ecosystem
sudo apt install -y python3 python3-pip python3-venv python3-dev

# Docker dependencies
sudo apt install -y apt-transport-https ca-certificates gnupg lsb-release
```

### **Docker Installation (Ubuntu)**
```bash
# Add Docker's official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Add Docker repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER
```

### **Python Dependencies**
```bash
# Install via pip3
pip3 install --user \
    boto3==1.34.0 \
    prometheus-client==0.19.0 \
    psutil==5.9.8 \
    PyPDF2==3.0.1 \
    reportlab==4.0.7 \
    python-dotenv==1.0.0
```

### **AWS CLI Installation (Ubuntu)**
```bash
# Install AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf awscliv2.zip aws
```

## 🔧 Ubuntu-Specific Commands

### **Service Management (systemctl)**
```bash
# Start Docker
sudo systemctl start docker
sudo systemctl enable docker

# Check Docker status
sudo systemctl status docker

# Start monitoring services
sudo systemctl start storage-monitor.timer
sudo systemctl enable storage-monitor.timer

# Check monitoring status
systemctl status storage-monitor.timer
systemctl status system-monitor.service
```

### **File Locations (Ubuntu)**
```bash
# Monitoring directory
/opt/monitoring/

# System services
/etc/systemd/system/

# User binaries
/usr/local/bin/

# Logs
/var/log/
/opt/monitoring/logs/
```

## 🐳 Docker Compose on Ubuntu

### **Using Docker Compose v2 (Ubuntu)**
```bash
# Ubuntu uses docker compose (space, not hyphen)
docker compose --version

# Start services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f
```

### **Ubuntu Docker Compose File**
```yaml
# Located at: /opt/monitoring/docker-compose.yml
# Uses Ubuntu-compatible paths and permissions
```

## 🔍 Troubleshooting Ubuntu Issues

### **Docker Permission Issues**
```bash
# Fix Docker permissions
sudo usermod -aG docker $USER
# Logout and login again

# Test Docker
docker run hello-world
```

### **Port Access Issues**
```bash
# Ubuntu firewall (ufw)
sudo ufw allow 3000/tcp  # Grafana
sudo ufw allow 9090/tcp  # Prometheus
sudo ufw allow 9100/tcp  # Node Exporter

# Or use AWS Security Groups instead
```

### **Python Path Issues**
```bash
# Add to .bashrc
echo 'export PYTHONPATH="/home/ubuntu/pdf-processor:$PYTHONPATH"' >> ~/.bashrc
source ~/.bashrc
```

### **Storage Issues**
```bash
# Check disk usage
df -h

# Check inode usage
df -i

# Clean apt cache
sudo apt clean

# Clean Docker
sudo docker system prune -a
```

## 📊 Verification Commands

### **Check Installation**
```bash
# Verify all components
monitoring-status

# Check individual components
docker --version
docker compose version
python3 --version
pip3 list | grep -E "(boto3|prometheus|psutil)"
aws --version
```

### **Test Services**
```bash
# Test Docker
sudo docker run hello-world

# Test Python imports
python3 -c "import boto3; print('boto3 OK')"
python3 -c "import prometheus_client; print('prometheus OK')"
python3 -c "import psutil; print('psutil OK')"

# Test AWS CLI
aws sts get-caller-identity
```

## 🚀 Complete Ubuntu Deployment

### **One-Command Setup**
```bash
# Copy and run this on Ubuntu EC2
curl -fsSL https://raw.githubusercontent.com/your-repo/setup-monitoring-ubuntu.sh | bash
```

### **Manual Steps**
```bash
# 1. Update system
sudo apt update && sudo apt upgrade -y

# 2. Install dependencies
sudo apt install -y curl wget git python3 python3-pip docker.io docker-compose-plugin

# 3. Install Python packages
pip3 install --user boto3 prometheus-client psutil PyPDF2 reportlab

# 4. Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip awscliv2.zip && sudo ./aws/install && rm -rf awscliv2.zip aws

# 5. Configure AWS
aws configure

# 6. Start monitoring
start-monitoring
```

## 🌐 Access URLs (Ubuntu)

After setup, access your monitoring:
- **Grafana**: `http://your-ubuntu-ec2-ip:3000`
- **Prometheus**: `http://your-ubuntu-ec2-ip:9090`
- **Node Exporter**: `http://your-ubuntu-ec2-ip:9100`

## 🔐 Security Setup

### **AWS Security Groups**
```bash
# Add inbound rules
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxx \
  --protocol tcp \
  --port 3000 \
  --cidr your-ip-address/32

aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxx \
  --protocol tcp \
  --port 9090 \
  --cidr your-ip-address/32
```

### **Change Default Passwords**
```bash
# Change Grafana password
# Access Grafana at http://your-ip:3000
# Login: admin/admin123
# Go to Configuration > Users > Change password
```

## 📞 Ubuntu Support

### **Common Ubuntu Commands**
```bash
# Check system info
lsb_release -a
uname -a

# Check services
systemctl list-units --type=service --state=running

# Check logs
journalctl -u docker.service -f
journalctl -u storage-monitor.service -f

# Check resources
htop
df -h
free -h
```

### **Get Help**
```bash
# Ubuntu-specific help
man systemd
man docker
man docker-compose

# Check logs
sudo journalctl -f
```
