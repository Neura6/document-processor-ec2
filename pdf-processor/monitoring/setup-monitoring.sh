#!/bin/bash
# Ubuntu EC2 Monitoring Setup Script
# Run these commands on your EC2 Ubuntu instance

echo "🚀 Starting PDF Processor Monitoring Setup..."

# Update system
echo "📦 Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install Docker
echo "🐳 Installing Docker..."
sudo apt install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER

# Install Python dependencies
echo "🐍 Installing Python dependencies..."
sudo apt install -y python3-pip python3-venv
pip3 install prometheus-client boto3 pypdf2 reportlab

# Create monitoring directories
echo "📁 Creating monitoring directories..."
mkdir -p ~/pdf-processor/monitoring/{data,config,logs}
mkdir -p ~/pdf-processor/monitoring/data/{prometheus,grafana}

# Set permissions
chmod 755 ~/pdf-processor/monitoring/data
chmod 755 ~/pdf-processor/monitoring/config

echo "✅ Monitoring setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Reboot to apply docker group changes: sudo reboot"
echo "2. After reboot, cd ~/pdf-processor"
echo "3. Run: docker-compose -f monitoring/docker-compose.yml up -d"
echo "4. Access Grafana at: http://your-ec2-ip:3000 (admin/admin123)"
echo "5. Access Prometheus at: http://your-ec2-ip:9090"
