# EC2 Monitoring Setup - Complete Guide

## Current Status
Your PDF processing pipeline is working perfectly. This guide ensures Prometheus and Grafana are properly configured and accessible.

## Step-by-Step EC2 Setup

### 1. Navigate to Correct Directory
```bash
# From your current location
cd ~/document-processor-ec2/pdf-processor/monitoring

# Check you're in the right place
pwd
ls -la
```

### 2. Run Fixed Verification
```bash
# Make the fixed script executable
chmod +x verify-setup-fixed.sh

# Run verification
./verify-setup-fixed.sh
```

### 3. Deploy Monitoring Stack
```bash
# Start monitoring services
cd docker
docker-compose up -d

# Check status
docker-compose ps

# View logs if needed
docker-compose logs
```

### 4. Access Dashboards
```bash
# Get your EC2 public IP
PUBLIC_IP=$(curl -s https://checkip.amazonaws.com)
echo "Access URLs:"
echo "  Grafana: http://$PUBLIC_IP:3000"
echo "  Prometheus: http://$PUBLIC_IP:9090"
```

### 5. Verify Everything Works
```bash
# Check Grafana
curl -s http://localhost:3000/api/health

# Check Prometheus
curl -s http://localhost:9090/api/v1/query?query=up

# Check services are running
sudo systemctl status pdf-processor-worker
sudo systemctl status pdf-metrics
```

## Common Issues & Fixes

### Port Already in Use
```bash
# Check what's using ports
sudo netstat -tulpn | grep -E ':(3000|9090|9100)'

# Stop conflicting services
sudo systemctl stop <service-name>
```

### Docker Issues
```bash
# Restart Docker
sudo systemctl restart docker

# Check Docker status
sudo systemctl status docker
```

### Permission Issues
```bash
# Fix Docker permissions
sudo usermod -aG docker $USER
# Log out and back in
```

### Firewall Issues
```bash
# Allow ports through firewall (if using ufw)
sudo ufw allow 3000/tcp
sudo ufw allow 9090/tcp
sudo ufw allow 9100/tcp
```

## Quick Health Check Commands

```bash
# Complete system health check
./monitoring/verify-setup-fixed.sh

# Check PDF processing
sudo journalctl -u pdf-processor-worker -f

# Check monitoring
sudo systemctl status pdf-metrics

# Check SQS worker
python3 services/sqs_worker.py --test
```

## Dashboard URLs
- **Main Dashboard**: `http://YOUR-EC2-IP:3000/d/pdf-pipeline-overview`
- **Grafana**: `http://YOUR-EC2-IP:3000` (admin/admin123)
- **Prometheus**: `http://YOUR-EC2-IP:9090`

## Production Commands
```bash
# Start everything
start-production

# Check status
production-status

# View logs
sudo journalctl -u pdf-processor-worker -f
```

## Success Indicators
✅ **Docker containers running** (prometheus, grafana, node-exporter)
✅ **Grafana accessible** at http://EC2-IP:3000
✅ **Prometheus accessible** at http://EC2-IP:9090
✅ **PDF processing working** (S3 → SQS → processing → KB sync)
✅ **Real-time metrics** visible in Grafana dashboards
