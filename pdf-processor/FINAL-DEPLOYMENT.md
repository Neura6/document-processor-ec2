# 🚀 Complete EC2 Production Deployment Guide

## Overview
This guide sets up your EC2 instance for **full production mode** - both monitoring and SQS worker running together to handle the complete pipeline: **S3 → SQS → EC2 Processing → KB Sync** with **real-time monitoring**.

## 📋 Complete Pipeline Flow
```
S3 Upload → SQS Event → EC2 Worker → PDF Processing → KB Sync → Monitoring Dashboard
```

## 🎯 Quick Start (5 minutes)

### 1. Copy Files to EC2
```bash
# From your local machine
scp -i your-key.pem -r pdf-processor/ ubuntu@your-ec2-ip:/home/ubuntu/
```

### 2. Run Complete Setup
```bash
# SSH into EC2
ssh -i your-key.pem ubuntu@your-ec2-ip

# Run production setup
cd /home/ubuntu/pdf-processor
chmod +x production-setup.sh
./production-setup.sh
```

### 3. Verify Everything is Running
```bash
# Check all services
production-status
```

## 🔧 Manual Setup (If Needed)

### Step 1: Environment Setup
```bash
# Set AWS credentials (update with your values)
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_DEFAULT_REGION=us-east-1

# Ensure Python path
export PYTHONPATH=/home/ubuntu/pdf-processor
```

### Step 2: Start Monitoring Stack
```bash
# Start Prometheus + Grafana + Node Exporter
cd /home/ubuntu/pdf-processor/monitoring/docker
sudo docker compose up -d

# Verify monitoring is running
sudo docker compose ps
```

### Step 3: Start SQS Worker Service
```bash
# Create and start SQS worker service
sudo systemctl daemon-reload
sudo systemctl enable --now pdf-processor-worker
sudo systemctl enable --now pdf-metrics
```

## 📊 Access Points

### Monitoring Dashboards
- **Grafana**: `http://your-ec2-ip:3000` (admin/admin123)
- **Prometheus**: `http://your-ec2-ip:9090`

### Service Status
```bash
# Check everything
production-status

# Check individual services
sudo systemctl status pdf-processor-worker
sudo systemctl status pdf-metrics
sudo docker compose -f monitoring/docker/docker-compose.yml ps
```

## 🔄 Production Commands

### Start Everything
```bash
start-production
```

### Stop Everything
```bash
sudo systemctl stop pdf-processor-worker pdf-metrics
cd /home/ubuntu/pdf-processor/monitoring/docker
sudo docker compose down
```

### Check Status
```bash
production-status
```

### View Logs
```bash
# SQS Worker logs
sudo journalctl -u pdf-processor-worker -f

# Metrics logs
sudo journalctl -u pdf-metrics -f

# Docker logs
cd /home/ubuntu/pdf-processor/monitoring/docker
sudo docker compose logs -f
```

## 📈 Monitoring Metrics

### Real-time Dashboards
- **PDF Processing Rate**: Files processed per minute
- **Success Rate**: Success/failure percentages
- **Queue Depth**: SQS messages waiting
- **Processing Time**: Time per processing step
- **System Resources**: CPU, memory, disk usage
- **Error Tracking**: Error types and rates

### Alerts Configured
- High error rate (>10%)
- Slow processing (>120s)
- Queue backlog (>50 messages)
- High resource usage (>80%)

## 🔍 Testing the Pipeline

### 1. Upload Test File to S3
```bash
# Upload test PDF to trigger processing
aws s3 cp test.pdf s3://your-rules-bucket/
```

### 2. Monitor in Real-time
```bash
# Watch processing in Grafana
curl http://your-ec2-ip:3000

# Watch SQS processing
sudo journalctl -u pdf-processor-worker -f
```

### 3. Verify KB Sync
```bash
# Check KB sync metrics in Prometheus
# Look for kb_sync_total metric
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. Service Not Starting
```bash
# Check service status
sudo systemctl status pdf-processor-worker
sudo journalctl -u pdf-processor-worker --no-pager

# Restart services
sudo systemctl restart pdf-processor-worker pdf-metrics
```

#### 2. Monitoring Not Accessible
```bash
# Check security groups
aws ec2 describe-security-groups --group-ids sg-xxxxxx

# Add rules if needed
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxxx \
  --protocol tcp \
  --port 3000 \
  --cidr 0.0.0.0/0

aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxxx \
  --protocol tcp \
  --port 9090 \
  --cidr 0.0.0.0/0
```

#### 3. Worker Not Processing
```bash
# Check SQS queue
aws sqs get-queue-attributes --queue-url your-queue-url --attribute-names ApproximateNumberOfMessages

# Check worker logs
sudo journalctl -u pdf-processor-worker --no-pager
```

## 🔄 Auto-restart on Reboot

All services are configured to start automatically on system reboot:
- Docker containers (monitoring stack)
- SQS worker service
- Metrics collection service

## 📋 Production Checklist

- [ ] AWS credentials configured
- [ ] Security group allows ports 3000, 9090
- [ ] S3 bucket configured with SQS notifications
- [ ] SQS queue created and configured
- [ ] Environment variables set
- [ ] Services running: `production-status`
- [ ] Monitoring accessible via browser
- [ ] Test file upload working end-to-end

## 🚨 Emergency Commands

```bash
# Emergency restart
sudo systemctl restart pdf-processor-worker pdf-metrics
sudo docker compose -f /home/ubuntu/pdf-processor/monitoring/docker/docker-compose.yml restart

# Check disk space
df -h

# Check memory
free -h

# Check CPU usage
top
```

## 🎉 Success Indicators

When everything is working correctly:
1. **Grafana dashboard** shows real-time metrics
2. **SQS worker** processes files automatically
3. **KB sync** completes successfully
4. **No errors** in logs
5. **All services** show as running in `production-status`

**Your EC2 instance is now fully production-ready for complete PDF processing with monitoring!**
