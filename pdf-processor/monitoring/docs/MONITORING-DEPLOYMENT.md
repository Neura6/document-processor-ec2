# PDF Processing Monitoring Deployment Guide

## Overview
This guide provides step-by-step instructions for deploying a comprehensive monitoring solution for the PDF processing pipeline using Prometheus and Grafana on Ubuntu EC2.

## Architecture
- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization and dashboards
- **Node Exporter**: System metrics
- **Custom Exporters**: PDF processing metrics

## Prerequisites
- Ubuntu 20.04+ EC2 instance
- AWS CLI configured
- Security group allowing ports 3000, 9090
- Docker and Docker Compose installed

## Quick Start

### 1. Setup Monitoring
```bash
# Clone and navigate to project
cd pdf-processor

# Run setup script
chmod +x monitoring/scripts/setup-ubuntu.sh
./monitoring/scripts/setup-ubuntu.sh
```

### 2. Start Monitoring
```bash
# Start all services
start-monitoring

# Check status
monitoring-status
```

### 3. Access Dashboards
- **Grafana**: http://your-ec2-ip:3000 (admin/admin123)
- **Prometheus**: http://your-ec2-ip:9090

## Directory Structure
```
monitoring/
├── docker/
│   └── docker-compose.yml
├── config/
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   └── dashboards/
│   └── alertmanager/
├── scripts/
│   ├── setup-ubuntu.sh
│   ├── start-monitoring.sh
│   └── auto-resize.sh
└── docs/
    └── MONITORING-DEPLOYMENT.md
```

## Security Group Configuration
```bash
# Allow monitoring ports
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

## Monitoring Metrics

### PDF Processing Metrics
- `pdf_files_processed_total`: Total files processed
- `pdf_processing_duration_seconds`: Processing time per step
- `pdf_processing_errors_total`: Error counts by type
- `sqs_messages_in_queue`: Queue depth
- `kb_sync_total`: Knowledge Base sync operations

### System Metrics
- `system_cpu_percent`: CPU usage
- `system_memory_percent`: Memory usage
- `system_disk_percent`: Disk usage

## Dashboards

### PDF Processing Pipeline Overview
- Real-time processing rate
- Success rate gauge
- Processing duration by step
- Queue depth
- System resource usage
- Error rate by type

## Alerts

### Configured Alerts
- **HighErrorRate**: Error rate > 10%
- **SlowProcessing**: 95th percentile > 120s
- **QueueBacklog**: > 50 messages in queue
- **HighCPUUsage**: > 80%
- **HighMemoryUsage**: > 85%
- **HighDiskUsage**: > 90%
- **KBSyncFailure**: KB sync failures

## Troubleshooting

### Common Issues

#### Docker Permission Denied
```bash
# Fix Docker permissions
sudo usermod -aG docker $USER
newgrp docker
```

#### Port Access Issues
```bash
# Check security groups
aws ec2 describe-security-groups --group-ids sg-xxxxxx
```

#### Service Status
```bash
# Check all services
monitoring-status

# Check individual containers
docker compose -f monitoring/docker/docker-compose.yml ps
```

### Logs
```bash
# Prometheus logs
docker logs prometheus

# Grafana logs
docker logs grafana

# System logs
sudo journalctl -u monitoring.service -f
```

## Maintenance

### Storage Management
- Auto-cleanup runs hourly
- Keeps 7 days of data
- Monitors disk usage > 85%

### Backup
```bash
# Backup Prometheus data
docker exec prometheus tar -czf /tmp/prometheus-backup.tar.gz /prometheus

# Backup Grafana dashboards
docker exec grafana tar -czf /tmp/grafana-backup.tar.gz /var/lib/grafana
```

## Performance Tuning

### Prometheus
- Retention: 15 days
- Storage: 10GB limit
- Scrape interval: 15s

### Grafana
- Refresh interval: 30s
- Cache enabled
- Optimized for PDF processing metrics

## Security
- Change default Grafana password
- Use HTTPS in production
- Restrict security group access
- Regular security updates

## Support
For issues or questions:
1. Check logs: `monitoring-status`
2. Review: `docker compose logs`
3. Check: AWS security groups
4. Verify: Network connectivity
