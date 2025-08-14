# PDF Processing Monitoring Setup Guide

## Overview
Complete monitoring setup for PDF processing pipeline with Prometheus, Grafana, and Alertmanager.

## Quick Start on EC2

### 1. Setup Monitoring Stack
```bash
# Navigate to monitoring directory
cd /home/ubuntu/pdf-processor/monitoring

# Start monitoring services
docker-compose up -d

# Check status
docker-compose ps
```

### 2. Verify Services
```bash
# Check Prometheus
http://EC2-IP:9090

# Check Grafana
http://EC2-IP:3000
# Login: admin/admin123

# Check Node Exporter
http://EC2-IP:9100/metrics
```

### 3. Dashboard Access
- **Main Dashboard**: http://EC2-IP:3000/d/pdf-pipeline-overview
- **Metrics Available**:
  - PDF processing success/failure rates
  - S3 operations metrics
  - Processing time per step
  - Queue depth monitoring
  - System resource usage

### 4. Troubleshooting

#### Service Not Starting
```bash
# Check logs
docker-compose logs prometheus
docker-compose logs grafana

# Restart services
docker-compose down
docker-compose up -d
```

#### Metrics Not Showing
1. Verify PDF processor is running on port 8001
2. Check Prometheus targets: http://EC2-IP:9090/targets
3. Verify Grafana data source: http://EC2-IP:3000/datasources

#### Common Issues
- **Port conflicts**: Ensure ports 9090, 3000, 9100 are available
- **Permission issues**: Run with sudo if needed
- **Network issues**: Check firewall rules

### 5. Production Commands
```bash
# Start monitoring
start-monitoring

# Check status
production-status

# View logs
sudo journalctl -u pdf-processor-worker -f
```

## Configuration Files
- **Prometheus**: `config/prometheus/prometheus.yml`
- **Grafana**: `config/grafana/provisioning/`
- **Dashboards**: `config/grafana/dashboards/`

## Metrics Endpoints
- **PDF Processor**: http://localhost:8001/metrics
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000
