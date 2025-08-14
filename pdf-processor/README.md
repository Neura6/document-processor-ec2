# PDF Processing Pipeline with Monitoring

A comprehensive PDF processing pipeline with real-time monitoring using Prometheus and Grafana.

## 🚀 Features

- **PDF Processing**: Convert, clean, watermark, OCR, chunk, and upload PDFs
- **SQS Integration**: Automatic processing from S3 uploads
- **Real-time Monitoring**: Prometheus + Grafana dashboards
- **Auto-scaling**: Storage management and error handling
- **Knowledge Base**: Automatic sync to vector database

## 📁 Project Structure

```
pdf-processor/
├── services/                 # Core processing services
│   ├── __init__.py
│   ├── metrics_service.py    # Prometheus metrics
│   ├── orchestrator_instrumented.py  # Main processing pipeline
│   └── sqs_worker_metrics.py # SQS worker with metrics
├── monitoring/              # Monitoring stack
│   ├── docker/
│   │   └── docker-compose.yml
│   ├── config/
│   │   ├── prometheus/
│   │   ├── grafana/
│   │   └── alertmanager/
│   ├── scripts/
│   │   └── setup-ubuntu.sh
│   └── docs/
│       └── MONITORING-DEPLOYMENT.md
├── config/                  # Configuration files
└── deployment/              # Deployment scripts
```

## 🛠️ Quick Start

### 1. Setup Monitoring
```bash
# Run setup script
chmod +x monitoring/scripts/setup-ubuntu.sh
./monitoring/scripts/setup-ubuntu.sh
```

### 2. Start Services
```bash
# Start monitoring stack
start-monitoring

# Check status
monitoring-status
```

### 3. Access Dashboards
- **Grafana**: http://your-ec2-ip:3000 (admin/admin123)
- **Prometheus**: http://your-ec2-ip:9090

## 📊 Monitoring Metrics

### PDF Processing
- Files processed per minute
- Success/failure rates
- Processing time per step
- Queue depth
- Error rates by type

### System Resources
- CPU, memory, disk usage
- Network I/O
- Docker container metrics

## 🔧 Commands

### Service Management
```bash
start-monitoring      # Start all services
stop-monitoring       # Stop all services
monitoring-status     # Check service status
```

### Development
```bash
# Process files manually
python services/orchestrator_instrumented.py /path/to/pdfs/

# Start SQS worker
python services/sqs_worker_metrics.py
```

## 🌐 AWS Setup

### Security Groups
Ensure your EC2 security group allows:
- Port 3000 (Grafana)
- Port 9090 (Prometheus)
- Port 22 (SSH)

### Environment Variables
```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
```

## 📈 Dashboards

### PDF Processing Pipeline Overview
- Real-time processing metrics
- System resource usage
- Error tracking and alerts
- Queue monitoring

## 🚨 Alerts

Configured alerts for:
- High error rates (>10%)
- Slow processing (>120s)
- Queue backlog (>50 messages)
- High resource usage (>80%)

## 🔍 Troubleshooting

### Common Issues
1. **Docker permissions**: `sudo usermod -aG docker $USER`
2. **Port access**: Check AWS security groups
3. **Service logs**: `docker compose -f monitoring/docker/docker-compose.yml logs`

### Support
- Check `monitoring-status` for service health
- Review logs in Grafana dashboard
- Verify AWS credentials and permissions

## 📝 Development

### Adding New Metrics
1. Add metric in `services/metrics_service.py`
2. Update Prometheus configuration
3. Add to Grafana dashboard

### Testing
```bash
# Test monitoring locally
docker compose -f monitoring/docker/docker-compose.yml up

# Test PDF processing
python services/orchestrator_instrumented.py test-pdfs/
```

## 🎯 Next Steps

1. **Customize dashboards** for your specific needs
2. **Set up alerts** for your team
3. **Add custom metrics** for business requirements
4. **Scale horizontally** with multiple workers
5. **Implement CI/CD** for automated deployments
