# PDF Processing Pipeline

Production-grade PDF processing with real-time monitoring on EC2 Ubuntu.

## 🚀 Complete System
- **S3 → SQS → Processing → Knowledge Base** pipeline
- **Real-time monitoring** with Prometheus + Grafana
- **Auto-scaling** and error handling
- **16 AWS Bedrock Knowledge Bases** sync

## 📊 Quick Start

```bash
# Deploy on EC2
cd ~/document-processor-ec2/pdf-processor/monitoring/docker
docker-compose up -d

# Start processing
sudo systemctl start pdf-processor-worker pdf-metrics

# Access dashboards
# Grafana: http://YOUR-EC2-IP:3000 (admin/admin123)
# Prometheus: http://YOUR-EC2-IP:9090
```

## 🎯 Features
- **PDF Processing**: 7-step pipeline (convert→clean→watermark→OCR→chunk→upload→sync)
- **SQS Integration**: Event-driven from S3 uploads
- **Monitoring**: Real-time dashboards
- **Auto-retry**: Failed processing recovery
- **Production-ready**: Systemd services

## 🔧 Commands
```bash
start-production        # Start everything
production-status       # Check status
sudo journalctl -u pdf-processor-worker -f  # View logs
```

## 📁 Structure
- `services/` - Core processing microservices
- `monitoring/` - Prometheus, Grafana, Alertmanager
- `config/` - AWS, S3, SQS configurations
- `deployment/` - Production deployment scripts
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
