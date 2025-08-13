# 📊 PDF Processing Monitoring Deployment Guide

## 🚀 Quick Start (5 minutes)

### **Step 1: Copy Files to EC2**
```bash
# From your local machine, copy all files to EC2
scp -i your-key.pem -r pdf-processor/ ec2-user@your-ec2-ip:/home/ec2-user/
```

### **Step 2: Run Setup Script**
```bash
# SSH into EC2
ssh -i your-key.pem ec2-user@your-ec2-ip

# Navigate to directory
cd /home/ec2-user/pdf-processor

# Make setup script executable
chmod +x setup-monitoring.sh

# Run setup
./setup-monitoring.sh
```

### **Step 3: Start Monitoring**
```bash
# Start all monitoring services
start-monitoring

# Verify services are running
docker-compose ps
```

## 📋 Complete Setup Instructions

### **1. Prerequisites**
- EC2 instance (t3.medium or larger recommended)
- Docker and Docker Compose
- AWS credentials configured
- 20GB+ available disk space

### **2. File Structure After Setup**
```
/opt/monitoring/
├── data/
│   ├── prometheus/          # Prometheus data
│   └── grafana/            # Grafana data
├── config/
│   ├── prometheus.yml      # Prometheus config
│   ├── alert_rules.yml     # Alert rules
│   └── grafana/            # Grafana dashboards
├── scripts/
│   ├── auto-resize.sh      # Storage auto-scaling
│   ├── import-dashboards.sh # Dashboard import
│   ├── start-monitoring.sh # Start script
│   └── stop-monitoring.sh  # Stop script
└── docker-compose.yml      # Service definitions
```

### **3. Access URLs**
- **Grafana**: `http://your-ec2-ip:3000`
- **Prometheus**: `http://your-ec2-ip:9090`
- **Default credentials**: Username: `admin`, Password: `admin123`

### **4. Available Dashboards**

#### **📊 PDF Pipeline Overview**
- Files processed today
- Success rate
- Average processing time
- Queue depth
- Real-time processing charts

#### **🔍 Step-by-Step Analysis**
- Conversion metrics
- OCR performance
- Chunking statistics
- S3 upload success rates
- KB sync performance

#### **🖥️ System Health**
- CPU, memory, disk usage
- Network I/O
- Docker container status

#### **📈 Business Intelligence**
- Processing volume trends
- Folder performance comparison
- Error analysis
- Growth predictions

### **5. Metrics Collection**

#### **Application Metrics**
- `pdf_files_processed_total{status, folder, step}`
- `pdf_processing_duration_seconds{step, folder}`
- `pdf_processing_errors_total{step, error_type, folder}`

#### **Infrastructure Metrics**
- `system_cpu_percent`
- `system_memory_percent`
- `system_disk_percent{mount_point}`
- `sqs_messages_in_queue{queue_name}`

#### **Business Metrics**
- `pdf_files_per_hour{folder}`
- `folder_processing_volume_total{folder}`
- `kb_sync_total{status, folder}`

### **6. Alerting**

#### **Active Alerts**
- **High Error Rate**: >10% error rate
- **Slow Processing**: >120s processing time
- **Queue Backlog**: >50 messages in queue
- **High Resource Usage**: CPU >80%, Memory >85%, Disk >90%

#### **Alert Webhooks**
Configure in `config/alert_rules.yml` to send to:
- Slack
- Email
- PagerDuty
- Custom webhooks

### **7. Storage Auto-Scaling**

#### **Automatic Cleanup**
- Runs every 5 minutes
- Cleans Docker system
- Removes old Prometheus data
- Monitors disk usage

#### **Manual Scaling**
```bash
# Check current usage
df -h /opt/monitoring

# Manual cleanup
docker system prune -f

# Resize EBS volume (if needed)
aws ec2 modify-volume --volume-id vol-xxxxx --size 50
```

### **8. Service Management**

#### **Start All Services**
```bash
start-monitoring
```

#### **Stop All Services**
```bash
stop-monitoring
```

#### **Check Status**
```bash
docker-compose ps
docker-compose logs prometheus
docker-compose logs grafana
```

#### **Restart Services**
```bash
docker-compose restart prometheus
docker-compose restart grafana
```

### **9. Customization**

#### **Add New Metrics**
```python
# In your service
from services.metrics_service import metrics

# Record custom metric
metrics.record_custom_metric('my_metric', value, labels={'label': 'value'})
```

#### **Modify Dashboards**
1. Access Grafana at `http://your-ec2-ip:3000`
2. Edit existing dashboards
3. Export JSON and save to `config/grafana/dashboards/`
4. Run `/opt/monitoring/scripts/import-dashboards.sh`

#### **Change Retention**
```bash
# Edit docker-compose.yml
# Change --storage.tsdb.retention.time and --storage.tsdb.retention.size
# Restart Prometheus
docker-compose restart prometheus
```

### **10. Troubleshooting**

#### **Common Issues**

**Port 3000/9090 not accessible:**
```bash
# Check security group
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxx \
  --protocol tcp \
  --port 3000 \
  --cidr 0.0.0.0/0

aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxx \
  --protocol tcp \
  --port 9090 \
  --cidr 0.0.0.0/0
```

**Docker permissions:**
```bash
sudo usermod -a -G docker $USER
# Log out and log back in
```

**Storage full:**
```bash
# Check usage
df -h

# Clean up
docker system prune -a
sudo rm -rf /opt/monitoring/data/prometheus/*
```

### **11. Performance Tuning**

#### **Resource Limits**
- **CPU**: Monitor via Grafana dashboard
- **Memory**: 2GB minimum, 4GB recommended
- **Disk**: 20GB minimum, 50GB recommended for 30-day retention

#### **Scaling Options**
- **Vertical**: Increase EC2 instance size
- **Horizontal**: Add more instances with load balancer
- **Storage**: Use EBS gp3 volumes with auto-scaling

### **12. Security**

#### **Access Control**
- Change default Grafana password
- Use VPC security groups
- Enable HTTPS with ALB
- Use IAM roles instead of access keys

#### **Backup Strategy**
```bash
# Backup Grafana dashboards
docker exec grafana grafana-cli admin export-dashboard --dashboard="PDF Pipeline Overview"

# Backup Prometheus data
tar -czf prometheus-backup.tar.gz /opt/monitoring/data/prometheus/
```

## 🎯 Next Steps

1. **Deploy**: Follow Quick Start section
2. **Configure**: Update alert rules and dashboards
3. **Monitor**: Set up notifications
4. **Scale**: Adjust resources based on usage
5. **Optimize**: Fine-tune retention and performance

## 📞 Support

For issues or questions:
1. Check logs: `docker-compose logs`
2. Check system resources: `htop`, `df -h`
3. Review Grafana dashboards for performance insights
4. Check AWS CloudWatch for infrastructure metrics
