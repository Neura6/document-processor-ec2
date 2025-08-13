# PDF Processor SQS Worker Deployment Guide

## Overview
This guide explains how to deploy the PDF processor as an SQS-driven worker service on EC2.

## Prerequisites
- AWS EC2 instance (Amazon Linux 2 or Ubuntu)
- S3 bucket with event notifications configured to SQS
- SQS queue configured to receive S3 events
- AWS credentials with appropriate permissions

## Step 1: EC2 Instance Setup

### 1.1 Install Dependencies
```bash
# Update system
sudo yum update -y  # Amazon Linux
# or
sudo apt update && sudo apt upgrade -y  # Ubuntu

# Install Python 3 and pip
sudo yum install python3 python3-pip -y  # Amazon Linux
# or
sudo apt install python3 python3-pip -y  # Ubuntu

# Install system dependencies for OCR
sudo yum install tesseract -y  # Amazon Linux
# or
sudo apt install tesseract-ocr -y  # Ubuntu
```

### 1.2 Setup Project
```bash
# Clone or copy your project
cd /home/ec2-user
# Copy your pdf-processor directory here

# Run setup
python3 setup_worker.py
```

## Step 2: AWS Configuration

### 2.1 IAM Role (Recommended)
Create an IAM role with these permissions:
- AmazonS3FullAccess (or specific bucket permissions)
- AmazonSQSFullAccess (or specific queue permissions)
- AmazonBedrockFullAccess (for knowledge base operations)

Attach the role to your EC2 instance.

### 2.2 Environment Variables
Create `/home/ec2-user/pdf-processor/.env`:
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
SOURCE_BUCKET=rules-repository
CHUNKED_BUCKET=chunked-rules-repository
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/your-queue-name
```

## Step 3: Systemd Service Setup

### 3.1 Install Service
```bash
# Copy service file
sudo cp pdf-processor-worker.service /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable service to start on boot
sudo systemctl enable pdf-processor-worker

# Start service
sudo systemctl start pdf-processor-worker

# Check status
sudo systemctl status pdf-processor-worker
```

### 3.2 Service Management Commands
```bash
# View logs
sudo journalctl -u pdf-processor-worker -f

# Restart service
sudo systemctl restart pdf-processor-worker

# Stop service
sudo systemctl stop pdf-processor-worker

# View recent logs
sudo journalctl -u pdf-processor-worker --since "1 hour ago"
```

## Step 4: Testing

### 4.1 Manual Testing
```bash
# Test the worker manually first
cd /home/ec2-user/pdf-processor
source venv/bin/activate
python sqs_worker.py --queue-url YOUR_QUEUE_URL --batch-size 5
```

### 4.2 End-to-End Testing
1. Upload a PDF file to your S3 bucket
2. Check SQS for the event message
3. Verify the worker processes the file
4. Check the logs for processing status
5. Verify chunks appear in the chunked bucket
6. Check knowledge base sync status

## Step 5: Monitoring and Troubleshooting

### 5.1 Log Locations
- Application logs: `/home/ec2-user/pdf-processor/logs/`
- System logs: `sudo journalctl -u pdf-processor-worker`

### 5.2 Common Issues

**Permission Errors:**
- Ensure IAM role has S3 and SQS permissions
- Check .env file has correct credentials

**Processing Failures:**
- Check `sqs_worker.log` for detailed error messages
- Verify S3 bucket names in config
- Ensure knowledge base IDs are correct

**Service Not Starting:**
- Check service status: `sudo systemctl status pdf-processor-worker`
- View logs: `sudo journalctl -u pdf-processor-worker -f`
- Verify .env file exists and is readable

### 5.3 Health Check Script
Create `/home/ec2-user/health_check.sh`:
```bash
#!/bin/bash
if systemctl is-active --quiet pdf-processor-worker; then
    echo "Service is running"
    tail -n 10 /home/ec2-user/pdf-processor/sqs_worker.log
else
    echo "Service is not running"
    sudo systemctl status pdf-processor-worker
fi
```

## Step 6: Scaling Considerations

### 6.1 Multiple Workers
For high volume, consider:
- Multiple EC2 instances with the same setup
- Use SQS FIFO queue for ordered processing
- Implement dead letter queue for failed messages

### 6.2 Auto-scaling
- Use AWS Auto Scaling Groups based on SQS queue depth
- CloudWatch alarms for queue length > 100 messages
- Launch template with the worker setup

## Security Best Practices

1. **Use IAM roles instead of access keys when possible**
2. **Restrict S3 bucket permissions to specific buckets**
3. **Use VPC endpoints for S3 and SQS access**
4. **Enable CloudTrail logging**
5. **Regular security updates**: `sudo yum update -y`

## Quick Start Commands

```bash
# Complete setup in one go
wget https://your-bucket.s3.amazonaws.com/setup_worker.sh
chmod +x setup_worker.sh
./setup_worker.sh
```

## Verification Checklist

- [ ] EC2 instance has IAM role with required permissions
- [ ] .env file configured with correct values
- [ ] systemd service is running
- [ ] Logs show successful startup
- [ ] Test file upload triggers processing
- [ ] Processed chunks appear in chunked bucket
- [ ] Knowledge base sync completes successfully
