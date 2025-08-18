#!/bin/bash

# Create systemd service for PDF processor worker
sudo tee /etc/systemd/system/pdf-processor-worker.service > /dev/null <<EOF
[Unit]
Description=PDF Processing SQS Worker
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/document-processor-ec2/pdf-processor
Environment=PATH=/home/ubuntu/document-processor-ec2/venv/bin
Environment=PYTHONPATH=/home/ubuntu/document-processor-ec2/pdf-processor
ExecStart=/home/ubuntu/document-processor-ec2/venv/bin/python sqs_worker.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Create systemd service for Docker monitoring stack
sudo tee /etc/systemd/system/pdf-monitoring.service > /dev/null <<EOF
[Unit]
Description=PDF Monitoring Stack (Prometheus + Grafana)
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/ubuntu/document-processor-ec2/pdf-processor
ExecStart=/usr/bin/docker-compose up -d
ExecStop=/usr/bin/docker-compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and enable services
sudo systemctl daemon-reload
sudo systemctl enable pdf-processor-worker
sudo systemctl enable pdf-monitoring

# Start services
sudo systemctl start pdf-processor-worker
sudo systemctl start pdf-monitoring

echo "✅ Systemd services created and started!"
echo "📊 Check status: sudo systemctl status pdf-processor-worker"
echo "📈 Check monitoring: sudo systemctl status pdf-monitoring"
