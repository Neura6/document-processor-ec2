#!/usr/bin/env python3
"""
Start metrics server and verify data collection
"""

import subprocess
import time
import os

def start_metrics():
    print("🚀 Starting metrics server...")
    
    # Start metrics server
    subprocess.Popen(["python", "-m", "monitoring.metrics"])
    time.sleep(3)
    
    # Start SQS worker
    subprocess.Popen(["python", "sqs_worker.py"])
    
    print("\n📊 Metrics available at:")
    print("- Prometheus: http://localhost:9090")
    print("- Grafana: http://localhost:3000")
    print("- Metrics endpoint: http://localhost:8000/metrics")
    
    print("\n🔍 Check metrics:")
    print("curl http://localhost:8000/metrics")

if __name__ == "__main__":
    start_metrics()
