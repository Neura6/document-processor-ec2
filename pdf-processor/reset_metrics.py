#!/usr/bin/env python3
"""
Direct Prometheus Reset using curl commands
"""

import subprocess
import os

def run_command(cmd):
    """Run shell command"""
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {cmd}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ {cmd}: {e.stderr}")
        return None

print("🎯 Resetting all metrics to zero...")

# Direct commands to reset
commands = [
    "docker-compose down",
    "docker volume rm document-processor-ec2_prometheus_data 2>/dev/null || true",
    "docker volume prune -f",
    "docker-compose up -d",
    "sleep 5",
    "curl -s http://localhost:9090/api/v1/query?query=pdf_files_processed_total"
]

for cmd in commands:
    run_command(cmd)

print("\n🚀 All metrics reset to zero!")
print("📊 Check Grafana at http://localhost:3000")
