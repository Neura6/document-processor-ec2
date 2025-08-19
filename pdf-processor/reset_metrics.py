#!/usr/bin/env python3
"""
Simple Prometheus Metrics Reset Program
Resets all PDF processor metrics to zero using Docker commands
"""

import subprocess
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetricResetter:
    def __init__(self):
        pass
    
    def reset_via_docker(self):
        """Reset metrics using Docker commands"""
        logger.info("🔄 Resetting all metrics to zero...")
        
        try:
            # Stop containers
            subprocess.run(["docker-compose", "down"], check=True)
            logger.info("✅ Stopped containers")
            
            # Remove Prometheus data volume
            subprocess.run(["docker", "volume", "rm", "document-processor-ec2_prometheus_data"], 
                          check=True, capture_output=True)
            logger.info("✅ Removed Prometheus data")
            
            # Start fresh
            subprocess.run(["docker-compose", "up", "-d"], check=True)
            logger.info("✅ Started fresh containers")
            
            print("\n🎯 All metrics reset to zero!")
            print("📊 Dashboards now show fresh data")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error: {e}")
            print("🚨 Try: docker volume prune -f")
    
    def manual_reset(self):
        """Manual reset instructions"""
        print("\n🔧 Manual reset commands:")
        print("1. docker-compose down")
        print("2. docker volume prune -f") 
        print("3. docker-compose up -d")
        print("4. python sqs_worker.py")

if __name__ == "__main__":
    resetter = MetricResetter()
    resetter.reset_via_docker()
    resetter.manual_reset()
