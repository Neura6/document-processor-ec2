#!/usr/bin/env python3
"""
System Monitoring Script
Collects system metrics and updates Prometheus metrics
"""

import psutil
import time
import logging
import os
from services.metrics_service import metrics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SystemMonitor:
    """System metrics collector"""
    
    def __init__(self, interval=30):
        self.interval = interval
        
    def collect_metrics(self):
        """Collect and update system metrics"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            metrics.update_system_metrics(cpu_percent, 0, '/', 0)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            
            # Update all metrics
            metrics.update_system_metrics(cpu_percent, memory_percent, '/', disk_percent)
            
            # Log system status
            logger.info(f"System metrics - CPU: {cpu_percent}%, Memory: {memory_percent}%, Disk: {disk_percent}%")
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'disk_percent': disk_percent,
                'disk_free_gb': disk.free / (1024**3),
                'memory_free_gb': memory.available / (1024**3)
            }
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return None
    
    def run(self):
        """Run continuous monitoring"""
        logger.info(f"Starting system monitoring (interval: {self.interval}s)")
        
        while True:
            try:
                self.collect_metrics()
                time.sleep(self.interval)
            except KeyboardInterrupt:
                logger.info("Stopping system monitoring...")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.interval)

if __name__ == "__main__":
    monitor = SystemMonitor()
    monitor.run()
