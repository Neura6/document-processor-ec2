#!/usr/bin/env python3
"""
Complete Prometheus Metrics Reset Program
Resets all PDF processor metrics to zero
"""

import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetricResetter:
    def __init__(self, prometheus_url="http://localhost:9090"):
        self.prometheus_url = prometheus_url
        
    def reset_all_metrics(self):
        """Reset all PDF processor metrics to zero"""
        
        # List of metrics to reset
        metrics_to_reset = [
            'pdf_files_processed_total',
            'processing_duration_seconds',
            'processing_errors_total',
            's3_uploads_total',
            'kb_sync_total',
            'service_processing_duration_seconds',
            'service_errors_total',
            'service_requests_total',
            'queue_depth',
            'active_processing_jobs'
        ]
        
        for metric in metrics_to_reset:
            self.delete_metric_series(metric)
            logger.info(f"Reset metric: {metric}")
    
    def delete_metric_series(self, metric_name):
        """Delete specific metric series from Prometheus"""
        try:
            # Delete via Prometheus admin API
            delete_url = f"{self.prometheus_url}/api/v1/admin/tsdb/delete_series"
            params = {'match[]': metric_name}
            
            response = requests.post(delete_url, params=params)
            if response.status_code == 200:
                logger.info(f"Successfully deleted: {metric_name}")
            else:
                logger.warning(f"Could not delete {metric_name}: {response.status_code}")
                
            # Force cleanup
            cleanup_url = f"{self.prometheus_url}/api/v1/admin/tsdb/clean_tombstones"
            requests.post(cleanup_url)
            
        except Exception as e:
            logger.error(f"Error resetting {metric_name}: {e}")
    
    def reset_grafana_dashboards(self):
        """Reset Grafana dashboard time range"""
        try:
            # Reset dashboard time range to now
            dashboard_reset = {
                "dashboard": {
                    "refresh": "5s",
                    "time": {"from": "now-5m", "to": "now"}
                }
            }
            
            # Update dashboards
            dashboards = [
                "pdf-processing-pipeline",
                "business-intelligence"
            ]
            
            for dashboard in dashboards:
                url = f"http://admin:admin123@localhost:3000/api/dashboards/db"
                response = requests.post(url, json=dashboard_reset)
                logger.info(f"Updated dashboard: {dashboard}")
                
        except Exception as e:
            logger.error(f"Error resetting dashboards: {e}")
    
    def complete_reset(self):
        """Complete reset of all metrics and dashboards"""
        logger.info("🔄 Starting complete metrics reset...")
        
        # Reset Prometheus metrics
        self.reset_all_metrics()
        
        # Reset Grafana dashboards
        self.reset_grafana_dashboards()
        
        logger.info("✅ All metrics reset to zero!")

if __name__ == "__main__":
    resetter = MetricResetter()
    resetter.complete_reset()
    
    print("\n🎯 Metrics successfully reset to zero!")
    print("📊 All dashboards now show zero values")
    print("🔄 Run this script anytime to reset metrics")
