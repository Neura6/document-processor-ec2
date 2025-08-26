#!/usr/bin/env python3
"""
Real-time Metrics Dashboard for PDF Processing Pipeline
Quick utility to display live processing metrics
"""

import requests
import time
import os
from datetime import datetime

def get_metric_value(metric_name):
    """Get current value of a metric from Prometheus endpoint"""
    try:
        response = requests.get('http://localhost:8000/metrics')
        lines = response.text.split('\n')
        for line in lines:
            if line.startswith(metric_name):
                if ' ' in line:
                    return float(line.split(' ')[1])
        return 0.0
    except:
        return 0.0

def display_real_time_metrics():
    """Display real-time processing metrics"""
    print("\n" + "="*60)
    print("PDF PROCESSING PIPELINE - REAL-TIME METRICS")
    print("="*60)
    print(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-"*60)
    
    # Queue metrics
    sqs_depth = get_metric_value('sqs_queue_depth')
    active_jobs = get_metric_value('active_processing_jobs')
    
    # Processing state metrics
    conversion_active = get_metric_value('conversion_files_active')
    ocr_active = get_metric_value('ocr_files_active')
    watermark_active = get_metric_value('watermark_files_active')
    chunking_active = get_metric_value('chunking_files_active')
    kb_sync_active = get_metric_value('kb_sync_files_active')
    s3_upload_active = get_metric_value('s3_upload_files_active')
    
    print(f"📊 QUEUE STATUS:")
    print(f"   Files in SQS Queue: {int(sqs_depth)}")
    print(f"   Active Processing Jobs: {int(active_jobs)}")
    
    print(f"\n⚡ REAL-TIME PROCESSING STATES:")
    print(f"   🔄 Format Conversion: {int(conversion_active)} files")
    print(f"   🔍 OCR Processing: {int(ocr_active)} files")
    print(f"   🏷️  Watermark Removal: {int(watermark_active)} files")
    print(f"   ✂️  Document Chunking: {int(chunking_active)} files")
    print(f"   ☁️  S3 Upload: {int(s3_upload_active)} files")
    print(f"   🧠 KB Sync: {int(kb_sync_active)} files")
    
    print("-"*60)
    print("Press Ctrl+C to exit")
    print("="*60)

if __name__ == "__main__":
    print("Starting Real-time Metrics Dashboard...")
    print("Ensure Prometheus metrics server is running on port 8000")
    
    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            display_real_time_metrics()
            time.sleep(2)  # Update every 2 seconds
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
