from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time

# Processing metrics
files_processed_total = Counter('pdf_files_processed_total', 'Total files processed', ['status', 'folder'])
processing_duration = Histogram('pdf_processing_duration_seconds', 'Processing time per file', ['step'])
processing_errors = Counter('pdf_processing_errors_total', 'Total processing errors', ['error_type', 'step'])

# S3 metrics
s3_uploads_total = Counter('s3_uploads_total', 'Total S3 uploads', ['bucket', 'status'])
s3_upload_duration = Histogram('s3_upload_duration_seconds', 'S3 upload duration')

# KB Sync metrics
kb_sync_total = Counter('kb_sync_total', 'Total KB sync attempts', ['folder', 'status'])
kb_sync_duration = Histogram('kb_sync_duration_seconds', 'KB sync duration', ['folder'])
kb_mapping_found = Gauge('kb_mapping_found', 'KB mapping found for folder', ['folder'])

# Queue metrics
queue_depth = Gauge('sqs_queue_depth', 'Current SQS queue depth')
messages_processed = Counter('sqs_messages_processed_total', 'Total SQS messages processed')

# System metrics
active_processing_jobs = Gauge('active_processing_jobs', 'Number of active processing jobs')
processing_rate = Gauge('processing_rate_per_hour', 'Processing rate per hour')

# Real-time processing state metrics
conversion_files_active = Gauge('conversion_files_active', 'Files currently in format conversion')
ocr_files_active = Gauge('ocr_files_active', 'Files currently in OCR processing')
watermark_files_active = Gauge('watermark_files_active', 'Files currently in watermark removal')
chunking_files_active = Gauge('chunking_files_active', 'Files currently being chunked')
kb_sync_files_active = Gauge('kb_sync_files_active', 'Files currently syncing to KB')
s3_upload_files_active = Gauge('s3_upload_files_active', 'Files currently being uploaded to S3')

def start_metrics_server(port=8000):
    """Start Prometheus metrics server"""
    start_http_server(port)
    print(f"Metrics server started on port {port}")

def record_processing_time(step_name, duration):
    """Record processing time for a specific step"""
    processing_duration.labels(step=step_name).observe(duration)

def record_file_processed(status, folder):
    """Record a file processing completion"""
    files_processed_total.labels(status=status, folder=folder).inc()

def record_kb_sync(folder, status, duration=None):
    """Record KB sync attempt"""
    kb_sync_total.labels(folder=folder, status=status).inc()
    if duration:
        kb_sync_duration.labels(folder=folder).observe(duration)
