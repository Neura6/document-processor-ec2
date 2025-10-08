from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time
import re

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

# File tracking metrics - NEW ADDITIONS
files_uploaded_total = Counter('files_uploaded_total', 'Total files uploaded to source bucket', ['folder'])
chunks_created_total = Counter('chunks_created_total', 'Total PDF chunks created', ['folder'])
processed_chunks_created_total = Counter('processed_chunks_created_total', 'Total processed PDF chunks created', ['folder'])
direct_chunks_created_total = Counter('direct_chunks_created_total', 'Total direct PDF chunks created', ['folder'])
files_pending_sync = Gauge('files_pending_kb_sync', 'Files waiting for KB sync', ['folder'])
kb_sync_success_total = Counter('kb_sync_success_total', 'Successfully synced files to KB', ['folder'])

# Real-time SQS Queue metrics
sqs_messages_available = Gauge('sqs_messages_available', 'Messages currently in SQS queue')
sqs_messages_in_flight = Gauge('sqs_messages_in_flight', 'Messages being processed by EC2')
messages_processed = Counter('sqs_messages_processed_total', 'Total SQS messages processed')

# Real-time Processing Stage metrics
files_in_conversion = Gauge('files_in_conversion', 'Files currently being converted')
files_in_ocr = Gauge('files_in_ocr', 'Files currently in OCR processing')
files_in_chunking = Gauge('files_in_chunking', 'Files currently being chunked')
files_in_kb_sync = Gauge('files_in_kb_sync', 'Files currently syncing to KB')

# Pipeline overview
pipeline_stage_files = Gauge('pipeline_stage_files', 'Files in each pipeline stage', ['stage'])

# System metrics
active_processing_jobs = Gauge('active_processing_jobs', 'Number of active processing jobs')
processing_rate = Gauge('processing_rate_per_hour', 'Processing rate per hour')

def start_metrics_server(port=8000):
    """Start Prometheus metrics server"""
    start_http_server(port)
    print(f"Metrics server started on port {port}")

def sanitize_label_value(value):
    """Sanitize label values for Prometheus compatibility"""
    if not value:
        return "default"
    # Replace spaces and special characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(value))
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    return sanitized or "default"

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

# NEW HELPER FUNCTIONS
def record_file_uploaded(folder):
    """Record a file uploaded to source bucket"""
    files_uploaded_total.labels(folder=folder).inc()

def record_chunks_created(folder, chunk_count):
    """Record PDF chunks created"""
    chunks_created_total.labels(folder=folder).inc(chunk_count)

def record_kb_sync_success(folder):
    """Record successful KB sync"""
    kb_sync_success_total.labels(folder=folder).inc()

def update_pending_sync_count(folder, count):
    """Update the count of files pending KB sync"""
    files_pending_sync.labels(folder=folder).set(count)
