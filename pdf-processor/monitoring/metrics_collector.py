import time
import logging
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Document Processing Pipeline Metrics
class DocumentMetrics:
    """Centralized metrics collection for document processing pipeline."""
    
    def __init__(self):
        # Processing metrics (matching metrics.py)
        self.files_processed_total = Counter('pdf_files_processed_total', 'Total files processed', ['status', 'folder'])
        self.processing_duration = Histogram('pdf_processing_duration_seconds', 'Processing time per file', ['step'])
        self.processing_errors = Counter('pdf_processing_errors_total', 'Total processing errors', ['error_type', 'step'])
        
        # S3 metrics (matching metrics.py)
        self.s3_uploads_total = Counter('s3_uploads_total', 'Total S3 uploads', ['bucket', 'status'])
        self.s3_upload_duration = Histogram('s3_upload_duration_seconds', 'S3 upload duration')
        
        # KB Sync metrics (matching metrics.py)
        self.kb_sync_total = Counter('kb_sync_total', 'Total KB sync attempts', ['folder', 'status'])
        self.kb_sync_duration = Histogram('kb_sync_duration_seconds', 'KB sync duration', ['folder'])
        self.kb_mapping_found = Gauge('kb_mapping_found', 'KB mapping found for folder', ['folder'])
        
        # File tracking metrics - YOUR NEW REQUIREMENTS (imported from metrics.py)
        from monitoring.metrics import files_uploaded_total, chunks_created_total, files_pending_sync, kb_sync_success_total
        self.files_uploaded_total = files_uploaded_total
        self.chunks_created_total = chunks_created_total
        self.files_pending_sync = files_pending_sync
        self.kb_sync_success_total = kb_sync_success_total
        
        # Real-time SQS Queue metrics
        self.sqs_messages_available = Gauge('sqs_messages_available', 'Messages currently in SQS queue')
        self.sqs_messages_in_flight = Gauge('sqs_messages_in_flight', 'Messages being processed by EC2')
        self.messages_processed = Counter('sqs_messages_processed_total', 'Total SQS messages processed')
        
        # Real-time Processing Stage metrics
        self.files_in_conversion = Gauge('files_in_conversion', 'Files currently being converted')
        self.files_in_ocr = Gauge('files_in_ocr', 'Files currently in OCR processing')
        self.files_in_chunking = Gauge('files_in_chunking', 'Files currently being chunked')
        self.files_in_kb_sync = Gauge('files_in_kb_sync', 'Files currently syncing to KB')
        
        # Pipeline overview
        self.pipeline_stage_files = Gauge('pipeline_stage_files', 'Files in each pipeline stage', ['stage'])
        
        # System metrics
        self.active_processing_jobs = Gauge('active_processing_jobs', 'Number of active processing jobs')
        self.processing_rate = Gauge('processing_rate_per_hour', 'Processing rate per hour')
        
        # Additional detailed metrics for advanced monitoring
        self.conversions_total = Counter('document_conversions_total', 'Total format conversions', ['from_format', 'to_format', 'status'])
        self.conversion_duration = Histogram('document_conversion_duration_seconds', 'Time to convert file format')
        self.ocr_jobs_total = Counter('document_ocr_jobs_total', 'Total OCR jobs processed', ['status'])
        self.ocr_duration = Histogram('document_ocr_duration_seconds', 'Time for OCR processing')
        self.chunking_duration = Histogram('document_chunking_duration_seconds', 'Time to chunk document')
        
    def record_s3_upload(self, bucket: str, duration: float, success: bool = True):
        """Record S3 upload metrics."""
        status = 'success' if success else 'failed'
        self.s3_uploads_total.labels(bucket=bucket, status=status).inc()
        self.s3_upload_duration.observe(duration)
        
    def record_conversion(self, from_format: str, to_format: str, duration: float, success: bool = True):
        """Record format conversion metrics."""
        status = 'success' if success else 'failed'
        self.conversions_total.labels(from_format=from_format, to_format=to_format, status=status).inc()
        self.conversion_duration.observe(duration)
        
    def record_ocr_job(self, duration: float, success: bool = True):
        """Record OCR processing metrics."""
        status = 'success' if success else 'failed'
        self.ocr_jobs_total.labels(status=status).inc()
        self.ocr_duration.observe(duration)
        
    def record_chunking(self, chunk_count: int, duration: float, file_type: str = 'pdf'):
        """Record chunking metrics."""
        # Note: This method is for backward compatibility
        self.chunking_duration.observe(duration)
        
    def record_s3_output_upload(self, duration: float, success: bool = True):
        """Record S3 output upload metrics."""
        status = 'success' if success else 'failed'
        self.s3_output_uploads_total.labels(status=status).inc()
        self.s3_output_duration.observe(duration)
        
    def record_kb_sync(self, duration: float, success: bool = True):
        """Record KB sync metrics."""
        status = 'success' if success else 'failed'
        self.kb_sync_total.labels(status=status).inc()
        self.kb_sync_duration.observe(duration)
        
    def record_file_processing(self, duration: float, success: bool = True):
        """Record overall file processing metrics."""
        status = 'success' if success else 'failed'
        self.files_processed_total.labels(status=status).inc()
        self.processing_duration.observe(duration)
        
    def record_error(self, stage: str, error_type: str):
        """Record processing errors."""
        self.errors_total.labels(stage=stage, error_type=error_type).inc()
        
    def increment_active_jobs(self):
        """Increment active jobs counter."""
        self.active_processing_jobs.inc()
        
    def decrement_active_jobs(self):
        """Decrement active jobs counter."""
        self.active_processing_jobs.dec()
    
    # NEW HELPER METHODS FOR FILE TRACKING
    def record_file_uploaded(self, folder: str):
        """Record a file uploaded to source bucket"""
        self.files_uploaded_total.labels(folder=folder).inc()
    
    def record_chunks_created(self, folder: str, chunk_count: int):
        """Record PDF chunks created"""
        self.chunks_created_total.labels(folder=folder).inc(chunk_count)
    
    def record_kb_sync_success(self, folder: str):
        """Record successful KB sync"""
        self.kb_sync_success_total.labels(folder=folder).inc()
    
    def update_pending_sync_count(self, folder: str, count: int):
        """Update the count of files pending KB sync"""
        self.files_pending_sync.labels(folder=folder).set(count)
    
    def record_processing_time(self, step_name: str, duration: float):
        """Record processing time for a specific step"""
        self.processing_duration.labels(step=step_name).observe(duration)
    
    def record_file_processed(self, status: str, folder: str):
        """Record a file processing completion"""
        self.files_processed_total.labels(status=status, folder=folder).inc()
    
    def record_kb_sync_attempt(self, folder: str, status: str, duration: float = None):
        """Record KB sync attempt"""
        self.kb_sync_total.labels(folder=folder, status=status).inc()
        if duration:
            self.kb_sync_duration.labels(folder=folder).observe(duration)

# Global metrics instance
metrics = DocumentMetrics()

def start_metrics_server(port: int = 8000):
    """Start the Prometheus metrics server."""
    start_http_server(port)
    logger.info(f"Metrics server started on port {port}")

if __name__ == "__main__":
    start_metrics_server()
    while True:
        time.sleep(1)
