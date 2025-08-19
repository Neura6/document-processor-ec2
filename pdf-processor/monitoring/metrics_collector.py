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
        # S3 Upload Metrics
        self.s3_uploads_total = Counter('document_s3_uploads_total', 'Total files uploaded to S3', ['bucket', 'status'])
        self.s3_upload_duration = Histogram('document_s3_upload_duration_seconds', 'Time to upload file to S3')
        
        # SQS Metrics
        self.sqs_messages_total = Counter('document_sqs_messages_total', 'Total messages processed from SQS', ['status'])
        self.sqs_message_age = Histogram('document_sqs_message_age_seconds', 'Age of messages in SQS')
        
        # Format Conversion Metrics
        self.conversions_total = Counter('document_conversions_total', 'Total format conversions', ['from_format', 'to_format', 'status'])
        self.conversion_duration = Histogram('document_conversion_duration_seconds', 'Time to convert file format')
        
        # OCR Metrics
        self.ocr_jobs_total = Counter('document_ocr_jobs_total', 'Total OCR jobs processed', ['status'])
        self.ocr_duration = Histogram('document_ocr_duration_seconds', 'Time for OCR processing')
        
        # Chunking Metrics
        self.chunks_created_total = Counter('document_chunks_created_total', 'Total chunks created', ['file_type'])
        self.chunking_duration = Histogram('document_chunking_duration_seconds', 'Time to chunk document')
        
        # S3 Output Metrics
        self.s3_output_uploads_total = Counter('document_s3_output_uploads_total', 'Total uploads to chunked repository', ['status'])
        self.s3_output_duration = Histogram('document_s3_output_duration_seconds', 'Time to upload chunks')
        
        # Knowledge Base Sync Metrics
        self.kb_sync_total = Counter('document_kb_sync_total', 'Total KB sync operations', ['status'])
        self.kb_sync_duration = Histogram('document_kb_sync_duration_seconds', 'Time to sync to KB')
        
        # Overall Processing
        self.files_processed_total = Counter('document_files_processed_total', 'Total files processed', ['status'])
        self.processing_duration = Histogram('document_processing_duration_seconds', 'Total processing time per file')
        
        # Active Jobs
        self.active_jobs = Gauge('document_active_processing_jobs', 'Currently active processing jobs')
        
        # Error Tracking
        self.errors_total = Counter('document_errors_total', 'Total processing errors', ['stage', 'error_type'])
        
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
        self.chunks_created_total.labels(file_type=file_type).inc(chunk_count)
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
        self.active_jobs.inc()
        
    def decrement_active_jobs(self):
        """Decrement active jobs counter."""
        self.active_jobs.dec()

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
