import time
import logging
import socketserver
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time
# Import shared metrics from metrics.py
from monitoring import metrics as shared_metrics
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilentHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Custom HTTP server that silently handles connection reset errors."""
    
    def handle_error(self, request, client_address):
        """Override to silently handle ConnectionResetError."""
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        
        # Only log non-connection reset errors
        if not isinstance(exc_value, (ConnectionResetError, BrokenPipeError)):
            logger.error(f"HTTP server error from {client_address}: {exc_value}")
        # Silently ignore connection reset errors as they're normal

# Document Processing Pipeline Metrics
class DocumentMetrics:
    """Centralized metrics collection for document processing pipeline."""
    
    def __init__(self):
        # Use ALL shared metrics from metrics.py to avoid duplicates
        self.files_processed_total = shared_metrics.files_processed_total
        self.processing_duration = shared_metrics.processing_duration
        self.processing_errors = shared_metrics.processing_errors
        
        # S3 metrics (using shared instances)
        self.s3_uploads_total = shared_metrics.s3_uploads_total
        self.s3_upload_duration = shared_metrics.s3_upload_duration
        
        # KB Sync metrics (using shared instances)
        self.kb_sync_total = shared_metrics.kb_sync_total
        self.kb_sync_duration = shared_metrics.kb_sync_duration
        self.kb_mapping_found = shared_metrics.kb_mapping_found
        
        # File tracking metrics - YOUR NEW REQUIREMENTS (using shared instances)
        self.files_uploaded_total = shared_metrics.files_uploaded_total
        self.chunks_created_total = shared_metrics.chunks_created_total
        self.processed_chunks_created_total = shared_metrics.processed_chunks_created_total
        self.direct_chunks_created_total = shared_metrics.direct_chunks_created_total
        self.files_pending_sync = shared_metrics.files_pending_sync
        self.kb_sync_success_total = shared_metrics.kb_sync_success_total
        
        # Real-time SQS Queue metrics (using shared instances)
        self.sqs_messages_available = shared_metrics.sqs_messages_available
        self.sqs_messages_in_flight = shared_metrics.sqs_messages_in_flight
        self.messages_processed = shared_metrics.messages_processed
        
        # Real-time Processing Stage metrics (using shared instances)
        self.files_in_conversion = shared_metrics.files_in_conversion
        self.files_in_ocr = shared_metrics.files_in_ocr
        self.files_in_chunking = shared_metrics.files_in_chunking
        self.files_in_kb_sync = shared_metrics.files_in_kb_sync
        
        # Pipeline overview (using shared instances)
        self.pipeline_stage_files = shared_metrics.pipeline_stage_files
        
        # System metrics (using shared instances)
        self.active_processing_jobs = shared_metrics.active_processing_jobs
        self.processing_rate = shared_metrics.processing_rate
        
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
        self.s3_uploads_total.labels(bucket='chunked-rules-repository', status=status).inc()
        self.s3_upload_duration.observe(duration)
        
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
        self.processing_errors.labels(stage=stage, error_type=error_type).inc()
        
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
    """Start the Prometheus metrics server with connection error handling."""
    try:
        # Monkey patch the socketserver to handle connection errors silently
        original_handle_error = socketserver.BaseServer.handle_error
        
        def silent_handle_error(self, request, client_address):
            """Handle errors silently for connection resets."""
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            
            # Only log non-connection reset errors
            if not isinstance(exc_value, (ConnectionResetError, BrokenPipeError, OSError)):
                original_handle_error(self, request, client_address)
            # Silently ignore connection reset errors
        
        socketserver.BaseServer.handle_error = silent_handle_error
        
        start_http_server(port)
        logger.info(f"Metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
        # Continue without metrics server rather than crashing

if __name__ == "__main__":
    start_metrics_server()
    while True:
        time.sleep(1)
