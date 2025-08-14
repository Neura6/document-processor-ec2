"""
Metrics Service for Prometheus Integration
Collects metrics from PDF processing pipeline
"""

from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time
import logging

class MetricsService:
    def __init__(self, port=8000):
        self.logger = logging.getLogger(__name__)
        self.setup_metrics()
        start_http_server(port)
        self.logger.info(f"Metrics server started on port {port}")
    
    def setup_metrics(self):
        """Setup all Prometheus metrics"""
        
        # Processing metrics
        self.files_processed = Counter(
            'pdf_files_processed_total',
            'Total PDF files processed',
            ['status', 'folder']
        )
        
        self.processing_errors = Counter(
            'pdf_processing_errors_total',
            'Processing errors by step and type',
            ['step', 'error_type']
        )
        
        self.processing_duration = Histogram(
            'pdf_processing_duration_seconds',
            'Time to process a PDF file',
            ['step']
        )
        
        # S3 metrics
        self.s3_operations = Counter(
            's3_operations_total',
            'S3 API calls',
            ['operation', 'status']
        )
        
        self.s3_upload_duration = Histogram(
            's3_upload_duration_seconds',
            'Time to upload to S3',
            ['bucket']
        )
        
        # KB sync metrics
        self.kb_sync = Counter(
            'kb_sync_total',
            'KB sync attempts',
            ['status', 'folder']
        )
        
        self.kb_sync_duration = Histogram(
            'kb_sync_duration_seconds',
            'Time for KB sync operation'
        )
        
        # System metrics
        self.active_processing = Gauge(
            'processing_files_current',
            'Files currently being processed'
        )
        
        self.queue_depth = Gauge(
            'sqs_messages_in_queue',
            'Messages in SQS queue'
        )
    
    def record_file_processed(self, folder, success=True):
        """Record file processing completion"""
        status = 'success' if success else 'failed'
        self.files_processed.labels(status=status, folder=folder).inc()
    
    def record_processing_step(self, step, duration, success=True):
        """Record processing step completion"""
        self.processing_duration.labels(step=step).observe(duration)
        if not success:
            self.processing_errors.labels(step=step, error_type='processing_error').inc()
    
    def record_s3_operation(self, operation, success=True, duration=0):
        """Record S3 operation"""
        status = 'success' if success else 'failed'
        self.s3_operations.labels(operation=operation, status=status).inc()
        if duration > 0:
            self.s3_upload_duration.labels(bucket='source').observe(duration)
    
    def record_kb_sync(self, folder, success=True, duration=0):
        """Record KB sync operation"""
        status = 'success' if success else 'failed'
        self.kb_sync.labels(status=status, folder=folder).inc()
        if duration > 0:
            self.kb_sync_duration.observe(duration)
    
    def set_active_processing(self, count):
        """Set current active processing count"""
        self.active_processing.set(count)
    
    def set_queue_depth(self, count):
        """Set current queue depth"""
        self.queue_depth.set(count)

# Global instance
metrics = MetricsService()
