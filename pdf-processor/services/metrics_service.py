"""
Metrics Service for Prometheus integration
Provides centralized metrics collection for PDF processing pipeline
"""

from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time
import logging

logger = logging.getLogger(__name__)

class MetricsService:
    """Centralized metrics collection for PDF processing pipeline"""
    
    def __init__(self, port=8000):
        """Initialize metrics service"""
        self.port = port
        self.setup_metrics()
        self.start_server()
        logger.info(f"Metrics service started on port {port}")
    
    def setup_metrics(self):
        """Setup all Prometheus metrics"""
        
        # File processing metrics
        self.files_processed_total = Counter(
            'pdf_files_processed_total',
            'Total files processed',
            ['status', 'folder', 'step']
        )
        
        self.processing_errors_total = Counter(
            'pdf_processing_errors_total',
            'Processing errors by step and type',
            ['step', 'error_type', 'folder']
        )
        
        # Step-level metrics
        self.processing_duration = Histogram(
            'pdf_processing_duration_seconds',
            'Time to process each step',
            ['step', 'folder']
        )
        
        # S3 metrics
        self.s3_operations_total = Counter(
            's3_operations_total',
            'S3 operations by type and status',
            ['operation', 'status', 'bucket']
        )
        
        self.s3_operation_duration = Histogram(
            's3_operation_duration_seconds',
            'S3 operation duration',
            ['operation', 'bucket']
        )
        
        # KB sync metrics
        self.kb_sync_total = Counter(
            'kb_sync_total',
            'KB sync attempts by status and folder',
            ['status', 'folder']
        )
        
        self.kb_sync_duration = Histogram(
            'kb_sync_duration_seconds',
            'KB sync operation duration',
            ['folder']
        )
        
        # Queue metrics
        self.queue_messages = Gauge(
            'sqs_messages_in_queue',
            'Messages currently in SQS queue',
            ['queue_name']
        )
        
        self.processing_files = Gauge(
            'processing_files_current',
            'Files currently being processed'
        )
        
        # System metrics
        self.system_cpu_percent = Gauge(
            'system_cpu_percent',
            'CPU usage percentage'
        )
        
        self.system_memory_percent = Gauge(
            'system_memory_percent',
            'Memory usage percentage'
        )
        
        self.system_disk_percent = Gauge(
            'system_disk_percent',
            'Disk usage percentage',
            ['mount_point']
        )
        
        # Business metrics
        self.files_per_hour = Gauge(
            'pdf_files_per_hour',
            'Files processed per hour',
            ['folder']
        )
        
        self.folder_processing_volume = Gauge(
            'folder_processing_volume_total',
            'Total files processed by folder',
            ['folder']
        )
    
    def start_server(self):
        """Start Prometheus metrics server"""
        start_http_server(self.port)
    
    def record_file_processed(self, status, folder, step):
        """Record file processing completion"""
        self.files_processed_total.labels(status=status, folder=folder, step=step).inc()
    
    def record_error(self, step, error_type, folder):
        """Record processing error"""
        self.processing_errors_total.labels(step=step, error_type=error_type, folder=folder).inc()
    
    def record_processing_time(self, step, folder, duration):
        """Record processing time for a step"""
        self.processing_duration.labels(step=step, folder=folder).observe(duration)
    
    def record_s3_operation(self, operation, status, bucket, duration=None):
        """Record S3 operation"""
        self.s3_operations_total.labels(operation=operation, status=status, bucket=bucket).inc()
        if duration:
            self.s3_operation_duration.labels(operation=operation, bucket=bucket).observe(duration)
    
    def record_kb_sync(self, status, folder, duration=None):
        """Record KB sync operation"""
        self.kb_sync_total.labels(status=status, folder=folder).inc()
        if duration:
            self.kb_sync_duration.labels(folder=folder).observe(duration)
    
    def update_queue_messages(self, queue_name, count):
        """Update queue message count"""
        self.queue_messages.labels(queue_name=queue_name).set(count)
    
    def update_processing_files(self, count):
        """Update currently processing files count"""
        self.processing_files.set(count)
    
    def update_system_metrics(self, cpu, memory, disk_mount, disk_percent):
        """Update system metrics"""
        self.system_cpu_percent.set(cpu)
        self.system_memory_percent.set(memory)
        self.system_disk_percent.labels(mount_point=disk_mount).set(disk_percent)

# Global metrics instance
metrics = MetricsService()
