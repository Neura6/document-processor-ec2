"""
Complete Fixed Orchestrator Service with Working Metrics
"""

import sys
import boto3
import io
import os
import time
import logging
from typing import List, Dict, Any
from urllib.parse import unquote_plus
import PyPDF2
from services.filename_service import FilenameService
from services.watermark_service import WatermarkService
from services.ocr_service import OCRService
from services.chunking_service import ChunkingService
from services.s3_service import S3Service
from services.conversion_service import ConversionService
from prometheus_client import Counter, Histogram, Gauge
from monitoring.metrics import (
    record_processing_time, record_file_processed, record_kb_sync,
    active_processing_jobs, processing_duration,
    conversion_files_active, ocr_files_active, watermark_files_active,
    chunking_files_active, kb_sync_files_active, s3_upload_files_active
)
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SOURCE_BUCKET, CHUNKED_BUCKET

# Document Pipeline Metrics - EXACT MATCHES FOR DASHBOARD
s3_uploads_total = Counter('document_s3_uploads_total', 'Total files uploaded to S3', ['bucket', 'status'])
document_conversions_total = Counter('document_conversions_total', 'Total format conversions', ['from_format', 'to_format', 'status'])
ocr_processing_total = Counter('document_ocr_processing_total', 'Total OCR processing jobs', ['status'])
chunks_created_total = Counter('document_chunks_created_total', 'Total chunks created')
kb_sync_operations_total = Counter('document_kb_sync_operations_total', 'Total KB sync operations', ['status'])
processing_duration_seconds = Histogram('document_processing_duration_seconds', 'Total processing time per file', ['stage'])
files_processed_total = Counter('document_files_processed_total', 'Total files processed', ['status'])
active_processing_jobs = Gauge('document_active_processing_jobs', 'Currently active processing jobs')
processing_errors_total = Counter('document_processing_errors_total', 'Total processing errors', ['stage', 'error_type'])

class Orchestrator:
    """Main orchestrator for PDF processing workflow."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.conversion_service = ConversionService()  # FIXED
        self.filename_service = FilenameService()
        self.watermark_service = WatermarkService()
        self.ocr_service = OCRService()
        self.chunking_service = ChunkingService()
        self.s3_service = S3Service(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
        # Initialize S3 constants
        self.source_bucket = SOURCE_BUCKET
        self.chunked_bucket = CHUNKED_BUCKET
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('pdf_processor.log')
            ]
        )
        self.logger.info("Orchestrator initialized successfully")

    def process_single_file(self, file_key: str) -> bool:
        """Process a single PDF file through the complete pipeline."""
        active_processing_jobs.inc()
        start_time = time.time()
        folder_name = file_key.split('/')[0] if '/' in file_key else 'default'
        
        try:
            active_processing_jobs.inc()
            
            # Step 1: Check if file exists in S3
            start_time = time.time()
            if not self.s3_service.object_exists(self.source_bucket, file_key):
                self.logger.error(f"File not found in S3: {file_key}")
                record_file_processed("error", folder_name)
                active_processing_jobs.dec()
                return False
            
            # Step 2: Download file from S3
            start_time = time.time()
            file_bytes = self.s3_service.get_object(self.source_bucket, file_key)
            record_processing_time("download", time.time() - start_time)

            # Step 3: Convert to PDF if needed
            start_time = time.time()
            conversion_files_active.inc()
            converted_content = self.conversion_service.convert_to_pdf(file_bytes, file_key)
            conversion_files_active.dec()
            if converted_content:
                file_bytes = converted_content
                file_key = self.filename_service.update_file_extension(file_key, '.pdf')
            record_processing_time("conversion", time.time() - start_time)

            # Step 4: Remove watermarks
            start_time = time.time()
            watermark_files_active.inc()
            cleaned_content = self.watermark_service.remove_watermarks(io.BytesIO(file_bytes), file_key)
            watermark_files_active.dec()
            if cleaned_content:
                file_bytes = cleaned_content
            record_processing_time("watermark", time.time() - start_time)

            # Step 5: OCR processing
            start_time = time.time()
            ocr_files_active.inc()
            ocr_content = self.ocr_service.apply_ocr_to_pdf(io.BytesIO(file_bytes), file_key)
            ocr_files_active.dec()
            if ocr_content:
                file_bytes = ocr_content
            record_processing_time("ocr", time.time() - start_time)

            # Step 6: Chunk the PDF
            start_time = time.time()
            chunking_files_active.inc()
            chunks = self.chunking_service.chunk_pdf(io.BytesIO(file_bytes), file_key)
            chunking_files_active.dec()
            record_processing_time("chunking", time.time() - start_time)

            # Step 7: Upload chunks to S3
            start_time = time.time()
            s3_upload_files_active.inc()
            uploaded_chunks = []
            for writer, metadata in chunks:
                output = io.BytesIO()
                writer.write(output)
                output.seek(0)
                
                page_num = metadata.get('page_number', 1)
                # Preserve original folder structure in chunk key
                base_name = os.path.splitext(file_key)[0]
                chunk_key = f"{base_name}_page_{page_num}.pdf"
                
                if self.s3_service.put_object(self.chunked_bucket, chunk_key, output.getvalue()):
                    uploaded_chunks.append(chunk_key)
                    s3_uploads_total.labels(bucket=self.chunked_bucket, status='success').inc()
                    record_processing_time("upload", time.time() - start_time)
                    self.logger.info(f"Uploaded chunk: {chunk_key}")
                else:
                    s3_uploads_total.labels(bucket=self.chunked_bucket, status='failed').inc()
                    processing_errors_total.labels(stage='s3_upload', error_type='upload_failed').inc()
            s3_upload_files_active.dec()

            # Step 8: Sync to Knowledge Base
            start_time = time.time()
            kb_sync_files_active.inc()
            from services.kb_sync_service import KBSyncService
            kb_service = KBSyncService(
                aws_access_key_id=AWS_ACCESS_KEY_ID, 
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            
            if folder_name in kb_service.get_kb_mapping():
                sync_success = kb_service.sync_to_knowledge_base_simple(folder_name)
                kb_sync_duration = time.time() - start_time
                record_kb_sync(folder_name, "success" if sync_success else "failed", kb_sync_duration)
                kb_sync_files_active.dec()

                if sync_success:
                    record_file_processed("success", folder_name)
                    self.logger.info(f"Successfully processed: {file_key}")
                    return True
                else:
                    record_file_processed("error", folder_name)
                    return False

        except Exception as e:
            self.logger.error(f"Error processing {file_key}: {str(e)}")
            record_file_processed("error", folder_name)
            
            # Ensure all active counters are decremented on error
            conversion_files_active.dec()
            ocr_files_active.dec()
            watermark_files_active.dec()
            chunking_files_active.dec()
            kb_sync_files_active.dec()
            s3_upload_files_active.dec()
            
            return False
        finally:
            active_processing_jobs.dec()