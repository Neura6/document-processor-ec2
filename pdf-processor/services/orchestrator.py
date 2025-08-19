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
import PyPDF2
from services.filename_service import FilenameService
from services.watermark_service import WatermarkService
from services.ocr_service import OCRService
from services.chunking_service import ChunkingService
from services.s3_service import S3Service
from services.conversion_service import ConversionService  # ADDED
from prometheus_client import Counter, Histogram, Gauge
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
        self.SOURCE_BUCKET = SOURCE_BUCKET
        self.CHUNKED_BUCKET = CHUNKED_BUCKET
        
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
            self.logger.info(f"Starting processing for: {file_key}")
            
            # Check if file exists first
            if not self.s3_service.object_exists(self.SOURCE_BUCKET, file_key):
                self.logger.warning(f"File not found: {file_key} - skipping")
                processing_errors_total.labels(stage='validation', error_type='file_not_found').inc()
                return False
            
            # Track S3 download
            download_start = time.time()
            file_bytes = self.s3_service.get_object(self.SOURCE_BUCKET, file_key)
            if file_bytes is None:
                self.logger.warning(f"Could not retrieve file: {file_key} - skipping")
                processing_errors_total.labels(stage='s3_download', error_type='download_failed').inc()
                return False
                
            processing_duration_seconds.labels(stage='s3_download').observe(time.time() - download_start)
            
            extension = os.path.splitext(file_key)[1].lower()
            pdf_stream = io.BytesIO(file_bytes)
            
            # Format conversion if needed
            if self.conversion_service.is_convertible_format(file_key):
                convert_start = time.time()
                pdf_content, converted_filename = self.conversion_service.convert_to_pdf(file_bytes, file_key)
                processing_duration_seconds.labels(stage='conversion').observe(time.time() - convert_start)
                
                if pdf_content is None:
                    processing_errors_total.labels(stage='conversion', error_type='conversion_failed').inc()
                    document_conversions_total.labels(from_format=extension, to_format='pdf', status='failed').inc()
                    files_processed_total.labels(status='failed').inc()
                    return False
                
                document_conversions_total.labels(from_format=extension, to_format='pdf', status='success').inc()
                pdf_stream = io.BytesIO(pdf_content)
                self.logger.info(f"Successfully converted {file_key} to PDF")

            # Watermark removal
            watermark_start = time.time()
            watermark_result = self.watermark_service.remove_watermarks(pdf_stream, file_key)
            processing_duration_seconds.labels(stage='watermark_removal').observe(time.time() - watermark_start)
            
            if watermark_result[0]:
                pdf_stream = watermark_result[0]
                self.logger.info("Watermark processing completed")

            # OCR processing
            ocr_start = time.time()
            ocr_result = self.ocr_service.apply_ocr_to_pdf(pdf_stream, file_key)
            processing_duration_seconds.labels(stage='ocr').observe(time.time() - ocr_start)
            
            if ocr_result[0]:
                pdf_stream = ocr_result[0]
                ocr_processing_total.labels(status='success').inc()
                self.logger.info("OCR processing completed")
            else:
                ocr_processing_total.labels(status='skipped').inc()

            # Chunking
            chunk_start = time.time()
            chunks = self.chunking_service.chunk_pdf(pdf_stream, file_key)
            processing_duration_seconds.labels(stage='chunking').observe(time.time() - chunk_start)
            chunks_created_total.inc(len(chunks))
            
            if not chunks:
                processing_errors_total.labels(stage='chunking', error_type='chunking_failed').inc()
                files_processed_total.labels(status='failed').inc()
                return False

            # Upload chunks to S3
            success_count = 0
            for writer, metadata in chunks:
                output = io.BytesIO()
                writer.write(output)
                output.seek(0)
                
                page_num = metadata.get('page_number', 1)
                chunk_key = f"{os.path.splitext(file_key)[0]}_page_{page_num}.pdf"
                
                upload_start = time.time()
                if self.s3_service.put_object(self.CHUNKED_BUCKET, chunk_key, output.getvalue()):
                    success_count += 1
                    s3_uploads_total.labels(bucket=self.CHUNKED_BUCKET, status='success').inc()
                    processing_duration_seconds.labels(stage='s3_upload').observe(time.time() - upload_start)
                    self.logger.info(f"Uploaded chunk: {chunk_key}")
                else:
                    s3_uploads_total.labels(bucket=self.CHUNKED_BUCKET, status='failed').inc()
                    processing_errors_total.labels(stage='s3_upload', error_type='upload_failed').inc()

            # KB sync
            try:
                from services.kb_sync_service import KBIngestionService
                kb_service = KBIngestionService(
                    aws_access_key_id=AWS_ACCESS_KEY_ID, 
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
                
                if folder_name in kb_service.get_kb_mapping():
                    kb_start = time.time()
                    kb_result = kb_service.sync_to_knowledge_base_simple(folder_name)
                    kb_duration = time.time() - kb_start
                    
                    if kb_result.get('status') == 'COMPLETE':
                        kb_sync_operations_total.labels(status='success').inc()
                        processing_duration_seconds.labels(stage='kb_sync').observe(kb_duration)
                        self.logger.info(f"KB sync completed in {kb_duration:.1f}s")
                    else:
                        kb_sync_operations_total.labels(status='failed').inc()
                        self.logger.warning(f"KB sync failed with status: {kb_result.get('status')}")
                        
            except Exception as e:
                kb_sync_operations_total.labels(status='failed').inc()
                processing_errors_total.labels(stage='kb_sync', error_type='sync_failed').inc()
                self.logger.error(f"KB sync error: {str(e)}")

            # Final metrics
            processing_time_total = time.time() - start_time
            processing_duration_seconds.labels(stage='total').observe(processing_time_total)
            files_processed_total.labels(status='success').inc()
            
            return success_count > 0
            
        except Exception as e:
            # Handle S3 NoSuchKey specifically
            if 'NoSuchKey' in str(e) or 'NoSuchBucket' in str(e):
                self.logger.warning(f"File not found, skipping: {file_key}")
                processing_errors_total.labels(stage='validation', error_type='file_not_found').inc()
            else:
                processing_errors_total.labels(stage='processing', error_type='general').inc()
                self.logger.error(f"Error processing {file_key}: {e}")
            
            files_processed_total.labels(status='failed').inc()
            return False
        finally:
            active_processing_jobs.dec()