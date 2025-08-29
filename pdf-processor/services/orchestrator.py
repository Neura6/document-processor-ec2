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
from urllib.parse import unquote
import PyPDF2
from services.filename_service import FilenameService
from services.watermark_service import WatermarkService
from services.ocr_service import OCRService
from services.chunking_service import ChunkingService
from services.s3_service import S3Service
from services.conversion_service import ConversionService
from prometheus_client import Counter, Histogram, Gauge
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SOURCE_BUCKET, CHUNKED_BUCKET
from monitoring.metrics import (
    files_in_conversion, files_in_ocr, files_in_chunking, files_in_kb_sync,
    pipeline_stage_files, sqs_messages_in_flight
)

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
        sqs_messages_in_flight.inc()
        pipeline_stage_files.labels(stage='processing').inc()
        start_time = time.time()
        folder_name = file_key.split('/')[0] if '/' in file_key else 'default'
        
        try:
            # Handle URL encoding for Arabic characters
            from urllib.parse import unquote
            decoded_file_key = unquote(file_key)
            
            self.logger.info(f"Starting processing for: {file_key}")
            self.logger.info(f"Decoded filename: {decoded_file_key}")
            
            # Enhanced S3 file access with retry and better logging
            max_retries = 3
            retry_delay = 1  # seconds
            
            # Use original encoded key for S3 operations (preserve actual S3 key)
            final_file_key = file_key
            
            for attempt in range(max_retries):
                try:
                    self.logger.info(f"Attempt {attempt + 1}/{max_retries} to download: {final_file_key}")
                    download_start = time.time()
                    file_bytes = self.s3_service.get_object(self.SOURCE_BUCKET, final_file_key)
                    
                    if file_bytes is not None:
                        processing_duration_seconds.labels(stage='s3_download').observe(time.time() - download_start)
                        self.logger.info(f"Successfully downloaded {len(file_bytes)} bytes from {decoded_file_key}")
                        break
                    
                    # If we get here, file_bytes is None (file not found)
                    if attempt == max_retries - 1:
                        self.logger.error(f"File not found after {max_retries} attempts: {decoded_file_key}")
                        return False
                    
                    self.logger.warning(f"File not found, retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    
                except Exception as e:
                    if attempt == max_retries - 1:
                        self.logger.error(f"Failed to download after {max_retries} attempts: {str(e)}")
                        return False
                    self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}, retrying...")
                    time.sleep(retry_delay)
            
            # Use decoded key for processing
            file_key = decoded_file_key
            
            extension = os.path.splitext(file_key)[1].lower()
            pdf_stream = io.BytesIO(file_bytes)
            
            # Format conversion if needed
            if self.conversion_service.is_convertible_format(file_key):
                files_in_conversion.inc()
                pipeline_stage_files.labels(stage='conversion').inc()
                convert_start = time.time()
                pdf_content, converted_filename = self.conversion_service.convert_to_pdf(file_bytes, file_key)
                processing_duration_seconds.labels(stage='conversion').observe(time.time() - convert_start)
                files_in_conversion.dec()
                pipeline_stage_files.labels(stage='conversion').dec()
                
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
            files_in_ocr.inc()
            pipeline_stage_files.labels(stage='ocr').inc()
            ocr_start = time.time()
            ocr_result = self.ocr_service.apply_ocr_to_pdf(pdf_stream, file_key)
            processing_duration_seconds.labels(stage='ocr').observe(time.time() - ocr_start)
            files_in_ocr.dec()
            pipeline_stage_files.labels(stage='ocr').dec()
            
            if ocr_result[0]:
                pdf_stream = ocr_result[0]
                ocr_processing_total.labels(status='success').inc()
                self.logger.info("OCR processing completed")
            else:
                ocr_processing_total.labels(status='skipped').inc()

            # Clean filename while preserving folder structure
            folder_path = '/'.join(file_key.split('/')[:-1]) if '/' in file_key else ''
            original_filename = file_key.split('/')[-1]
            cleaned_filename_only = self.filename_service.clean_filename(original_filename)
            
            if folder_path:
                cleaned_filename = f"{folder_path}/{cleaned_filename_only}"
            else:
                cleaned_filename = cleaned_filename_only
                
            self.logger.info(f"Original: {original_filename} -> Cleaned: {cleaned_filename_only}")
            self.logger.info(f"Full cleaned path: {cleaned_filename}")

            # Chunking
            files_in_chunking.inc()
            pipeline_stage_files.labels(stage='chunking').inc()
            chunk_start = time.time()
            chunks = self.chunking_service.chunk_pdf(pdf_stream, cleaned_filename)
            processing_duration_seconds.labels(stage='chunking').observe(time.time() - chunk_start)
            chunks_created_total.inc(len(chunks))
            files_in_chunking.dec()
            pipeline_stage_files.labels(stage='chunking').dec()
            
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
                # Use cleaned filename for chunk key, preserving folder structure
                folder_path = '/'.join(cleaned_filename.split('/')[:-1]) if '/' in cleaned_filename else ''
                filename_only = cleaned_filename.split('/')[-1]
                base_name = os.path.splitext(filename_only)[0]
                
                if folder_path:
                    chunk_key = f"{folder_path}/{base_name}_page_{page_num}.pdf"
                else:
                    chunk_key = f"{base_name}_page_{page_num}.pdf"
                
                upload_start = time.time()
                if self.s3_service.put_object(self.CHUNKED_BUCKET, chunk_key, output.getvalue()):
                    success_count += 1
                    s3_uploads_total.labels(bucket=self.CHUNKED_BUCKET, status='success').inc()
                    processing_duration_seconds.labels(stage='s3_upload').observe(time.time() - upload_start)
                    self.logger.info(f"Uploaded chunk: {chunk_key}")
                    
                    # Create metadata file for the uploaded chunk
                    try:
                        folder_name = folder_path.split('/')[-1] if folder_path else 'default'
                        self.chunking_service.metadata_service.create_metadata_file(
                            bucket=self.CHUNKED_BUCKET,
                            key=chunk_key,
                            folder=folder_name,
                            original_filename=filename_only
                        )
                        self.logger.info(f"Created metadata file for: {chunk_key}")
                    except Exception as e:
                        self.logger.error(f"Failed to create metadata file for {chunk_key}: {e}")
                        processing_errors_total.labels(stage='metadata_creation', error_type='metadata_failed').inc()
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
                    files_in_kb_sync.inc()
                    pipeline_stage_files.labels(stage='kb_sync').inc()
                    kb_start = time.time()
                    kb_result = kb_service.sync_to_knowledge_base_simple(folder_name)
                    kb_duration = time.time() - kb_start
                    files_in_kb_sync.dec()
                    pipeline_stage_files.labels(stage='kb_sync').dec()
                    
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
            # Handle S3 NoSuchKey specifically - don't count as error
            if 'NoSuchKey' in str(e) or 'NoSuchBucket' in str(e):
                self.logger.info(f"File not found, skipping: {file_key}")
                return False
            else:
                processing_errors_total.labels(stage='processing', error_type='general').inc()
                self.logger.error(f"Error processing {file_key}: {e}")
                files_processed_total.labels(status='failed').inc()
                return False
        finally:
            active_processing_jobs.dec()
            sqs_messages_in_flight.dec()
            pipeline_stage_files.labels(stage='processing').dec()
            pipeline_stage_files.labels(stage='completed').inc()