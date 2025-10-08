"""
Complete Fixed Orchestrator Service with Working Metrics
"""

import sys
import boto3
import io
import os
import time
import logging
import asyncio
from typing import List, Dict, Any
from urllib.parse import unquote
import PyPDF2
from concurrent.futures import ThreadPoolExecutor
from services.filename_service import FilenameService
from services.watermark_service import WatermarkService
from services.ocr_service import OCRService
from services.chunking_service import ChunkingService
from services.s3_service import S3Service
from services.conversion_service import ConversionService
from services.pdf_plumber_service import PDFPlumberService
from prometheus_client import Counter, Histogram, Gauge
from config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    SOURCE_BUCKET, CHUNKED_BUCKET, DIRECT_CHUNKED_BUCKET,
    MAX_WORKERS_PER_STAGE, ASYNC_PROCESSING
)
from monitoring.metrics_collector import metrics
from monitoring.metrics import sanitize_label_value

class Orchestrator:
    """Main orchestrator for PDF processing workflow."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.conversion_service = ConversionService()
        self.filename_service = FilenameService()
        self.watermark_service = WatermarkService()
        self.ocr_service = OCRService()
        self.pdf_plumber_service = PDFPlumberService()
        self.chunking_service = ChunkingService()
        self.s3_service = S3Service(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
        
        # Initialize S3 constants
        self.SOURCE_BUCKET = SOURCE_BUCKET
        self.CHUNKED_BUCKET = CHUNKED_BUCKET
        self.DIRECT_CHUNKED_BUCKET = DIRECT_CHUNKED_BUCKET
        
        # Thread pool for CPU-intensive operations
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_STAGE)
        
        # Async processing semaphores
        self.processing_semaphore = asyncio.Semaphore(MAX_WORKERS_PER_STAGE)
        
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
        metrics.increment_active_jobs()
        metrics.sqs_messages_in_flight.inc()
        metrics.pipeline_stage_files.labels(stage='processing').inc()
        start_time = time.time()
        folder_name = file_key.split('/')[0] if '/' in file_key else 'default'
        
        # Record file uploaded to source bucket
        metrics.record_file_uploaded(folder_name)
        
        try:
            # Handle URL encoding for Arabic characters
            from urllib.parse import unquote
            decoded_file_key = unquote(file_key)
            
            self.logger.info(f"Starting processing for: {file_key}")
            self.logger.info(f"Decoded filename: {decoded_file_key}")
            
            # Enhanced S3 file access with retry and better logging
            max_retries = 3
            retry_delay = 1  # seconds
                    
            for attempt in range(max_retries):
                try:
                    self.logger.info(f"Attempt {attempt + 1}/{max_retries} to download: {decoded_file_key}")
                    download_start = time.time()
                    file_bytes = self.s3_service.get_object(self.SOURCE_BUCKET, decoded_file_key)
                    
                    if file_bytes is not None:
                        metrics.processing_duration.labels(step='s3_download').observe(time.time() - download_start)
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
# Keep original file_key for S3 operations - no decoding needed            
            file_ext = os.path.splitext(file_key)[1].lower()
            pdf_stream = io.BytesIO(file_bytes)
            
            # Format conversion if needed
            if self.conversion_service.is_convertible_format(file_key):
                metrics.files_in_conversion.inc()
                metrics.pipeline_stage_files.labels(stage='conversion').inc()
                convert_start = time.time()
                pdf_content, converted_filename = self.conversion_service.convert_to_pdf(file_bytes, file_key)
                metrics.record_processing_time('conversion', time.time() - convert_start)
                metrics.files_in_conversion.dec()
                metrics.pipeline_stage_files.labels(stage='conversion').dec()
                
                if pdf_content is None:
                    metrics.processing_errors.labels(error_type='conversion_failed', step='conversion').inc()
                    metrics.conversions_total.labels(from_format=file_ext, to_format='pdf', status='failed').inc()
                    metrics.record_file_processed('failed', folder_name)
                    return False
                
                metrics.conversions_total.labels(from_format=file_ext, to_format='pdf', status='success').inc()
                pdf_stream = io.BytesIO(pdf_content)
                self.logger.info(f"Successfully converted {file_key} to PDF")

            # Watermark removal
            watermark_start = time.time()
            watermark_result = self.watermark_service.remove_watermarks(pdf_stream, file_key)
            metrics.record_processing_time('watermark_removal', time.time() - watermark_start)
            
            if watermark_result[0]:
                pdf_stream = watermark_result[0]
                self.logger.info("Watermark processing completed")

            # OCR processing
            metrics.files_in_ocr.inc()
            metrics.pipeline_stage_files.labels(stage='ocr').inc()
            ocr_start = time.time()
            ocr_result = self.ocr_service.apply_ocr_to_pdf(pdf_stream, file_key)
            metrics.record_processing_time('ocr', time.time() - ocr_start)
            metrics.files_in_ocr.dec()
            metrics.pipeline_stage_files.labels(stage='ocr').dec()
            
            if ocr_result[0]:
                pdf_stream = ocr_result[0]
                metrics.ocr_jobs_total.labels(status='success').inc()
                self.logger.info("OCR processing completed")
            else:
                metrics.ocr_jobs_total.labels(status='skipped').inc()

            # Clean filename while preserving folder structure
            folder_path = '/'.join(file_key.split('/')[:-1]) if '/' in file_key else ''
            original_filename = file_key.split('/')[-1]
            
            # Extract extension properly
            filename_only, ext = os.path.splitext(original_filename)
            if not ext:
                ext = '.pdf'  # Default to PDF if no extension
            
            cleaned_filename_only = self.filename_service.clean_filename(original_filename)
            
            if folder_path:
                cleaned_key = f"{folder_path}/{cleaned_filename_only}"
            else:
                cleaned_key = cleaned_filename_only
                
            self.logger.info(f"Original: {original_filename} -> Cleaned: {cleaned_filename_only}")
            self.logger.info(f"Full cleaned path: {cleaned_key}")

            # Chunking
            metrics.files_in_chunking.inc()
            metrics.pipeline_stage_files.labels(stage='chunking').inc()
            chunk_start = time.time()
            chunks = self.chunking_service.chunk_pdf(pdf_stream, file_key, cleaned_key)
            metrics.record_processing_time('chunking', time.time() - chunk_start)
            
            # Record chunks created - YOUR NEW METRIC!
            metrics.record_chunks_created(folder_name, len(chunks))
            
            metrics.files_in_chunking.dec()
            metrics.pipeline_stage_files.labels(stage='chunking').dec()
            
            if not chunks:
                metrics.processing_errors.labels(error_type='chunking_failed', step='chunking').inc()
                metrics.record_file_processed('failed', folder_name)
                return False

            # Upload chunks to S3
            success_count = 0
            for writer, metadata in chunks:
                output = io.BytesIO()
                writer.write(output)
                output.seek(0)
                
                page_num = metadata.get('page_number', 1)
                # Use cleaned filename for chunk key, preserving folder structure
                folder_path = '/'.join(cleaned_key.split('/')[:-1]) if '/' in cleaned_key else ''
                filename_only = cleaned_key.split('/')[-1]
                base_name = os.path.splitext(filename_only)[0]
                
                # Only normalize the filename, preserve folder structure
                normalized_base_name = base_name.replace(' ', '_')
                
                if folder_path:
                    chunk_key = f"{folder_path}/{normalized_base_name}_page_{page_num}.pdf"
                else:
                    chunk_key = f"{normalized_base_name}_page_{page_num}.pdf"
                
                upload_start = time.time()
                if self.s3_service.put_object(self.CHUNKED_BUCKET, chunk_key, output.getvalue()):
                    success_count += 1
                    metrics.s3_uploads_total.labels(bucket=self.CHUNKED_BUCKET, status='success').inc()
                    metrics.record_processing_time('s3_upload', time.time() - upload_start)
                    self.logger.info(f"Uploaded chunk: {chunk_key}")
                    
                    # COMMENTED OUT: Fix metadata page orientation to landscape
                    # self.logger.info(f"🔧 Starting landscape fix for: {chunk_key}")
                    # try:
                    #     self.logger.info(f"🔧 Importing MetadataFixer...")
                    #     from services.metadata_fixer import MetadataFixer
                    #     self.logger.info(f"🔧 Creating MetadataFixer instance...")
                    #     fixer = MetadataFixer(s3_service=self.s3_service, bucket_name=self.CHUNKED_BUCKET)
                    #     self.logger.info(f"🔧 Calling fix_single_file for: {chunk_key}")
                    #     fix_result = fixer.fix_single_file(chunk_key)
                    #     self.logger.info(f"🔧 Fix result: {fix_result}")
                    #     
                    #     if fix_result['status'] == 'fixed':
                    #         self.logger.info(f"✅ Fixed landscape orientation for: {chunk_key}")
                    #     elif fix_result['status'] == 'skipped':
                    #         self.logger.info(f"⏭️ Landscape fix skipped for: {chunk_key} - {fix_result['action_taken']}")
                    #     else:
                    #         self.logger.warning(f"⚠️ Landscape fix result for {chunk_key}: {fix_result['status']} - {fix_result.get('error', 'Unknown')}")
                    #         
                    # except Exception as e:
                    #     self.logger.error(f"❌ Failed to fix landscape orientation for {chunk_key}: {e}")
                    #     metrics.processing_errors.labels(error_type='landscape_fix_failed', step='metadata_fixing').inc()
                    #     # Continue processing - don't fail the entire pipeline
                    
                    # Create metadata file
                    try:
                        from services.metadata_service import MetadataService
                        metadata_service = MetadataService()
                        metadata_service.create_metadata_for_file(
                            chunk_key,
                            self.CHUNKED_BUCKET
                        )
                        self.logger.info(f"Created metadata file for {chunk_key}")
                    except Exception as e:
                        self.logger.error(f"Failed to create metadata file for {chunk_key}: {e}")
                        metrics.processing_errors.labels(error_type='metadata_failed', step='metadata_creation').inc()
                else:
                    metrics.s3_uploads_total.labels(bucket=self.CHUNKED_BUCKET, status='failed').inc()
                    metrics.processing_errors.labels(error_type='upload_failed', step='s3_upload').inc()

            # KB sync
            try:
                from services.kb_sync_service import KBIngestionService
                kb_service = KBIngestionService(
                    aws_access_key_id=AWS_ACCESS_KEY_ID, 
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
                
                kb_mapping = kb_service.get_kb_mapping()
                self.logger.info(f"KB sync check: folder_name={folder_name}, available_mappings={list(kb_mapping.keys())}")
                
                if folder_name in kb_mapping:
                    self.logger.info(f"Starting KB sync for folder: {folder_name}")
                    metrics.files_in_kb_sync.inc()
                    metrics.pipeline_stage_files.labels(stage='kb_sync').inc()
                    kb_start = time.time()
                    kb_result = kb_service.sync_to_knowledge_base_simple(folder_name)
                    kb_duration = time.time() - kb_start
                    metrics.files_in_kb_sync.dec()
                    metrics.pipeline_stage_files.labels(stage='kb_sync').dec()
                    self.logger.info(f"KB sync completed for {folder_name}: {kb_result}, duration={kb_duration:.2f}s")
                    
                    if kb_result.get('status') == 'COMPLETE':
                        # Record successful KB sync - YOUR NEW METRIC!
                        metrics.record_kb_sync_success(folder_name)
                        metrics.record_processing_time('kb_sync', kb_duration)
                        self.logger.info(f"KB sync completed in {kb_duration:.1f}s")
                    else:
                        metrics.record_kb_sync_attempt(folder_name, 'failed', kb_duration)
                        self.logger.warning(f"KB sync failed with status: {kb_result.get('status')}")
                else:
                    self.logger.info(f"No KB mapping found for folder: {folder_name}")
                    # No KB sync attempted, so no metrics to record
                        
            except Exception as e:
                metrics.record_kb_sync_attempt(folder_name, 'failed')
                metrics.processing_errors.labels(error_type='sync_failed', step='kb_sync').inc()
                self.logger.error(f"KB sync error: {str(e)}")

            # Final metrics
            processing_time_total = time.time() - start_time
            metrics.record_processing_time('total', processing_time_total)
            metrics.record_file_processed('success', folder_name)
            
            return success_count > 0
            
        except Exception as e:
            # Handle S3 NoSuchKey specifically - don't count as error
            if 'NoSuchKey' in str(e) or 'NoSuchBucket' in str(e):
                self.logger.info(f"File not found, skipping: {file_key}")
                return False
            else:
                metrics.processing_errors.labels(error_type='general', step='processing').inc()
                self.logger.error(f"Error processing {file_key}: {e}")
                metrics.record_file_processed('failed', folder_name)
                return False
        finally:
            metrics.decrement_active_jobs()
            metrics.sqs_messages_in_flight.dec()
            metrics.pipeline_stage_files.labels(stage='processing').dec()
            metrics.pipeline_stage_files.labels(stage='completed').inc()
    
    async def process_single_file_async(self, file_key: str) -> bool:
        """
        Async version of process_single_file with dual chunking strategy
        
        Args:
            file_key: S3 key of the file to process
            
        Returns:
            True if successful, False otherwise
        """
        async with self.processing_semaphore:
            start_time = time.time()
            folder_name = sanitize_label_value(file_key.split('/')[0])
            
            try:
                self.logger.info(f"🚀 Starting async processing: {file_key}")
                
                # Stage 1: File Download
                loop = asyncio.get_event_loop()
                pdf_data = await loop.run_in_executor(
                    self.executor, 
                    self._download_file_sync, 
                    file_key
                )
                
                if not pdf_data:
                    return False
                
                # Stage 2: Document Preparation (sync operations in thread pool)
                original_pdf_data, processed_pdf_data = await loop.run_in_executor(
                    self.executor,
                    self._prepare_document_sync,
                    pdf_data, file_key
                )
                
                if not original_pdf_data:
                    return False
                
                # Stage 3: Enhanced Processing (OCR + PDF-plumber)
                enhanced_pdf_data = await loop.run_in_executor(
                    self.executor,
                    self._enhance_document_sync,
                    processed_pdf_data, file_key
                )
                
                # Stage 4: Dual Chunking Strategy (PARALLEL PROCESSING)
                self.logger.info(f"🔄 Starting dual chunking for: {file_key}")
                
                # Create both chunking tasks simultaneously
                processed_task = asyncio.create_task(
                    self.chunking_service.chunk_pdf_processed(
                        original_pdf_data, file_key, enhanced_pdf_data
                    )
                )
                
                direct_task = asyncio.create_task(
                    self.chunking_service.chunk_pdf_direct(
                        original_pdf_data, file_key
                    )
                )
                
                # Wait for both chunking streams to complete
                processed_chunks, direct_chunks = await asyncio.gather(
                    processed_task, direct_task, return_exceptions=True
                )
                
                # Handle exceptions in chunking results
                if isinstance(processed_chunks, Exception):
                    self.logger.error(f"Processed chunking failed: {processed_chunks}")
                    processed_chunks = []
                
                if isinstance(direct_chunks, Exception):
                    self.logger.error(f"Direct chunking failed: {direct_chunks}")
                    direct_chunks = []
                
                # Record dual chunking metrics
                if processed_chunks:
                    metrics.processed_chunks_created_total.labels(folder=folder_name).inc(len(processed_chunks))
                if direct_chunks:
                    metrics.direct_chunks_created_total.labels(folder=folder_name).inc(len(direct_chunks))
                
                # Record metrics
                processing_time = time.time() - start_time
                metrics.record_processing_time('total_async', processing_time)
                metrics.record_file_processed('success', folder_name)
                
                self.logger.info(f"🎉 Async processing completed: {file_key} ({processing_time:.2f}s)")
                return True
                
            except Exception as e:
                processing_time = time.time() - start_time
                self.logger.error(f"💥 Async processing failed for {file_key}: {e} ({processing_time:.2f}s)")
                metrics.record_file_processed('failed', folder_name)
                metrics.processing_errors.labels(error_type='async_processing', step='orchestrator').inc()
                return False
    
    def _download_file_sync(self, file_key: str) -> bytes:
        """Synchronous file download for thread pool execution"""
        try:
            return self.s3_service.download_file(self.SOURCE_BUCKET, file_key)
        except Exception as e:
            self.logger.error(f"Download failed for {file_key}: {e}")
            return None
    
    def _prepare_document_sync(self, pdf_data: bytes, file_key: str) -> tuple:
        """Synchronous document preparation (conversion + watermark removal)"""
        try:
            # Format conversion if needed
            processed_data = self.conversion_service.convert_to_pdf(pdf_data, file_key)
            if not processed_data:
                processed_data = pdf_data
            
            # Watermark removal
            cleaned_data = self.watermark_service.remove_watermarks(processed_data)
            if not cleaned_data:
                cleaned_data = processed_data
            
            return pdf_data, cleaned_data  # Return both original and processed
            
        except Exception as e:
            self.logger.error(f"Document preparation failed for {file_key}: {e}")
            return pdf_data, pdf_data  # Return original as fallback
    
    def _enhance_document_sync(self, pdf_data: bytes, file_key: str) -> bytes:
        """Synchronous document enhancement (OCR + PDF-plumber)"""
        try:
            enhanced_data = pdf_data
            
            # OCR processing if needed
            ocr_result = self.ocr_service.process_pdf(io.BytesIO(pdf_data), file_key)
            if ocr_result:
                enhanced_data = ocr_result.getvalue()
            
            # PDF-plumber processing
            plumber_result, processed_pages = self.pdf_plumber_service.apply_pdf_plumber_to_pdf(
                io.BytesIO(enhanced_data), file_key
            )
            
            if plumber_result:
                enhanced_data = plumber_result.getvalue()
                self.logger.info(f"PDF-plumber enhanced {len(processed_pages)} pages for {file_key}")
            
            return enhanced_data
            
        except Exception as e:
            self.logger.error(f"Document enhancement failed for {file_key}: {e}")
            return pdf_data  # Return original as fallback