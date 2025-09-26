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
from monitoring.metrics_collector import metrics

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
                    metrics.processing_errors.labels(stage='conversion', error_type='conversion_failed').inc()
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
                metrics.processing_errors.labels(stage='chunking', error_type='chunking_failed').inc()
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
                
                if folder_path:
                    chunk_key = f"{folder_path}/{base_name}_page_{page_num}.pdf"
                else:
                    chunk_key = f"{base_name}_page_{page_num}.pdf"
                
                # Ensure no spaces in chunk key (replace with underscores)
                chunk_key = chunk_key.replace(' ', '_')
                
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
                    #     processing_errors_total.labels(stage='metadata_fixing', error_type='landscape_fix_failed').inc()
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
                        metrics.processing_errors.labels(stage='metadata_creation', error_type='metadata_failed').inc()
                else:
                    metrics.s3_uploads_total.labels(bucket=self.CHUNKED_BUCKET, status='failed').inc()
                    metrics.processing_errors.labels(stage='s3_upload', error_type='upload_failed').inc()

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
                metrics.processing_errors.labels(stage='kb_sync', error_type='sync_failed').inc()
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
                metrics.processing_errors.labels(stage='processing', error_type='general').inc()
                self.logger.error(f"Error processing {file_key}: {e}")
                metrics.record_file_processed('failed', folder_name)
                return False
        finally:
            metrics.decrement_active_jobs()
            metrics.sqs_messages_in_flight.dec()
            metrics.pipeline_stage_files.labels(stage='processing').dec()
            metrics.pipeline_stage_files.labels(stage='completed').inc()