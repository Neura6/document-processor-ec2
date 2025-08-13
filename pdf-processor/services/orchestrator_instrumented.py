"""
Instrumented Orchestrator Service with Prometheus metrics
Coordinates all services to process PDF files end-to-end with metrics collection.
"""

import sys
import boto3
import io
import os
import logging
import time
from typing import List, Dict, Any
import PyPDF2
from services.filename_service import FilenameService
from services.watermark_service import WatermarkService
from services.ocr_service import OCRService
from services.chunking_service import ChunkingService
from services.s3_service import S3Service
from services.conversion_service import ConversionService
from services.metrics_service import metrics
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SOURCE_BUCKET, CHUNKED_BUCKET

class InstrumentedOrchestrator:
    """Instrumented orchestrator for PDF processing workflow with metrics collection."""
    
    def __init__(self):
        self.conversion_service = ConversionService()
        
        # Initialize S3 constants
        self.SOURCE_BUCKET = SOURCE_BUCKET
        self.CHUNKED_BUCKET = CHUNKED_BUCKET
        self.s3_service = S3Service(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        self.filename_service = FilenameService()
        self.watermark_service = WatermarkService()
        self.ocr_service = OCRService()
        self.chunking_service = ChunkingService()
        self.conversion_service = ConversionService()
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Instrumented Orchestrator initialized successfully")
    
    def process_single_file(self, file_key: str) -> bool:
        """
        Process a single file through the complete pipeline with metrics.
        
        Args:
            file_key: S3 object key
            
        Returns:
            True if successful, False otherwise
        """
        start_time = time.time()
        folder_name = file_key.split('/')[0] if '/' in file_key else 'default'
        
        try:
            self.logger.info(f"Starting processing for: {file_key}")
            metrics.update_processing_files(1)
            
            # Step 0: Check if already chunked (skip processing)
            check_start = time.time()
            chunked_key = f"{os.path.splitext(file_key)[0]}_page_1.pdf"
            if self.s3_service.object_exists(CHUNKED_BUCKET, chunked_key):
                self.logger.info(f"File already chunked: {file_key}, skipping processing")
                metrics.record_file_processed('SKIPPED', folder_name, 'already_chunked')
                return True
            metrics.record_processing_time('check_existing', folder_name, time.time() - check_start)
            
            # Step 1: Check file format and convert if necessary
            conversion_start = time.time()
            extension = os.path.splitext(file_key)[1].lower()
            if self.conversion_service.is_convertible_format(file_key):
                self.logger.info("Step 1: Converting document to PDF")
                
                # Download file
                download_start = time.time()
                file_bytes = self.s3_service.get_object(SOURCE_BUCKET, file_key)
                metrics.record_s3_operation('download', 'success', SOURCE_BUCKET, time.time() - download_start)
                
                # Convert to PDF
                convert_start = time.time()
                pdf_content, converted_filename = self.conversion_service.convert_to_pdf(file_bytes, file_key)
                
                if pdf_content is None:
                    self.logger.error(f"Failed to convert {file_key} to PDF")
                    metrics.record_error('conversion', 'conversion_failed', folder_name)
                    metrics.record_file_processed('FAILED', folder_name, 'conversion')
                    return False
                
                metrics.record_processing_time('conversion', folder_name, time.time() - convert_start)
                
                # Update file_key to use the converted filename
                file_key = converted_filename
                pdf_stream = io.BytesIO(pdf_content)
                self.logger.info(f"Successfully converted to {converted_filename}")
                metrics.record_file_processed('SUCCESS', folder_name, 'conversion')
            else:
                # Already PDF, download normally
                download_start = time.time()
                pdf_bytes = self.s3_service.get_object(SOURCE_BUCKET, file_key)
                metrics.record_s3_operation('download', 'success', SOURCE_BUCKET, time.time() - download_start)
                pdf_stream = io.BytesIO(pdf_bytes)
                metrics.record_file_processed('SUCCESS', folder_name, 'download')
            
            # Step 2: Clean filename if needed
            filename_start = time.time()
            self.logger.info("Step 2: Cleaning filename")
            cleaned_key = self.filename_service.clean_filename(file_key)
            if cleaned_key != file_key:
                # Handle filename cleaning (copy new, delete old)
                if not self.s3_service.object_exists(SOURCE_BUCKET, cleaned_key):
                    copy_start = time.time()
                    self.s3_service.copy_object(SOURCE_BUCKET, file_key, SOURCE_BUCKET, cleaned_key)
                    metrics.record_s3_operation('copy', 'success', SOURCE_BUCKET, time.time() - copy_start)
                    
                    delete_start = time.time()
                    self.s3_service.delete_object(SOURCE_BUCKET, file_key)
                    metrics.record_s3_operation('delete', 'success', SOURCE_BUCKET, time.time() - delete_start)
                    
                    file_key = cleaned_key
                    self.logger.info(f"File renamed to: {file_key}")
                else:
                    self.logger.warning(f"Target key {cleaned_key} already exists, skipping rename")
            metrics.record_processing_time('filename_cleaning', folder_name, time.time() - filename_start)
            
            # Step 3: Remove watermarks
            watermark_start = time.time()
            self.logger.info("Step 3: Removing watermarks")
            watermark_result = self.watermark_service.remove_watermarks(pdf_stream, file_key)
            if watermark_result[0]:
                pdf_stream = watermark_result[0]
                if watermark_result[1]:
                    self.logger.info(f"Removed pages: {watermark_result[1]}")
                self.logger.info("Watermark processing completed")
                metrics.record_file_processed('SUCCESS', folder_name, 'watermark_removal')
            else:
                self.logger.info("No watermarks found, continuing")
                metrics.record_file_processed('SUCCESS', folder_name, 'watermark_skipped')
            metrics.record_processing_time('watermark_removal', folder_name, time.time() - watermark_start)
            
            # Step 4: Apply OCR if needed
            ocr_start = time.time()
            self.logger.info("Step 4: Applying OCR")
            ocr_result = self.ocr_service.apply_ocr_to_pdf(pdf_stream, file_key)
            if ocr_result[0]:
                pdf_stream = ocr_result[0]
                if ocr_result[1]:
                    self.logger.info(f"OCR applied to pages: {ocr_result[1]}")
                self.logger.info("OCR processing completed")
                metrics.record_file_processed('SUCCESS', folder_name, 'ocr')
            else:
                self.logger.info("No OCR needed, continuing")
                metrics.record_file_processed('SUCCESS', folder_name, 'ocr_skipped')
            metrics.record_processing_time('ocr', folder_name, time.time() - ocr_start)
            
            # Step 5: Chunk PDF
            chunking_start = time.time()
            self.logger.info("Step 5: Chunking PDF")
            chunks = self.chunking_service.chunk_pdf(pdf_stream, file_key)
            if not chunks:
                self.logger.error("Failed to chunk PDF")
                metrics.record_error('chunking', 'chunking_failed', folder_name)
                metrics.record_file_processed('FAILED', folder_name, 'chunking')
                return False
            metrics.record_processing_time('chunking', folder_name, time.time() - chunking_start)
            
            # Step 6: Upload chunks to S3
            upload_start = time.time()
            self.logger.info("Step 6: Uploading chunks to S3")
            success_count = 0
            total_chunks = len(chunks)
            
            for writer, metadata in chunks:
                chunk_upload_start = time.time()
                output = io.BytesIO()
                writer.write(output)
                output.seek(0)
                
                page_num = metadata.get('page_number', 1)
                chunk_key = f"{os.path.splitext(file_key)[0]}_page_{page_num}.pdf"
                
                if self.s3_service.put_object(CHUNKED_BUCKET, chunk_key, output.getvalue()):
                    success_count += 1
                    self.logger.info(f"Uploaded chunk: {chunk_key}")
                    metrics.record_s3_operation('upload', 'success', CHUNKED_BUCKET, time.time() - chunk_upload_start)
                else:
                    metrics.record_s3_operation('upload', 'failed', CHUNKED_BUCKET, time.time() - chunk_upload_start)
            
            metrics.record_processing_time('upload', folder_name, time.time() - upload_start)
            
            # Step 7: Sync to Knowledge Base immediately after upload
            kb_start = time.time()
            folder_name = file_key.split('/')[0] if '/' in file_key else 'default'
            
            try:
                from services.kb_sync_service import KBIngestionService
                kb_service = KBIngestionService(
                    aws_access_key_id=AWS_ACCESS_KEY_ID, 
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
                
                if folder_name in kb_service.get_kb_mapping():
                    kb_info = kb_service.get_kb_mapping()[folder_name]
                    self.logger.info(f"KB_SYNC: Starting sync for folder '{folder_name}' -> KB ID: {kb_info['id']}")
                    
                    kb_result = kb_service.sync_to_knowledge_base_simple(folder_name)
                    kb_duration = time.time() - kb_start
                    
                    if kb_result.get('status') == 'COMPLETE':
                        duration = kb_result.get('duration', 0)
                        self.logger.info(f"KB_SYNC: Successfully synced '{folder_name}' in {duration:.1f}s")
                        metrics.record_kb_sync('SUCCESS', folder_name, kb_duration)
                    else:
                        status = kb_result.get('status')
                        failed_count = len(kb_result.get('failed_files', []))
                        self.logger.warning(f"KB_SYNC: Sync completed with status '{status}' ({failed_count} failed files)")
                        metrics.record_kb_sync('PARTIAL', folder_name, kb_duration)
                else:
                    self.logger.info(f"KB_SYNC: No KB mapping found for folder '{folder_name}', skipping sync")
                    metrics.record_kb_sync('SKIPPED', folder_name, time.time() - kb_start)
                    
            except Exception as e:
                self.logger.error(f"KB_SYNC: Error during sync for folder '{folder_name}': {str(e)}")
                metrics.record_kb_sync('FAILED', folder_name, time.time() - kb_start)
            
            # Record overall success
            total_time = time.time() - start_time
            metrics.record_file_processed('SUCCESS', folder_name, 'complete')
            metrics.record_processing_time('total', folder_name, total_time)
            metrics.update_processing_files(-1)
            
            self.logger.info(f"Successfully processed {file_key} in {total_time:.1f}s")
            return success_count > 0
        
        except Exception as e:
            total_time = time.time() - start_time
            self.logger.error(f"Error processing {file_key}: {e}")
            metrics.record_error('orchestrator', str(type(e).__name__), folder_name)
            metrics.record_file_processed('FAILED', folder_name, 'orchestrator')
            metrics.record_processing_time('total', folder_name, total_time)
            metrics.update_processing_files(-1)
            return False
    
    def get_folder_name_from_path(self, file_path: str) -> str:
        """Extract folder name from file path for KB sync mapping"""
        if '/' in file_path:
            return file_path.split('/')[0]
        else:
            return 'default'
    
    def process_folder(self, folder: str) -> Dict[str, Any]:
        """
        Process all PDFs in a folder with metrics collection.
        
        Args:
            folder: Folder prefix to process
            
        Returns:
            Dictionary with processing results
        """
        try:
            files = self.s3_service.list_files_in_folder(SOURCE_BUCKET, folder)
            self.logger.info(f"Found {len(files)} files in folder {folder}")
            
            results = {
                'total_files': len(files),
                'processed_files': 0,
                'failed_files': 0,
                'folder': folder
            }
            
            for file_key in files:
                if file_key.lower().endswith(('.pdf', '.docx', '.doc', '.txt', '.rtf')):
                    self.logger.info(f"Processing file: {file_key}")
                    if self.process_single_file(file_key):
                        results['processed_files'] += 1
                    else:
                        results['failed_files'] += 1
            
            # Update folder-level metrics
            metrics.folder_processing_volume.labels(folder=folder).set(results['processed_files'])
            
            self.logger.info(f"Folder processing completed: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error processing folder {folder}: {e}")
            metrics.record_error('folder_processing', str(type(e).__name__), folder)
            return {'error': str(e), 'folder': folder}
