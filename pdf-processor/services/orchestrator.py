"""
Main Orchestrator Service
Coordinates all services to process PDF files end-to-end.
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
from services.watermark_service_fixed import WatermarkService
from services.ocr_service import OCRService
from services.chunking_service import ChunkingService
from services.s3_service import S3Service
from services.conversion_service import ConversionService
import logging
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SOURCE_BUCKET, CHUNKED_BUCKET
from monitoring.metrics import (
    files_processed_total,
    processing_duration,
    processing_errors,
    s3_uploads_total,
    s3_upload_duration,
    kb_sync_total,
    kb_sync_duration,
    queue_depth,
    active_processing_jobs,
    start_metrics_server,
    record_processing_time,
    record_file_processed,
    record_kb_sync
)

class Orchestrator:
    """Main orchestrator for PDF processing workflow."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.conversion_service = ConversionService()
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
        """Legacy method - use process_single_file_parallel instead"""
        return self.process_single_file_parallel(file_key)

    def process_single_file_parallel(self, file_key: str) -> bool:
        """
        Process a single PDF file through the complete pipeline in parallel.
        
        Args:
            file_key: S3 object key to process
            
        Returns:
            bool: True if processing successful, False otherwise
        """
        active_processing_jobs.inc()
        start_time = time.time()
        folder_name = file_key.split('/')[0] if '/' in file_key else 'default'
        
        # URL decode the file key to handle spaces and special characters
        from urllib.parse import unquote_plus
        file_key = unquote_plus(file_key)
        
        try:
            self.logger.info(f"Starting processing for: {file_key}")
            
            # Step 1: Get file from S3
            file_content = self.s3_service.get_object(SOURCE_BUCKET, file_key)
            if not file_content:
                processing_errors.labels(error_type='s3', step='download').inc()
                self.logger.error(f"Failed to download file: {file_key}")
                return False
            
            # Step 2: Document conversion (if needed)
            converted_pdf = self.conversion_service.convert_to_pdf(file_content, file_key)
            if not converted_pdf:
                processing_errors.labels(error_type='conversion', step='convert').inc()
                self.logger.error(f"Failed to convert file: {file_key}")
                return False
            
            # Step 3: Clean filename
            clean_filename = self.filename_service.clean_filename(file_key)
            
            # Step 4: Remove watermarks
            cleaned_pdf = self.watermark_service.remove_watermarks(converted_pdf, file_key)
            if cleaned_pdf is None:
                processing_errors.labels(error_type='watermark', step='watermark').inc()
                self.logger.error(f"Failed to remove watermarks: {file_key}")
                return False
            
            # Step 5: OCR and chunking
            try:
                pages = self.ocr_service.extract_text(cleaned_pdf)
                if not pages:
                    processing_errors.labels(error_type='ocr', step='ocr').inc()
                    self.logger.error(f"Failed OCR processing: {file_key}")
                    return False
                
                # Chunk into pages
                chunks = self.chunking_service.chunk_document(pages, clean_filename)
                if not chunks:
                    processing_errors.labels(error_type='chunking', step='chunking').inc()
                    self.logger.error(f"Failed chunking: {file_key}")
                    return False
                
                # Upload chunks to S3
                success_count = 0
                for chunk in chunks:
                    chunk_key = f"{folder_name}/{chunk['filename']}"
                    
                    # Convert chunk to bytes for upload
                    output = io.BytesIO()
                    chunk['content'].save(output, format='PDF')
                    output.seek(0)
                    
                    upload_start = time.time()
                    if self.s3_service.put_object(CHUNKED_BUCKET, chunk_key, output.getvalue()):
                        success_count += 1
                        s3_uploads_total.labels(bucket=CHUNKED_BUCKET, status='success').inc()
                        s3_upload_duration.observe(time.time() - upload_start)
                        self.logger.info(f"Uploaded chunk: {chunk_key}")
                    else:
                        s3_uploads_total.labels(bucket=CHUNKED_BUCKET, status='failed').inc()
                
                if success_count == 0:
                    return False
                
            except Exception as e:
                processing_errors.labels(error_type='processing', step='chunking').inc()
                self.logger.error(f"Error during OCR/chunking: {str(e)}")
                return False
            
            # Step 6: Sync to Knowledge Base in parallel
            folder_name = file_key.split('/')[0] if '/' in file_key else 'default'
            
            try:
                # Use parallel KB sync service
                from concurrent.futures import ThreadPoolExecutor
                from services.kb_sync_service import KBIngestionService
                
                kb_service = KBIngestionService(
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name='us-east-1'
                )
                
                # Parallel KB sync using thread pool
                with ThreadPoolExecutor(max_workers=3) as executor:
                    future = executor.submit(
                        kb_service.sync_to_knowledge_base,
                        folder_name=folder_name,
                        s3_bucket=CHUNKED_BUCKET,
                        s3_prefix=f"{folder_name}/"
                    )
                    
                    # Wait for KB sync completion
                    kb_result = future.result(timeout=300)  # 5 min timeout
                    
                    if kb_result:
                        kb_sync_total.labels(status='success').inc()
                        self.logger.info(f"KB sync completed for {folder_name}")
                    else:
                        kb_sync_total.labels(status='failed').inc()
                        self.logger.error(f"KB sync failed for {folder_name}")
                        return False
            
            except Exception as e:
                kb_sync_total.labels(status='failed').inc()
                self.logger.error(f"KB sync failed: {str(e)}")
                return False
            
            # Record overall processing time
            processing_duration.observe(time.time() - start_time)
            record_file_processed('success')
            
            self.logger.info(f"✅ Successfully processed file: {file_key}")
            return True
        
        except Exception as e:
            processing_errors.labels(error_type='general', step='processing').inc()
            record_file_processed('failed', folder_name)
            self.logger.error(f"Error processing {file_key}: {e}")
            return False
        finally:
            active_processing_jobs.dec()
    
    def get_folder_name_from_path(self, file_path: str) -> str:
        """Extract folder name from file path for KB sync mapping"""
        if '/' in file_path:
            return file_path.split('/')[0]
        else:
            return 'default'
    
    def process_folder(self, folder: str) -> Dict[str, Any]:
        """
        Process all PDFs in a folder with smart KB sync.
        
        Args:
            folder: Folder prefix to process
            
        Returns:
            Dictionary with processing results
        """
        try:
            files = self.s3_service.list_files_in_folder(SOURCE_BUCKET, folder)
            self.logger.info(f"[PROCESSING] Found {len(files)} files to process in folder: {folder}")
            
            results = {'total': len(files), 'success': 0, 'failed': 0}
            
            for i, file_key in enumerate(files, 1):
                self.logger.info(f"[PROCESSING] Processing file {i}/{len(files)}: {file_key}")
                
                if self.process_single_file(file_key):
                    results['success'] += 1
                    self.logger.info(f"[PROCESSING] Successfully processed: {file_key}")
                else:
                    results['failed'] += 1
                    self.logger.warning(f"[PROCESSING] Failed to process: {file_key}")
            
            # KB sync is now immediate - no final sync needed
            self.logger.info("[PROCESSING] ✅ All processing completed - KB sync handled immediately")
            
            # Log final summary
            self.logger.info(f"[PROCESSING] Processing completed for {folder}: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"[PROCESSING] Error processing folder {folder}: {str(e)}")
            return {'total': 0, 'success': 0, 'failed': 0}