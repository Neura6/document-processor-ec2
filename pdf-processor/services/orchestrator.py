"""
Main Orchestrator Service
Coordinates all services to process PDF files end-to-end.
"""

import sys
import boto3
import io
import os
import logging
from typing import List, Dict, Any
import PyPDF2
from services.filename_service import FilenameService
from services.watermark_service import WatermarkService
from services.ocr_service import OCRService
from services.chunking_service import ChunkingService
from services.s3_service import S3Service
from services.conversion_service import ConversionService
from services.folder_sync_tracker import FolderSyncTracker
import logging
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SOURCE_BUCKET, CHUNKED_BUCKET

class Orchestrator:
    """Main orchestrator for PDF processing workflow."""
    
    def __init__(self):
        self.conversion_service = ConversionService()
        self.sync_tracker = FolderSyncTracker()
        
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
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('pdf_processor.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Orchestrator initialized successfully")
    
    def process_single_file(self, file_key: str) -> bool:
        """
        Process a single file through the complete pipeline.
        
        Args:
            file_key: S3 object key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Starting processing for: {file_key}")
            
            # Step 0: Check if already chunked (skip processing)
            chunked_key = f"{os.path.splitext(file_key)[0]}_page_1.pdf"
            if self.s3_service.object_exists(CHUNKED_BUCKET, chunked_key):
                self.logger.info(f"File already chunked: {file_key}, skipping processing")
                return True
            
            # Step 1: Check file format and convert if necessary
            extension = os.path.splitext(file_key)[1].lower()
            if self.conversion_service.is_convertible_format(file_key):
                self.logger.info("Step 0: Converting document to PDF")
                file_bytes = self.s3_service.get_object(SOURCE_BUCKET, file_key)
                pdf_content, converted_filename = self.conversion_service.convert_to_pdf(file_bytes, file_key)
                
                if pdf_content is None:
                    self.logger.error(f"Failed to convert {file_key} to PDF")
                    return False
                
                # Update file_key to use the converted filename
                file_key = converted_filename
                pdf_stream = io.BytesIO(pdf_content)
                self.logger.info(f"Successfully converted to {converted_filename}")
            else:
                # Already PDF, download normally
                pdf_bytes = self.s3_service.get_object(SOURCE_BUCKET, file_key)
                pdf_stream = io.BytesIO(pdf_bytes)
            
            # Step 1: Clean filename if needed
            self.logger.info("Step 1: Cleaning filename")
            cleaned_key = self.filename_service.clean_filename(file_key)
            if cleaned_key != file_key:
                # Handle filename cleaning (copy new, delete old)
                if not self.s3_service.object_exists(SOURCE_BUCKET, cleaned_key):
                    self.s3_service.copy_object(SOURCE_BUCKET, file_key, SOURCE_BUCKET, cleaned_key)
                    self.s3_service.delete_object(SOURCE_BUCKET, file_key)
                    file_key = cleaned_key
                    self.logger.info(f"File renamed to: {file_key}")
                else:
                    self.logger.warning(f"Target key {cleaned_key} already exists, skipping rename")
            
            # Step 2: Remove watermarks
            self.logger.info("Step 2: Removing watermarks")
            watermark_result = self.watermark_service.remove_watermarks(pdf_stream, file_key)
            if watermark_result[0]:
                pdf_stream = watermark_result[0]
                if watermark_result[1]:
                    self.logger.info(f"Removed pages: {watermark_result[1]}")
                self.logger.info("Watermark processing completed")
            else:
                self.logger.info("No watermarks found, continuing")
            
            # Step 3: Apply OCR if needed
            self.logger.info("Step 3: Applying OCR")
            ocr_result = self.ocr_service.apply_ocr_to_pdf(pdf_stream, file_key)
            if ocr_result[0]:
                pdf_stream = ocr_result[0]
                if ocr_result[1]:
                    self.logger.info(f"OCR applied to pages: {ocr_result[1]}")
                self.logger.info("OCR processing completed")
            else:
                self.logger.info("No OCR needed, continuing")
            
            # Step 4: Chunk PDF
            self.logger.info("Step 4: Chunking PDF")
            chunks = self.chunking_service.chunk_pdf(pdf_stream, file_key)
            if not chunks:
                self.logger.error("Failed to chunk PDF")
                return False
            
            # Step 5: Upload chunks to S3
            self.logger.info("Step 5: Uploading chunks to S3")
            success_count = 0
            for writer, metadata in chunks:
                output = io.BytesIO()
                writer.write(output)
                output.seek(0)
                
                page_num = metadata.get('page_number', 1)
                chunk_key = f"{os.path.splitext(file_key)[0]}_page_{page_num}.pdf"
                
                if self.s3_service.put_object(CHUNKED_BUCKET, chunk_key, output.getvalue()):
                    success_count += 1
                    self.logger.info(f"Uploaded chunk: {chunk_key}")
            
            # Step 6: Track and trigger KB sync
            self.logger.info("Step 6: Tracking KB sync batch")
            try:
                from services.kb_sync_service import KBIngestionService
                kb_service = KBIngestionService(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                
                # Track file and check if sync should trigger
                folder_name = file_key.split('/')[0] if '/' in file_key else 'default'
                should_sync = self.sync_tracker.increment_and_check_sync(folder_name)
                
                if should_sync:
                    self.logger.info(f"[KB-SYNC] 🔄 BATCH SYNC TRIGGERED for {folder_name} after 20 files")
                    try:
                        if folder_name in kb_service.get_kb_mapping():
                            self.logger.info(f"[KB-SYNC] 📡 Starting KB sync for {folder_name}")
                            kb_result = kb_service.sync_to_knowledge_base_simple(folder_name)
                            if kb_result.get('status') == 'COMPLETE':
                                self.logger.info(f"[KB-SYNC] ✅ Successfully synced {folder_name} to Knowledge Base")
                            else:
                                self.logger.warning(f"[KB-SYNC] ⚠️ KB sync completed with status: {kb_result.get('status')}")
                        else:
                            self.logger.info(f"[KB-SYNC] ❌ No KB mapping found for folder: {folder_name}, skipping sync")
                    except Exception as e:
                        self.logger.error(f"[KB-SYNC] ❌ Error during KB sync: {e}")
            except Exception as e:
                self.logger.error(f"[KB-SYNC] ❌ Error during KB sync setup: {e}")
        
            return success_count > 0
        
        except Exception as e:
            logging.error(f"Error processing {file_key}: {e}")
            return False
    
    def process_folder(self, folder: str) -> Dict[str, int]:
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
            
            # Final sync for remaining files (< 20)
            pending_folders = self.sync_tracker.get_all_pending_folders()
            self.logger.info(f"[KB-SYNC] 📊 FINAL SYNC CHECK: {len(pending_folders)} folders need KB sync")
            
            total_synced = 0
            total_failed = 0
            
            for folder_name, count in pending_folders.items():
                if count > 0:
                    self.logger.info(f"[KB-SYNC] 🎯 Processing {folder_name}: {count} files to sync")
                    try:
                        from services.kb_sync_service import KBIngestionService
                        kb_service = KBIngestionService(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                        
                        if folder_name in kb_service.get_kb_mapping():
                            self.logger.info(f"[KB-SYNC] 🚀 Starting KB sync for {folder_name}")
                            self.logger.info(f"[KB-SYNC] 📋 KB ID: {kb_service.get_kb_mapping()[folder_name]['id']}")
                            self.logger.info(f"[KB-SYNC] 📁 Data Source: {kb_service.get_kb_mapping()[folder_name]['data_source_id']}")
                            
                            kb_result = kb_service.sync_to_knowledge_base_simple(folder_name)
                            
                            if kb_result.get('status') == 'COMPLETE':
                                self.logger.info(f"[KB-SYNC] ✅ SUCCESS! {folder_name} fully synced to Knowledge Base")
                                self.logger.info(f"[KB-SYNC] ⏱️  Sync duration: {kb_result.get('duration', 0):.1f}s")
                                total_synced += count
                            else:
                                self.logger.warning(f"[KB-SYNC] ⚠️  {folder_name} sync completed with status: {kb_result.get('status')}")
                                if kb_result.get('failed_files'):
                                    self.logger.warning(f"[KB-SYNC] 📄 Failed files: {len(kb_result.get('failed_files', []))}")
                                total_failed += count
                        else:
                            self.logger.warning(f"[KB-SYNC] ❌ No KB mapping found for folder: {folder_name}")
                            total_failed += count
                        
                        self.sync_tracker.reset_count(folder_name)
                    except Exception as e:
                        self.logger.error(f"[KB-SYNC] 🔥 Error during KB sync for {folder_name}: {e}")
                        total_failed += count
            
            # Final summary
            self.logger.info(f"[KB-SYNC] 🎉 KB SYNC SUMMARY:")
            self.logger.info(f"[KB-SYNC] ✅ Successfully synced: {total_synced} files")
            self.logger.info(f"[KB-SYNC] ❌ Failed/Skipped: {total_failed} files")
            self.logger.info(f"[KB-SYNC] 📊 Total processed: {total_synced + total_failed} files")
            
            self.logger.info(f"[PROCESSING] Processing completed for {folder}: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"[PROCESSING] Error processing folder {folder}: {str(e)}")
            return {'total': 0, 'success': 0, 'failed': 0}
