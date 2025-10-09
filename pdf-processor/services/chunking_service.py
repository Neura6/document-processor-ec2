"""
PDF Chunking Service
Handles PDF chunking into individual pages with metadata.
"""

from PyPDF2 import PdfReader, PdfWriter
import os
from io import BytesIO
import logging
from typing import List, Dict, Any, Tuple
from config import CHUNKED_BUCKET, DIRECT_CHUNKED_BUCKET
from .metadata_service import MetadataService
from .metadata_page import MetadataPageService

logger = logging.getLogger(__name__)

class ChunkingService:
    """Service for chunking PDFs into individual pages."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metadata_service = MetadataService()
        self.metadata_page_service = MetadataPageService()
    
    def extract_metadata(self, key: str, page_number: int = 1, total_pages: int = 1, cleaned_key: str = None, chunk_type: str = "processed") -> Dict[str, Any]:
        """
        Extract metadata from S3 key path - exactly matching Lambda's extract_metadata_from_source_key
        
        Args:
            key: Original S3 key path
            page_number: Current page number
            total_pages: Total pages in document
            cleaned_key: Cleaned S3 key path (actual path where chunks will be saved)
            
        Returns:
            Dictionary with extracted metadata
        """
        parts = key.split('/')
        folder = parts[0]
        metadata = {'standard_type': folder}

        # Ensure enough parts exist before accessing indices - matching Lambda logic
        if folder in ['Auditing-global', 'Finance Tools', 'GIFT City','test']:
            if len(parts) > 1:
                if len(parts) > 2:
                    metadata['Standard_type'] = parts[1]  # Note: inconsistent key capitalization
                if len(parts) > 3:
                    metadata['document_type'] = parts[2]
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
        elif folder == 'accounting-global':
            if len(parts) > 1:
                metadata['complexity'] = parts[1]
                if len(parts) > 2:
                    metadata['Standard_type'] = parts[2]
                if len(parts) > 3:
                    metadata['document_type'] = parts[3]
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
        elif folder == 'Banking Regulations-test' and len(parts) > 1 and parts[1] == 'Bahrain':
            if len(parts) > 2:
                metadata['country'] = parts[1]
                metadata['complexity'] = parts[2]
                if len(parts) > 4:
                    metadata['document_type'] = parts[3]
                if len(parts) > 5:
                    metadata['document_category'] = parts[4]
                if len(parts) > 6:
                    metadata['document_sub-category'] = parts[5]
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
        elif folder == 'Banking-Regulations-Bahrain' and len(parts) > 1 and parts[1] == 'Bahrain':
            if len(parts) > 2:
                metadata['country'] = parts[1]
                metadata['volume'] = parts[2]
                if len(parts) > 4:
                    metadata['document_type'] = parts[3]
                if len(parts) > 5:
                    metadata['document_category'] = parts[4]
                if len(parts) > 6:
                    metadata['document_sub-category'] = parts[5]
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
        elif folder in ['accounting-standards','commercial-laws','Banking Regulations','Direct Taxes','Capital Market Regulations','Auditing Standards','Insurance','Labour Law']:
            if len(parts) > 1:
                metadata['country'] = parts[1]
                if len(parts) > 3:
                    metadata['document_type'] = parts[2]
                if len(parts) > 4:
                    metadata['document_category'] = parts[3]
                if len(parts) > 5:
                    metadata['document_sub-category'] = parts[4]
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
        elif folder == 'Indirect Taxes':
            if len(parts) > 1:
                metadata['country'] = parts[1]
                if len(parts) > 3:
                    metadata['document_type'] = parts[2]
                if len(parts) > 4:
                    metadata['State'] = parts[3]
                if len(parts) > 5:
                    metadata['State_category'] = parts[4]
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
        elif folder == 'usecase-reports-4':
            if len(parts) > 1:
                metadata['country'] = parts[1]
                if len(parts) > 2:
                    metadata['year'] = parts[2]
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
        else:
            # Handle generic case
            if len(parts) > 1:
                metadata['country'] = parts[1]
            metadata['document_name'] = os.path.splitext(parts[-1])[0]

        # Add page-specific metadata
        metadata['page_number'] = page_number
        metadata['total_pages'] = total_pages
        
        # NOTE: chunk_s3_uri generation is now handled in the specific async methods
        # (chunk_pdf_processed and chunk_pdf_direct) to support dual URI patterns
        # This method only extracts basic metadata, URIs are added later
            
        # CRITICAL DEBUG LOGGING - will show in logs
        self.logger.info(f"=== METADATA EXTRACTION DEBUG ===")
        self.logger.info(f"INPUT KEY: {key}")
        self.logger.info(f"CLEANED KEY: {cleaned_key}")
        self.logger.info(f"PARTS: {parts}")
        self.logger.info(f"FOLDER: {folder}")
        self.logger.info(f"COUNTRY: {metadata.get('country', 'NOT FOUND')}")
        self.logger.info(f"DOCUMENT_NAME: {metadata.get('document_name', 'NOT FOUND')}")
        self.logger.info(f"COMPLETE METADATA: {metadata}")
        self.logger.info(f"=== END DEBUG ===")

        return metadata
       
    def create_metadata_page(self, metadata: Dict[str, Any]):
        """
        Create a PDF page containing metadata using the MetadataPageService.
        
        Args:
            metadata: Dictionary with metadata from extract_metadata
            
        Returns:
            PyPDF2 PageObject with metadata
        """
        return self.metadata_page_service.create_metadata_page(metadata)
    
    def test_create_standalone_metadata_pdf(self, metadata: Dict[str, Any], output_path: str):
        """
        Test method to create a standalone metadata PDF file for debugging.
        This creates a complete PDF file with just the metadata page.
        """
        try:
            # Create metadata page
            metadata_page = self.metadata_page_service.create_metadata_page(metadata)
            
            # Create a new PDF with just this page
            writer = PdfWriter()
            writer.add_page(metadata_page)
            
            # Write to file
            with open(output_path, 'wb') as output_file:
                writer.write(output_file)
            
            self.logger.info(f"Test metadata PDF created: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error creating test metadata PDF: {e}")
    
    def chunk_pdf(self, pdf_stream: BytesIO, s3_key: str, cleaned_key: str = None) -> List[Tuple[PdfWriter, Dict[str, Any]]]:
        """
        Split PDF into individual pages with comprehensive metadata.
        
        Args:
            pdf_stream: PDF file as BytesIO
            s3_key: S3 key for metadata extraction
            cleaned_key: Cleaned S3 key path (actual path where chunks will be saved)
            
        Returns:
            List of (pdf_writer, metadata) tuples
        """
        try:
            pdf_stream.seek(0)
            reader = PdfReader(pdf_stream)
            
            if reader.is_encrypted:
                reader.decrypt('')
            
            total_pages = len(reader.pages)
            chunks = []
            
            for page_num in range(total_pages):
                writer = PdfWriter()
                
                # Get enhanced metadata for this specific page
                metadata = self.extract_metadata(s3_key, page_num + 1, total_pages, cleaned_key)
                
                # Create and prepend metadata page (Lambda style - metadata first)
                metadata_page = self.metadata_page_service.create_metadata_page(metadata)
                
                # DEBUG: Log page dimensions before adding to writer
                media_box_before = metadata_page.mediabox
                self.logger.info(f"BEFORE adding to writer - Page size: {float(media_box_before.width)}x{float(media_box_before.height)}")
                
                writer.add_page(metadata_page)
                
                # DEBUG: Check if writer has any pages and log their dimensions
                if len(writer.pages) > 0:
                    added_page = writer.pages[0]
                    media_box_after = added_page.mediabox
                    self.logger.info(f"AFTER adding to writer - Page size: {float(media_box_after.width)}x{float(media_box_after.height)}")
                
                # Add the actual content page (original or processed)
                content_page = reader.pages[page_num]
                content_media_box = content_page.mediabox
                self.logger.info(f"Content page size: {float(content_media_box.width)}x{float(content_media_box.height)}")
                
                writer.add_page(content_page)
                
                # DEBUG: Final check of all pages in writer
                self.logger.info(f"Final writer has {len(writer.pages)} pages")
                for i, page in enumerate(writer.pages):
                    page_media_box = page.mediabox
                    self.logger.info(f"Page {i+1} final size: {float(page_media_box.width)}x{float(page_media_box.height)}")
                
                chunks.append((writer, metadata))
            
            return chunks
            
        except Exception as e:
            self.logger.error(f"Error chunking PDF: {e}")
            return []
    
    async def chunk_pdf_processed(self, pdf_data: bytes, key: str, enhanced_pdf_data: bytes = None) -> List[Tuple[BytesIO, Dict[str, Any]]]:
        """
        Chunk PDF for processed stream (enhanced content) -> chunked-rules-repository -> KB sync
        
        Args:
            pdf_data: Original PDF data
            key: S3 key path (e.g., "test/space testing folder/NBB Q2 2025.pdf")
            enhanced_pdf_data: Enhanced PDF data (OCR + PDF-plumber processed)
            
        Returns:
            List of (chunk_stream, metadata) tuples
        """
        try:
            # Use enhanced data if available, otherwise use original
            data_to_chunk = enhanced_pdf_data if enhanced_pdf_data else pdf_data
            
            # Convert bytes to BytesIO stream
            pdf_stream = BytesIO(data_to_chunk)
            pdf_stream.seek(0)
            reader = PdfReader(pdf_stream)
            
            if reader.is_encrypted:
                reader.decrypt('')
            
            total_pages = len(reader.pages)
            processed_chunks = []
            
            for page_num in range(total_pages):
                # Get base metadata for this specific page
                metadata = self.extract_metadata(key, page_num + 1, total_pages)
                
                # Update metadata for processed stream
                metadata['processing_method'] = 'processed'
                metadata['chunk_type'] = 'processed'
                
                # Extract folder path and filename properly
                folder_path = '/'.join(key.split('/')[:-1]) if '/' in key else ''
                original_filename = key.split('/')[-1]
                base_filename = os.path.splitext(original_filename)[0]
                page_number = metadata.get('page_number', 1)
                
                # Generate normalized filename for S3 keys (replace spaces with underscores)
                normalized_base_filename = base_filename.replace(' ', '_')
                
                # Generate chunk filenames
                chunk_filename = f"{normalized_base_filename}_page_{page_number}.pdf"
                processed_chunk_filename = f"{normalized_base_filename}_page_{page_number}_processed.pdf"
                
                # Build full S3 keys with folder structure preserved
                if folder_path:
                    chunk_key = f"{folder_path}/{chunk_filename}"
                    processed_chunk_key = f"{folder_path}/{processed_chunk_filename}"
                else:
                    chunk_key = chunk_filename
                    processed_chunk_key = processed_chunk_filename
                
                # Dual URIs for processed chunks (stored in chunked-rules-repository)
                # 1. chunk_s3_uri_processed: Points to the processed chunk in chunked-rules-repository
                metadata['chunk_s3_uri_processed'] = f"s3://{CHUNKED_BUCKET}/{processed_chunk_key}"
                
                # 2. chunk_s3_uri: Points to the corresponding direct chunk in rules-repository-alpha
                metadata['chunk_s3_uri'] = f"s3://{DIRECT_CHUNKED_BUCKET}/{chunk_key}"
                
                # NOW create the metadata page with the complete metadata including URIs
                metadata_page = self.metadata_page_service.create_metadata_page(metadata)
                
                # Create writer and add pages
                writer = PdfWriter()
                writer.add_page(metadata_page)  # Metadata page first
                writer.add_page(reader.pages[page_num])  # Content page second
                
                # Convert writer to BytesIO
                chunk_stream = BytesIO()
                writer.write(chunk_stream)
                chunk_stream.seek(0)
                
                processed_chunks.append((chunk_stream, metadata))
                
                # Debug logging
                self.logger.info(f"Processed chunk metadata:")
                self.logger.info(f"  Original key: {key}")
                self.logger.info(f"  Folder path: {folder_path}")
                self.logger.info(f"  Base filename: {base_filename}")
                self.logger.info(f"  Normalized: {normalized_base_filename}")
                self.logger.info(f"  chunk_s3_uri_processed: {metadata['chunk_s3_uri_processed']}")
                self.logger.info(f"  chunk_s3_uri: {metadata['chunk_s3_uri']}")
                
            self.logger.info(f"Created {len(processed_chunks)} processed chunks for {key}")
            return processed_chunks
            
        except Exception as e:
            self.logger.error(f"Error in processed chunking for {key}: {e}")
            return []
    
    async def chunk_pdf_direct(self, pdf_data: bytes, key: str) -> List[Tuple[BytesIO, Dict[str, Any]]]:
        """
        Chunk PDF for direct stream (original content) -> rules-repository-alpha -> Storage only
        
        Args:
            pdf_data: Original PDF data
            key: S3 key path (e.g., "test/space testing folder/NBB Q2 2025.pdf")
            
        Returns:
            List of (chunk_stream, metadata) tuples
        """
        try:
            # Convert bytes to BytesIO stream
            pdf_stream = BytesIO(pdf_data)
            pdf_stream.seek(0)
            reader = PdfReader(pdf_stream)
            
            if reader.is_encrypted:
                reader.decrypt('')
            
            total_pages = len(reader.pages)
            direct_chunks = []
            
            for page_num in range(total_pages):
                # Get base metadata for this specific page
                metadata = self.extract_metadata(key, page_num + 1, total_pages)
                
                # Update metadata for direct stream
                metadata['processing_method'] = 'direct'
                metadata['chunk_type'] = 'direct'
                
                # Extract folder path and filename properly
                folder_path = '/'.join(key.split('/')[:-1]) if '/' in key else ''
                original_filename = key.split('/')[-1]
                base_filename = os.path.splitext(original_filename)[0]
                page_number = metadata.get('page_number', 1)
                
                # Generate normalized filename for S3 keys (replace spaces with underscores)
                normalized_base_filename = base_filename.replace(' ', '_')
                
                # Generate chunk filename
                chunk_filename = f"{normalized_base_filename}_page_{page_number}.pdf"
                
                # Build full S3 key with folder structure preserved
                if folder_path:
                    chunk_key = f"{folder_path}/{chunk_filename}"
                else:
                    chunk_key = chunk_filename
                
                # Single URI for direct chunks (stored in rules-repository-alpha)
                # chunk_s3_uri: Points to this direct chunk in rules-repository-alpha
                metadata['chunk_s3_uri'] = f"s3://{DIRECT_CHUNKED_BUCKET}/{chunk_key}"
                
                # NOW create the metadata page with the complete metadata including URI
                metadata_page = self.metadata_page_service.create_metadata_page(metadata)
                
                # Create writer and add pages
                writer = PdfWriter()
                writer.add_page(metadata_page)  # Metadata page first
                writer.add_page(reader.pages[page_num])  # Content page second
                
                # Convert writer to BytesIO
                chunk_stream = BytesIO()
                writer.write(chunk_stream)
                chunk_stream.seek(0)
                
                direct_chunks.append((chunk_stream, metadata))
                
                # Debug logging
                self.logger.info(f"Direct chunk metadata:")
                self.logger.info(f"  Original key: {key}")
                self.logger.info(f"  Folder path: {folder_path}")
                self.logger.info(f"  Base filename: {base_filename}")
                self.logger.info(f"  Normalized: {normalized_base_filename}")
                self.logger.info(f"  chunk_s3_uri: {metadata['chunk_s3_uri']}")
                
            self.logger.info(f"Created {len(direct_chunks)} direct chunks for {key}")
            return direct_chunks
            
        except Exception as e:
            self.logger.error(f"Error in direct chunking for {key}: {e}")
            return []
