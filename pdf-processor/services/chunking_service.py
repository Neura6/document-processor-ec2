"""
PDF Chunking Service
Handles PDF chunking into individual pages with metadata.
"""

import PyPDF2
import os
from io import BytesIO
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter, landscape
from config import CHUNKED_BUCKET
from .metadata_service import MetadataService

logger = logging.getLogger(__name__)

class ChunkingService:
    """Service for chunking PDFs into individual pages."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metadata_service = MetadataService()
    
    def extract_metadata(self, key: str, page_number: int = 1, total_pages: int = 1, cleaned_key: str = None) -> Dict[str, Any]:
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
        if folder in ['Auditing-global', 'Finance Tools', 'GIFT City']:
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
        
        # Generate chunk_s3_uri EXACTLY as orchestrator generates chunk_key
        # Use cleaned_key if provided (from orchestrator), otherwise use original key
        actual_key = cleaned_key if cleaned_key else key
        
        # Extract folder path and filename exactly as orchestrator does
        folder_path = '/'.join(actual_key.split('/')[:-1]) if '/' in actual_key else ''
        filename_only = actual_key.split('/')[-1]
        base_name = os.path.splitext(filename_only)[0]
        
        # Generate chunk_s3_uri EXACTLY as orchestrator generates chunk_key
        if folder_path:
            chunk_key = f"{folder_path}/{base_name}_page_{page_number}.pdf"
        else:
            chunk_key = f"{base_name}_page_{page_number}.pdf"
            
        # Ensure no spaces in chunk key (replace with underscores) - same as orchestrator
        chunk_key = chunk_key.replace(' ', '_')
        
        # EXTRA SAFETY: Also replace any remaining spaces in the full URI
        chunk_s3_uri = f"s3://{CHUNKED_BUCKET}/{chunk_key}"
        chunk_s3_uri = chunk_s3_uri.replace(' ', '_')  # Replace any remaining spaces
        metadata['chunk_s3_uri'] = chunk_s3_uri
            
        # CRITICAL DEBUG LOGGING - will show in logs
        self.logger.info(f"=== METADATA EXTRACTION DEBUG ===")
        self.logger.info(f"INPUT KEY: {key}")
        self.logger.info(f"CLEANED KEY: {cleaned_key}")
        self.logger.info(f"ACTUAL KEY USED: {actual_key}")
        self.logger.info(f"PARTS: {parts}")
        self.logger.info(f"FOLDER: {folder}")
        self.logger.info(f"FOLDER_PATH: {folder_path}")
        self.logger.info(f"BASE_NAME: {base_name}")
        self.logger.info(f"CHUNK_KEY: {chunk_key}")
        self.logger.info(f"COUNTRY: {metadata.get('country', 'NOT FOUND')}")
        self.logger.info(f"DOCUMENT_NAME: {metadata.get('document_name', 'NOT FOUND')}")
        self.logger.info(f"FINAL S3 URI: {metadata['chunk_s3_uri']}")
        self.logger.info(f"COMPLETE METADATA: {metadata}")
        self.logger.info(f"=== END DEBUG ===")

        return metadata
       
    def create_metadata_page(self, metadata: Dict[str, Any]) -> PyPDF2.PageObject:
        """
        Create a PDF page containing metadata exactly as done in Lambda's create_metadata_page_content
        
        Args:
            metadata: Dictionary with metadata from extract_metadata
            
        Returns:
            PyPDF2 PageObject with metadata
        """
        return self._create_metadata_page(metadata)
    
    def chunk_pdf(self, pdf_stream: BytesIO, s3_key: str, cleaned_key: str = None) -> List[Tuple[PyPDF2.PdfWriter, Dict[str, Any]]]:
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
            reader = PyPDF2.PdfReader(pdf_stream)
            
            if reader.is_encrypted:
                reader.decrypt('')
            
            total_pages = len(reader.pages)
            chunks = []
            
            for page_num in range(total_pages):
                writer = PyPDF2.PdfWriter()
                
                # Get enhanced metadata for this specific page
                metadata = self.extract_metadata(s3_key, page_num + 1, total_pages, cleaned_key)
                
                # Create and prepend metadata page (Lambda style - metadata first)
                metadata_page = self._create_metadata_page(metadata)
                writer.add_page(metadata_page)
                
                # Add the actual content page (original or processed)
                writer.add_page(reader.pages[page_num])
                
                chunks.append((writer, metadata))
            
            return chunks
            
        except Exception as e:
            self.logger.error(f"Error chunking PDF: {e}")
            return []

    def _create_metadata_page(self, metadata: Dict[str, Any]) -> PyPDF2.PageObject:
        """
        Create a PDF page with metadata information in structured table format.
        
        Args:
            metadata: Dictionary containing metadata
            
        Returns:
            PyPDF2 PageObject with metadata
        """
        try:
            # Use PyMuPDF (fitz) to create landscape page - more reliable than ReportLab
            import fitz  # PyMuPDF
            
            # Create a new PDF document with landscape page
            doc = fitz.open()  # Create new PDF
            landscape_page = doc.new_page(width=792, height=612)  # Landscape: 792x612
            
            self.logger.info(f"Created landscape page using PyMuPDF: 792x612")
            
            # Add metadata table to the landscape page using PyMuPDF
            y_pos = 80  # Start position from top
            row_height = 25
            col1_x = 50   # Field name column
            col2_x = 200  # Field value column
            
            # Add table header
            landscape_page.insert_text((col1_x, y_pos), "Field", fontsize=12, color=(0, 0, 0))
            landscape_page.insert_text((col2_x, y_pos), "Value", fontsize=12, color=(0, 0, 0))
            
            # Draw header line
            landscape_page.draw_line((col1_x, y_pos + 10), (col1_x + 650, y_pos + 10))
            
            y_pos += row_height
            
            self.logger.info(f"Added table header to landscape page")
            
            # Define field display order and labels
            field_labels = {
                'document_name': 'Document Name',
                'processed_file_path': 'Processed File Path', 
                'page_number': 'Page Number',
                'total_pages': 'Total Pages',
                'chunk_s3_uri': 'Chunk S3 Uri',
                'standard_type': 'Standard Type',
                'country': 'Country',
                'document_type': 'Document Type',
                'document_category': 'Document Category',
                'document_sub-category': 'Document Sub-Category',
                'year': 'Year',
                'state': 'State',
                'State': 'State',  # Handle both capitalizations
                'state_category': 'State Category',
                'State_category': 'State Category',  # Handle both capitalizations
                'Standard_type': 'Standard Type',  # Handle capitalization inconsistency
                'complexity': 'Complexity'
            }
            
            # Add metadata fields using PyMuPDF
            for key, label in field_labels.items():
                if key in metadata:
                    value_str = str(metadata[key]) if metadata[key] is not None else "None"
                    
                    # Add field name
                    landscape_page.insert_text((col1_x, y_pos), f"{label}:", fontsize=10, color=(0, 0, 0))
                    
                    # Special handling for URIs to prevent spaces when extracted
                    if key == 'chunk_s3_uri' or 'uri' in key.lower():
                        # Use full landscape width for URIs (792 - 200 - 50 margins)
                        available_width = 540  # Full landscape width available
                        
                        # Start with readable font size for landscape
                        font_size = 8
                        
                        # Calculate text width (approximate)
                        text_width = len(value_str) * font_size * 0.6  # Rough estimate
                        
                        # Reduce font size until it fits in one line
                        while text_width > available_width and font_size > 4:
                            font_size -= 1
                            text_width = len(value_str) * font_size * 0.6
                        
                        self.logger.info(f"URI font size: {font_size}, estimated width: {text_width}, available: {available_width}")
                        
                        # Add the URI directly on landscape page
                        landscape_page.insert_text((col2_x, y_pos), value_str, fontsize=font_size, color=(0, 0, 0))
                    else:
                        # Add short values normally
                        landscape_page.insert_text((col2_x, y_pos), value_str, fontsize=10, color=(0, 0, 0))
                    
                    y_pos += row_height
            
            # Convert PyMuPDF document to bytes
            pdf_bytes = doc.tobytes()
            doc.close()
            
            # Convert to PyPDF2 format
            packet = BytesIO(pdf_bytes)
            metadata_pdf = PyPDF2.PdfReader(packet)
            final_page = metadata_pdf.pages[0]
            
            # Verify dimensions
            media_box = final_page.mediabox
            width = float(media_box.width)
            height = float(media_box.height)
            self.logger.info(f"Final PyMuPDF landscape page dimensions: {width}x{height}")
            
            return final_page
            
        except Exception as e:
            self.logger.error(f"Error creating metadata page: {e}")
            # Return empty page if metadata creation fails - also use landscape
            packet = BytesIO()
            c = canvas.Canvas(packet, pagesize=(792, 612))  # Landscape fallback too
            c.showPage()
            c.save()
            packet.seek(0)
            return PyPDF2.PdfReader(packet).pages[0]
    