"""
PDF Chunking Service
Handles PDF chunking into individual pages with metadata.
"""

import PyPDF2
from io import BytesIO
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from config import CHUNKED_BUCKET
from .metadata_service import MetadataService

logger = logging.getLogger(__name__)

class ChunkingService:
    """Service for chunking PDFs into individual pages."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metadata_service = MetadataService()
    
    def extract_metadata(self, key: str, page_number: int = 1, total_pages: int = 1) -> Dict[str, Any]:
        """
        Extract metadata from S3 key exactly as done in Lambda's extract_metadata_from_source_key
        
        Args:
            key: S3 object key
            page_number: Current page number (1-indexed)
            total_pages: Total pages in the PDF
            
        Returns:
            Dictionary with metadata matching Lambda format
        """
        parts = key.split('/')
        folder = parts[0]
        metadata = {'standard_type': folder}

        # Ensure enough parts exist before accessing indices
        if folder in ['Auditing-global', 'Finance Tools', 'GIFT City']:
            if len(parts) > 1:
                if len(parts) > 2:
                    metadata['Standard_type'] = parts[1]
                if len(parts) > 3:
                    metadata['document_type'] = parts[2]
                if len(parts) > 1:
                    metadata['document_name'] = parts[-1].rsplit('.', 1)[0]
        elif folder == 'accounting-global':
            if len(parts) > 1:
                metadata['complexity'] = parts[1]
                if len(parts) > 2:
                    metadata['Standard_type'] = parts[2]
                if len(parts) > 3:
                    metadata['document_type'] = parts[3]
                if len(parts) > 1:
                    metadata['document_name'] = parts[-1].rsplit('.', 1)[0]
        elif folder in ['accounting-standards','commercial-laws','Banking Regulations','Direct Taxes','Capital Market Regulations','Auditing Standards','Insurance','Labour Law']:
            if len(parts) > 1:
                metadata['country'] = parts[1]
                if len(parts) > 3:
                    metadata['document_type'] = parts[2]
                if len(parts) > 4:
                    metadata['document_category'] = parts[3]
                if len(parts) > 5:
                    metadata['document_sub-category'] = parts[4]
                if len(parts) > 1:
                    metadata['document_name'] = parts[-1].rsplit('.', 1)[0]
        elif folder == 'Indirect Taxes':
            if len(parts) > 1:
                metadata['country'] = parts[1]
                if len(parts) > 3:
                    metadata['document_type'] = parts[2]
                if len(parts) > 4:
                    metadata['State'] = parts[3]
                if len(parts) > 5:
                    metadata['State_category'] = parts[4]
                if len(parts) > 1:
                    metadata['document_name'] = parts[-1].rsplit('.', 1)[0]
        elif folder == 'usecase-reports-4':
            if len(parts) > 1:
                metadata['country'] = parts[1]
                if len(parts) > 2:
                    metadata['year'] = parts[2]
                if len(parts) > 1:
                    metadata['document_name'] = parts[-1].rsplit('.', 1)[0]

        # Add page-specific metadata
        metadata['page_number'] = page_number
        metadata['total_pages'] = total_pages
        metadata['chunk_s3_uri'] = f"s3://{CHUNKED_BUCKET}/{key.rsplit('.', 1)[0]}_page_{page_number}.pdf"
        metadata['processed_file_path'] = key

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
    
    def chunk_pdf(self, pdf_stream: BytesIO, s3_key: str) -> List[Tuple[PyPDF2.PdfWriter, Dict[str, Any]]]:
        """
        Split PDF into individual pages with comprehensive metadata.
        
        Args:
            pdf_stream: PDF file as BytesIO
            s3_key: S3 key for metadata extraction
            
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
                metadata = self.extract_metadata(s3_key, page_num + 1, total_pages)
                
                # Create metadata page
                metadata_page = self._create_metadata_page(metadata)
                writer.add_page(metadata_page)
                
                # Add the actual content page
                writer.add_page(reader.pages[page_num])
                
                chunks.append((writer, metadata))
            
            return chunks
            
        except Exception as e:
            self.logger.error(f"Error chunking PDF: {e}")
            return []

    def _create_metadata_page(self, metadata: Dict[str, Any]) -> PyPDF2.PageObject:
        """
        Create a PDF page with metadata information exactly as done in Lambda's create_metadata_page_content.
        
        Args:
            metadata: Dictionary containing metadata
            
        Returns:
            PyPDF2 PageObject with metadata
        """
        try:
            # Create a new PDF with metadata - Lambda style
            packet = BytesIO()
            c = canvas.Canvas(packet, pagesize=letter)
            # Set font and starting position - Lambda style
            c.setFont("Helvetica", 8)
            y = 750  # Starting y coordinate

            # Draw each metadata item - Lambda style
            for key, value in metadata.items():
                # Handle None values by converting to string "None" or skipping
                value_str = str(value) if value is not None else "None"
                c.drawString(100, y, f"{key}: {value_str}")
                y -= 12  # Move down for the next line (reduced spacing like Lambda)

            c.showPage()
            c.save()
            packet.seek(0)
            # Return the BytesIO object containing the PDF metadata page content
            reader = PyPDF2.PdfReader(packet)
            return reader.pages[0]
            
        except Exception as e:
            self.logger.error(f"Error creating metadata page: {e}")
            # Return empty page if metadata creation fails
            packet = BytesIO()
            c = canvas.Canvas(packet, pagesize=letter)
            c.drawString(50, 400, "Metadata unavailable")
            c.save()
            packet.seek(0)
            reader = PyPDF2.PdfReader(packet)
            return reader.pages[0]
