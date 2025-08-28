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
        Extract metadata from S3 key exactly as done in data_cleaner.py
        
        Args:
            key: S3 object key
            page_number: Current page number (1-indexed)
            total_pages: Total pages in the PDF
            
        Returns:
            Dictionary with metadata matching data_cleaner.py format
        """
        parts = key.split('/')
        folder = parts[0]
        filename = parts[-1]
        
        # Build metadata exactly like data_cleaner.py
        metadata = {
            'document_name': filename.rsplit('.', 1)[0],
            'processed_file_path': key,
            'page_number': page_number,
            'total_pages': total_pages,
            'chunk_s3_uri': f"s3://{CHUNKED_BUCKET}/{key.rsplit('.', 1)[0]}_page_{page_number}.pdf"
        }
        
        # Extract country, document_type, standard_type from path structure
        if len(parts) >= 1:
            metadata['standard_type'] = parts[0]
        
        if len(parts) >= 2:
            metadata['country'] = parts[1]
        
        if len(parts) >= 3:
            metadata['document_type'] = parts[2]
        elif len(parts) >= 2:
            metadata['document_type'] = filename
        
        # Handle special folder structures like original
        if folder == 'Banking Regulations':
            metadata['standard_type'] = 'Banking Regulation'
        elif folder == 'Banking Regulations-test' and len(parts) > 1 and parts[1] == 'Bahrain':
            # Special handling for Banking Regulations-test Bahrain structure
            if len(parts) > 2:
                metadata['country'] = parts[1]
                metadata['complexity'] = parts[2]
            if len(parts) > 3:
                metadata['document_type'] = parts[3]
            if len(parts) > 4:
                metadata['document_category'] = parts[4]
        elif folder == 'Direct Taxes':
            metadata['standard_type'] = 'Direct Tax'
        elif folder == 'Indirect Taxes':
            metadata['standard_type'] = 'Indirect Tax'
        
        return metadata
    
    def create_metadata_page(self, metadata: Dict[str, Any]) -> PyPDF2.PageObject:
        """
        Create a PDF page containing metadata exactly as done in data_cleaner.py
        
        Args:
            metadata: Dictionary with metadata from extract_metadata
            
        Returns:
            PyPDF2 PageObject with metadata
        """
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
        
        packet = BytesIO()
        c = canvas.Canvas(packet, pagesize=letter)
        c.setFont("Helvetica", 8)
        y = 750
        
        # Display metadata exactly like data_cleaner.py
        metadata_fields = [
            ('Document Name', 'document_name'),
            ('Country', 'country'),
            ('Document Type', 'document_type'),
            ('Standard Type', 'standard_type'),
            ('Page Number', 'page_number'),
            ('Total Pages', 'total_pages'),
            ('Processed Path', 'processed_file_path'),
            ('S3 URI', 'chunk_s3_uri')
        ]
        
        for display_name, field_name in metadata_fields:
            value = str(metadata.get(field_name, "N/A"))
            c.drawString(100, y, f"{display_name}: {value}")
            y -= 20
            if y < 50:
                c.showPage()
                c.setFont("Helvetica", 8)
                y = 750
        
        c.showPage()
        c.save()
        packet.seek(0)
        
        reader = PyPDF2.PdfReader(packet)
        return reader.pages[0] if reader.pages else None
    
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
        Create a PDF page with metadata information.
        
        Args:
            metadata: Dictionary containing metadata
            
        Returns:
            PyPDF2 PageObject with metadata
        """
        try:
            # Create a new PDF with metadata
            packet = BytesIO()
            c = canvas.Canvas(packet, pagesize=letter)
            width, height = letter
            
            # Title
            c.setFont("Helvetica-Bold", 16)
            c.drawString(50, height - 50, "Document Metadata")
            
            # Metadata content
            c.setFont("Helvetica", 12)
            y_position = height - 100
            
            for key, value in metadata.items():
                if y_position < 50:  # Prevent going off page
                    break
                
                # Format key for display
                display_key = key.replace('_', ' ').title()
                display_value = str(value)
                
                # Handle long values
                if len(display_value) > 80:
                    display_value = display_value[:77] + "..."
                
                c.drawString(50, y_position, f"{display_key}: {display_value}")
                y_position -= 20
            
            # Add timestamp
            c.setFont("Helvetica", 10)
            c.drawString(50, 30, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            c.save()
            packet.seek(0)
            
            # Convert to PyPDF2 PageObject
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
