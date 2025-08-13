"""
PDF Chunking Service
Handles PDF chunking into individual pages with metadata.
"""

import PyPDF2
from io import BytesIO
import logging
from typing import List, Dict, Any, Tuple
from config import CHUNKED_BUCKET

logger = logging.getLogger(__name__)

class ChunkingService:
    """Service for chunking PDFs into individual pages."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
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
                
                # Add ONLY the actual content page (no metadata page)
                writer.add_page(reader.pages[page_num])
                
                chunks.append((writer, metadata))
            
            return chunks
            
        except Exception as e:
            self.logger.error(f"Error chunking PDF: {e}")
            return []
