"""
Metadata Page Creation Service
Handles creation of custom wide metadata pages for PDF chunks.
Extracted from metadata_fixer.py to integrate with chunking service flow.
"""

import io
import logging
from typing import Dict, Any
from datetime import datetime, timezone, timedelta
from reportlab.pdfgen import canvas
from PyPDF2 import PdfReader

logger = logging.getLogger(__name__)

class MetadataPageService:
    """Service for creating custom wide metadata pages for PDF chunks."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def create_corrected_metadata_page(self, metadata: Dict[str, Any]):
        """
        Create a new metadata page with corrected chunk_s3_uri in table format (custom wide format).
        EXACT COPY from metadata_fixer.py create_corrected_metadata_page method.
        
        Args:
            metadata: Dictionary containing metadata
            
        Returns:
            PyPDF2 PageObject with metadata
        """
        try:
            packet = io.BytesIO()
            # Custom page size: wider and shorter to fit S3 URIs on single line
            # Standard landscape letter is 792x612, we'll use 1000x500 (much wider, shorter)
            custom_page_size = (1000, 500)  # (width, height) in points
            c = canvas.Canvas(packet, pagesize=custom_page_size)
            
            # Log the page size for debugging
            self.logger.info(f"Created PDF with page size: {custom_page_size}")
            
            # Title - centered for custom wide page (1000px width)
            c.setFont("Helvetica-Bold", 16)
            c.drawString(400, 460, "Document Metadata")
            
            # Table setup - optimized for wide custom page (1000px width)
            c.setFont("Helvetica", 10)
            y_start = 430
            row_height = 22
            col1_x = 50   # Field name column
            col2_x = 170  # Field value column (plenty of space)
            table_width = 900  # Very wide table for custom page
            
            # Draw table header
            c.setFont("Helvetica-Bold", 10)
            c.drawString(col1_x, y_start, "Field")
            c.drawString(col2_x, y_start, "Value")
            
            # Draw header line
            c.line(col1_x, y_start - 5, col1_x + table_width, y_start - 5)
            
            # Draw table rows
            c.setFont("Helvetica", 10)
            y = y_start - row_height
            
            # Define field display order and labels - enhanced from metadata_fixer
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
            
            for key, label in field_labels.items():
                if key in metadata:
                    value_str = str(metadata[key]) if metadata[key] is not None else "None"
                    
                    # Draw field name
                    c.drawString(col1_x, y, f"{label}:")
                    
                    # Handle long values - custom wide page can fit S3 URIs on single line
                    if len(value_str) > 120:  # Very high limit for wide page
                        # Only break extremely long values (longer than typical S3 URIs)
                        max_chars = 160  # Much more characters per line for wide page
                        lines = []
                        for i in range(0, len(value_str), max_chars):
                            lines.append(value_str[i:i+max_chars])
                        
                        # Draw first line
                        c.drawString(col2_x, y, lines[0])
                        
                        # Draw additional lines with proper spacing
                        for i, line in enumerate(lines[1:], 1):
                            y -= 14  # Slightly more spacing for readability
                            c.drawString(col2_x, y, line)
                    else:
                        # Draw values normally (most S3 URIs will fit on single line)
                        c.drawString(col2_x, y, value_str)
                    
                    y -= row_height
                    
                    # Add separator line between rows
                    c.setStrokeColorRGB(0.8, 0.8, 0.8)
                    c.line(col1_x, y + 10, col1_x + table_width, y + 10)
                    c.setStrokeColorRGB(0, 0, 0)  # Reset to black
            
            # Draw table border
            c.rect(col1_x - 10, y, table_width + 20, y_start - y + 20)
            
            # Add timestamp in IST - positioned for custom wide page
            c.setFont("Helvetica", 8)
            ist = timezone(timedelta(hours=5, minutes=30))  # IST is UTC+5:30
            current_time_ist = datetime.now(ist)
            c.drawString(col1_x, y - 30, f"Generated: {current_time_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
            c.drawString(col1_x + 500, y - 30, f"Format: Wide Metadata Page (1000x500) - Single Line URIs")
            
            c.showPage()
            c.save()
            packet.seek(0)
            
            # Convert to PDF page - EXACT MATCH TO metadata_fixer.py
            metadata_pdf = PdfReader(packet)
            final_page = metadata_pdf.pages[0]
            
            # Log success with actual dimensions
            media_box = final_page.mediabox
            actual_width = float(media_box.width)
            actual_height = float(media_box.height)
            self.logger.info(f"Created custom wide metadata page - Size: {actual_width}x{actual_height}")
            
            return final_page
            
        except Exception as e:
            self.logger.error(f"Error creating metadata page: {e}")
            # Return empty page if metadata creation fails - use custom size fallback
            packet = io.BytesIO()
            c = canvas.Canvas(packet, pagesize=(1000, 500))  # Custom size fallback
            c.showPage()
            c.save()
            packet.seek(0)
            return PdfReader(packet).pages[0]
    
    def create_metadata_page(self, metadata: Dict[str, Any]):
        """
        Public interface for creating metadata pages.
        Delegates to create_corrected_metadata_page for consistency.
        
        Args:
            metadata: Dictionary with metadata from extract_metadata
            
        Returns:
            PyPDF2 PageObject with metadata
        """
        return self.create_corrected_metadata_page(metadata)
