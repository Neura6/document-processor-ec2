"""
PDF-plumber Processing Service
Handles enhanced structure extraction for non-scanned PDF pages using PDF-plumber.
Based on POC implementation for table and form detection.
"""

import io
import json
import logging
import pdfplumber
from typing import List, Tuple, Dict, Any
from PyPDF2 import PdfWriter, PdfReader
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import tempfile
import os

logger = logging.getLogger(__name__)

class PDFPlumberService:
    """Service for enhanced PDF processing using PDF-plumber for tables and structured content."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def detect_tables_or_forms(self, page) -> bool:
        """
        Detect if a page has tables or form-like structures.
        Based on POC logic from pdf_plumber_execution.py
        """
        try:
            # Method 1: Direct table extraction
            tables = page.extract_tables()
            if tables and len(tables) > 0:
                return True
            
            # Method 2: Pattern-based detection for table-like structures
            text = page.extract_text() or ""
            lines = text.split('\n')
            
            for line in lines:
                # Check for multiple columns (≥3 columns)
                if len(line.split()) >= 3:
                    # Check if line contains multiple numbers
                    numbers = [word for word in line.split() if any(c.isdigit() for c in word)]
                    if len(numbers) >= 2:  # At least 2 numeric values
                        return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error detecting tables/forms: {e}")
            return False
    
    def page_to_enhanced_data(self, page, page_num: int) -> Dict[str, Any]:
        """
        Convert a PDF page into enhanced structured data.
        Extracts text, tables, and image metadata.
        """
        try:
            page_data = {
                "page_number": page_num,
                "text": page.extract_text() or "",
                "tables": page.extract_tables() or [],
                "images": [],
                "has_tables": False,
                "processing_method": "pdf_plumber"
            }
            
            # Extract image metadata
            for img in page.images:
                page_data["images"].append({
                    "name": img.get("name"),
                    "width": img.get("width"),
                    "height": img.get("height"),
                    "x0": img.get("x0"),
                    "y0": img.get("y0"),
                })
            
            # Mark if tables were found
            page_data["has_tables"] = len(page_data["tables"]) > 0
            
            return page_data
            
        except Exception as e:
            self.logger.error(f"Error extracting page data: {e}")
            return {
                "page_number": page_num,
                "text": "",
                "tables": [],
                "images": [],
                "has_tables": False,
                "processing_method": "pdf_plumber_error"
            }
    
    def enhanced_data_to_pdf_page(self, page_data: Dict[str, Any]) -> io.BytesIO:
        """
        Convert enhanced structured data back into a PDF page.
        Handles text + tables with proper formatting and overflow.
        """
        try:
            # Create temporary file for PDF generation
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                temp_path = temp_file.name
            
            c = canvas.Canvas(temp_path, pagesize=letter)
            width, height = letter
            
            margin = 50
            line_height = 15
            y = height - margin
            
            def new_page():
                nonlocal y
                c.showPage()
                y = height - margin
            
            # Text content (removed title)
            c.setFont("Helvetica", 10)
            text_content = page_data.get("text", "")
            if text_content:
                for line in text_content.splitlines():
                    if line.strip():  # Skip empty lines
                        c.drawString(margin, y, line.strip())
                        y -= line_height
                        if y < margin:  # Page overflow
                            new_page()
            
            # Tables section
            tables = page_data.get("tables", [])
            if tables:
                y -= line_height  # Extra space before tables
                c.setFont("Helvetica-Bold", 11)
                c.drawString(margin, y, "[Extracted Tables]")
                y -= 2 * line_height
                
                c.setFont("Helvetica", 9)
                for table_idx, table in enumerate(tables):
                    # Table header
                    c.setFont("Helvetica-Bold", 9)
                    c.drawString(margin, y, f"Table {table_idx + 1}:")
                    y -= line_height
                    
                    c.setFont("Helvetica", 9)
                    for row in table:
                        if row:  # Skip empty rows
                            row_str = " | ".join(str(cell) if cell else "" for cell in row)
                            # Truncate long rows to fit page width
                            if len(row_str) > 80:
                                row_str = row_str[:77] + "..."
                            c.drawString(margin, y, row_str)
                            y -= line_height
                            if y < margin:  # Page overflow
                                new_page()
                    y -= line_height  # Extra gap between tables
            
            # Images metadata section
            images = page_data.get("images", [])
            if images:
                y -= line_height
                c.setFont("Helvetica-Bold", 11)
                c.drawString(margin, y, "[Image Metadata]")
                y -= 2 * line_height
                
                c.setFont("Helvetica", 9)
                for img_idx, img in enumerate(images):
                    img_info = f"Image {img_idx + 1}: {img.get('width', 'N/A')}x{img.get('height', 'N/A')} at ({img.get('x0', 'N/A')}, {img.get('y0', 'N/A')})"
                    c.drawString(margin, y, img_info)
                    y -= line_height
                    if y < margin:
                        new_page()
            
            c.save()
            
            # Read the generated PDF into BytesIO
            with open(temp_path, 'rb') as f:
                pdf_bytes = f.read()
            
            # Clean up temporary file
            os.unlink(temp_path)
            
            return io.BytesIO(pdf_bytes)
            
        except Exception as e:
            self.logger.error(f"Error creating enhanced PDF page: {e}")
            # Return empty PDF page on error
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                temp_path = temp_file.name
            
            c = canvas.Canvas(temp_path, pagesize=letter)
            c.setFont("Helvetica", 12)
            c.drawString(50, 750, f"Error processing page {page_data.get('page_number', '?')}")
            c.save()
            
            with open(temp_path, 'rb') as f:
                pdf_bytes = f.read()
            os.unlink(temp_path)
            
            return io.BytesIO(pdf_bytes)
    
    def apply_pdf_plumber_to_pdf(self, pdf_stream: io.BytesIO, file_key: str) -> Tuple[io.BytesIO, List[int]]:
        """
        Apply PDF-plumber processing to non-scanned PDF pages.
        
        Args:
            pdf_stream: PDF file as BytesIO
            file_key: File identifier for logging
            
        Returns:
            Tuple of (enhanced_pdf_stream, processed_page_numbers)
        """
        if pdf_stream is None:
            logger.error("Received None stream for PDF-plumber processing")
            return None, []
        
        try:
            pdf_stream.seek(0)
            pdf_bytes = pdf_stream.read()
            
            processed_pages = []
            enhanced_pages = []
            
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                num_pages = len(pdf.pages)
                self.logger.info(f"PDF-plumber processing {num_pages} pages for {file_key}")
                
                for i, page in enumerate(pdf.pages):
                    try:
                        # Extract enhanced data for this page
                        page_data = self.page_to_enhanced_data(page, i + 1)
                        
                        # Convert back to PDF page
                        enhanced_page_stream = self.enhanced_data_to_pdf_page(page_data)
                        enhanced_pages.append(enhanced_page_stream)
                        processed_pages.append(i)
                        
                        # Log processing details
                        has_tables = page_data.get("has_tables", False)
                        num_images = len(page_data.get("images", []))
                        self.logger.debug(f"Page {i+1}: Tables={has_tables}, Images={num_images}")
                        
                    except Exception as e:
                        self.logger.error(f"Error processing page {i+1}: {e}")
                        # Fall back to original page
                        original_reader = PdfReader(io.BytesIO(pdf_bytes))
                        original_page_stream = io.BytesIO()
                        writer = PdfWriter()
                        writer.add_page(original_reader.pages[i])
                        writer.write(original_page_stream)
                        enhanced_pages.append(original_page_stream)
            
            # Combine all enhanced pages into single PDF
            if enhanced_pages:
                final_writer = PdfWriter()
                
                for page_stream in enhanced_pages:
                    page_stream.seek(0)
                    reader = PdfReader(page_stream)
                    final_writer.add_page(reader.pages[0])
                
                final_stream = io.BytesIO()
                final_writer.write(final_stream)
                final_stream.seek(0)
                
                self.logger.info(f"PDF-plumber processing completed: {len(processed_pages)} pages enhanced")
                return final_stream, processed_pages
            else:
                self.logger.warning("No pages were successfully processed")
                return None, []
                
        except Exception as e:
            self.logger.error(f"Error in PDF-plumber processing for {file_key}: {e}")
            return None, []
