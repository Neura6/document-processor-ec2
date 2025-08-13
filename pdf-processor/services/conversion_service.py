"""
Document Conversion Service
Handles conversion of .doc, .docx, and .txt files to PDF format.
"""

import os
import io
import logging
from typing import Optional, Tuple
import subprocess
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import tempfile

logger = logging.getLogger(__name__)

class ConversionService:
    """Service for converting various document formats to PDF."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def convert_to_pdf(self, file_content: bytes, original_filename: str) -> Tuple[Optional[bytes], str]:
        """
        Convert document to PDF format.
        
        Args:
            file_content: Original file content as bytes
            original_filename: Original filename including extension
            
        Returns:
            Tuple of (pdf_content, converted_filename) or (None, original_filename) if conversion fails
        """
        try:
            extension = os.path.splitext(original_filename)[1].lower()
            
            if extension in ['.doc', '.docx']:
                return self._convert_word_to_pdf(file_content, original_filename)
            elif extension == '.txt':
                return self._convert_txt_to_pdf(file_content, original_filename)
            else:
                # Already PDF, return as-is
                return file_content, original_filename
                
        except Exception as e:
            self.logger.error(f"Error converting {original_filename} to PDF: {e}")
            return None, original_filename
    
    def _convert_word_to_pdf(self, file_content: bytes, original_filename: str) -> Tuple[Optional[bytes], str]:
        """Convert Word documents to PDF using pypandoc or libreoffice."""
        try:
            # Try using libreoffice for conversion
            with tempfile.NamedTemporaryFile(suffix='.docx' if original_filename.endswith('.docx') else '.doc', 
                                           delete=False) as temp_input:
                temp_input.write(file_content)
                temp_input.flush()
                
                # Create output filename
                pdf_filename = os.path.splitext(original_filename)[0] + '.pdf'
                
                # Use libreoffice to convert
                try:
                    subprocess.run([
                        'libreoffice', '--headless', '--convert-to', 'pdf',
                        '--outdir', tempfile.gettempdir(), temp_input.name
                    ], check=True, capture_output=True, text=True)
                    
                    # Read the converted PDF
                    converted_path = os.path.splitext(temp_input.name)[0] + '.pdf'
                    if os.path.exists(converted_path):
                        with open(converted_path, 'rb') as f:
                            pdf_content = f.read()
                        
                        # Cleanup
                        os.unlink(temp_input.name)
                        os.unlink(converted_path)
                        
                        return pdf_content, pdf_filename
                        
                except (subprocess.CalledProcessError, FileNotFoundError) as e:
                    self.logger.warning(f"LibreOffice conversion failed: {e}")
                    
                # Fallback to simple text extraction if libreoffice not available
                return self._convert_word_fallback(file_content, original_filename)
                
        except Exception as e:
            self.logger.error(f"Error converting Word document: {e}")
            return None, original_filename
    
    def _convert_word_fallback(self, file_content: bytes, original_filename: str) -> Tuple[Optional[bytes], str]:
        """Fallback conversion for Word documents using python-docx."""
        try:
            from docx import Document
            
            with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as temp_input:
                temp_input.write(file_content)
                temp_input.flush()
                
                # Extract text from document
                doc = Document(temp_input.name)
                text_content = "\n".join([paragraph.text for paragraph in doc.paragraphs])
                
                # Convert to PDF
                pdf_content = self._convert_text_to_pdf(text_content)
                pdf_filename = os.path.splitext(original_filename)[0] + '.pdf'
                
                os.unlink(temp_input.name)
                return pdf_content, pdf_filename
                
        except ImportError:
            self.logger.error("python-docx not available for fallback conversion")
            return None, original_filename
        except Exception as e:
            self.logger.error(f"Error in Word fallback conversion: {e}")
            return None, original_filename
    
    def _convert_txt_to_pdf(self, file_content: bytes, original_filename: str) -> Tuple[Optional[bytes], str]:
        """Convert text files to PDF."""
        try:
            # Decode text content
            try:
                text_content = file_content.decode('utf-8')
            except UnicodeDecodeError:
                text_content = file_content.decode('latin-1')
            
            # Convert to PDF
            pdf_content = self._convert_text_to_pdf(text_content)
            pdf_filename = os.path.splitext(original_filename)[0] + '.pdf'
            
            return pdf_content, pdf_filename
            
        except Exception as e:
            self.logger.error(f"Error converting TXT file: {e}")
            return None, original_filename
    
    def _convert_text_to_pdf(self, text_content: str) -> bytes:
        """Convert plain text to PDF format."""
        try:
            packet = io.BytesIO()
            c = canvas.Canvas(packet, pagesize=letter)
            
            # Set font and size
            c.setFont("Helvetica", 10)
            
            # Split text into lines
            lines = text_content.split('\n')
            
            # Position for text
            x, y = 100, 750
            line_height = 12
            
            for line in lines:
                if y < 50:  # New page needed
                    c.showPage()
                    c.setFont("Helvetica", 10)
                    y = 750
                
                # Handle long lines by wrapping
                words = line.split()
                current_line = ""
                
                for word in words:
                    test_line = current_line + " " + word if current_line else word
                    if len(test_line) > 80:  # Approximate width
                        if current_line:
                            c.drawString(x, y, current_line)
                            y -= line_height
                            current_line = word
                        else:
                            c.drawString(x, y, word)
                            y -= line_height
                    else:
                        current_line = test_line
                
                if current_line:
                    c.drawString(x, y, current_line)
                    y -= line_height
            
            c.showPage()
            c.save()
            
            packet.seek(0)
            return packet.getvalue()
            
        except Exception as e:
            self.logger.error(f"Error converting text to PDF: {e}")
            raise
    
    def is_convertible_format(self, filename: str) -> bool:
        """Check if file format can be converted to PDF."""
        extension = os.path.splitext(filename)[1].lower()
        return extension in ['.doc', '.docx', '.txt']
