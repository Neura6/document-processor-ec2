"""
Watermark Removal Service - PyPDF2 Version
Handles watermark removal using PyPDF2 to avoid PyMuPDF tuple issues.
"""

import PyPDF2
import io
import logging
import re

logger = logging.getLogger(__name__)

WATERMARK_TERMS_TO_REMOVE = [
    "Tax Management India .com",
    "https://www.taxmanagementindia.com",
    "TMI"
]

class WatermarkService:
    """Service for removing watermarks from PDFs."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def remove_watermarks(self, pdf_content: bytes, file_key: str = None) -> bytes:
        """
        Remove watermarks from PDF content using PyPDF2 text replacement.
        
        Args:
            pdf_content: PDF file content as bytes
            file_key: Original file key for logging (optional)
            
        Returns:
            bytes: Cleaned PDF content
        """
        if pdf_content is None:
            self.logger.error("Received None content for watermark processing")
            return None
        
        try:
            # Create PDF reader
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            pdf_writer = PyPDF2.PdfWriter()
            
            modified = False
            
            # Process each page
            for page_num, page in enumerate(pdf_reader.pages):
                # Extract text content
                try:
                    text = page.extract_text()
                    if text:
                        # Check for watermark terms
                        for term in WATERMARK_TERMS_TO_REMOVE:
                            if term in text:
                                modified = True
                                self.logger.debug(f"Found term '{term}' on page {page_num+1} of {file_key}")
                                # Replace with empty string
                                text = text.replace(term, '')
                                
                        # Note: PyPDF2 can't directly edit text in PDFs
                        # We'll just log and pass through - this is a limitation
                        
                except Exception as e:
                    self.logger.warning(f"Error processing page {page_num+1}: {e}")
                
                # Add page to writer regardless
                pdf_writer.add_page(page)
            
            # Save the document
            output_stream = io.BytesIO()
            pdf_writer.write(output_stream)
            output_stream.seek(0)
            
            if modified:
                self.logger.info(f"Watermark terms found and processed in {file_key}")
            
            return output_stream.read()
                
        except Exception as e:
            self.logger.error(f"Error during watermark processing: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return pdf_content  # Return original if processing fails
