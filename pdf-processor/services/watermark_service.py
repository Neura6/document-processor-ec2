"""
Watermark Removal Service
Handles watermark removal from PDFs using PyMuPDF.
"""

import fitz
import io
import logging
from typing import Tuple, List

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
    
    def is_page_empty(self, page: fitz.Page) -> bool:
        """Check if a page is completely empty."""
        return not page.get_text("text").strip() and not page.get_images() and not page.get_links()
    
    def remove_watermarks(self, pdf_stream: io.BytesIO, file_key: str) -> Tuple[io.BytesIO, List[int]]:
        """
        Remove watermarks and empty pages from PDF.
        
        Args:
            pdf_stream: PDF file as BytesIO
            file_key: File identifier for logging
            
        Returns:
            Tuple of (modified_pdf_stream, removed_page_numbers)
        """
        if pdf_stream is None:
            logger.error("Received None stream for watermark processing")
            return None, []
        
        try:
            pdf_stream.seek(0)
            doc = fitz.open("pdf", pdf_stream.read())
            modified = False
            pages_with_terms_indices = set()
            
            # Process each page
            for i, page in enumerate(doc):
                page_modified = False
                
                # Remove specified terms (case-sensitive)
                for term in WATERMARK_TERMS_TO_REMOVE:
                    text_instances = page.search_for(term)
                    if text_instances:
                        pages_with_terms_indices.add(i)
                        page_modified = True
                        modified = True
                        self.logger.debug(f"Found term '{term}' on page {i+1} of {file_key}")
                        for rect in text_instances:
                            page.add_redact_annot(rect, fill=(1, 1, 1))
                
                # Apply redactions
                if page_modified:
                    page.apply_redactions()
                
                # Remove hyperlinks containing terms (case-insensitive)
                try:
                    links = page.get_links()
                    annots_to_delete = []
                    for link in links:
                        if "uri" in link and any(term.lower() in link["uri"].lower() 
                                               for term in WATERMARK_TERMS_TO_REMOVE):
                            modified = True
                            if "xref" in link:
                                annots_to_delete.append(link["xref"])
                    
                    # Delete marked annotations
                    for annot in page.annots():
                        if annot.xref in annots_to_delete:
                            page.delete_annot(annot)
                
                except Exception as e:
                    self.logger.warning(f"Error processing links on page {i+1}: {e}")
            
            # Identify empty pages to remove
            if modified:
                temp_stream = io.BytesIO()
                doc.save(temp_stream, garbage=4, deflate=True)
                temp_stream.seek(0)
                temp_doc = fitz.open("pdf", temp_stream.read())
                
                # Find pages to remove
                indices_to_delete = [
                    i for i, page in enumerate(temp_doc)
                    if self.is_page_empty(page) and i not in pages_with_terms_indices
                ]
                
                temp_doc.close()
                
                # Remove identified pages
                removed_pages = []
                indices_to_delete.sort(reverse=True)
                for index in indices_to_delete:
                    if 0 <= index < len(doc):
                        doc.delete_page(index)
                        removed_pages.append(index + 1)
                
                # Save final document
                final_stream = io.BytesIO()
                doc.save(final_stream, garbage=4, deflate=True)
                final_stream.seek(0)
                doc.close()
                
                return final_stream, removed_pages
            
            else:
                doc.close()
                return None, []
                
        except Exception as e:
            self.logger.error(f"Error during watermark processing: {e}")
            if 'doc' in locals():
                doc.close()
            return None, []
