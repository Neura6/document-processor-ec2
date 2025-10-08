"""
OCR Processing Service
Handles OCR processing for scanned PDF pages using Tesseract.
"""

import fitz
import pytesseract
from PIL import Image
import io
import logging
import asyncio
from typing import List, Tuple
from concurrent.futures import ProcessPoolExecutor
import os

logger = logging.getLogger(__name__)

DEFAULT_DPI_OCR = 300
OCR_TEXT_THRESHOLD = 50
MAX_WORKERS_OCR_PAGE = os.cpu_count() if os.cpu_count() else 4

class OCRService:
    """Service for performing OCR on scanned PDF pages."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def perform_ocr_on_page(self, pdf_bytes: bytes, page_num: int, dpi: int = DEFAULT_DPI_OCR) -> Tuple[int, str]:
        """
        Perform OCR on a single page.
        
        Args:
            pdf_bytes: PDF file as bytes
            page_num: Page number (0-indexed)
            dpi: DPI for rendering
            
        Returns:
            Tuple of (page_number, extracted_text)
        """
        try:
            temp_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            page = temp_doc.load_page(page_num)
            pix = page.get_pixmap(dpi=dpi)
            
            img_mode = "RGB" if pix.n == 3 else "RGBA" if pix.n == 4 else pix.mode
            img = Image.frombytes(img_mode, [pix.width, pix.height], pix.samples)
            
            text = pytesseract.image_to_string(img, lang='eng', config='--psm 6')
            temp_doc.close()
            
            self.logger.debug(f"Page {page_num+1}: OCR completed, {len(text)} characters")
            return (page_num, text)
            
        except Exception as e:
            self.logger.error(f"Page {page_num+1}: OCR failed: {e}")
            return (page_num, f"Error on page {page_num+1}: {str(e)}")
    
    def apply_ocr_to_pdf(self, pdf_stream: io.BytesIO, file_key: str) -> Tuple[io.BytesIO, List[int]]:
        """
        Apply OCR to scanned pages in PDF.
        
        Args:
            pdf_stream: PDF file as BytesIO
            file_key: File identifier for logging
            
        Returns:
            Tuple of (modified_pdf_stream, replaced_page_numbers)
        """
        if pdf_stream is None:
            logger.error("Received None stream for OCR processing")
            return None, []
        
        try:
            pdf_stream.seek(0)
            pdf_bytes = pdf_stream.read()
            ocr_analysis_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            
            num_pages = len(ocr_analysis_doc)
            pages_to_ocr = []
            
            # Identify pages needing OCR based on your exact requirements:
            # Apply OCR for: (a) text+images, (b) only images, (c) no text
            # Skip OCR only for: pure text pages with no embedded images
            for i, page in enumerate(ocr_analysis_doc):
                original_text = page.get_text("text", flags=fitz.TEXT_PRESERVE_WHITESPACE).strip()
                has_images = len(page.get_images()) > 0
                has_text = bool(original_text) and len(original_text.strip()) > 0
                
                # Apply OCR for these cases:
                # 1. Text + Images (hybrid content)
                # 2. Only Images (no text)
                # 3. No text content at all
                # Skip OCR only for: pure text pages with no embedded images
                
                if has_images or not has_text:
                    pages_to_ocr.append(i)
            
            self.logger.info(f"Identified {len(pages_to_ocr)} pages for OCR")
            ocr_analysis_doc.close()
            
            if not pages_to_ocr:
                return None, []
            
            # Perform OCR in parallel
            ocr_results = {}
            with ProcessPoolExecutor(max_workers=MAX_WORKERS_OCR_PAGE) as executor:
                future_to_page = {
                    executor.submit(self.perform_ocr_on_page, pdf_bytes, i): i
                    for i in pages_to_ocr
                }
                
                for future in future_to_page:
                    page_num, text = future.result()
                    ocr_results[page_num] = text
            
            # Rebuild PDF with OCR results
            new_pdf_doc = fitz.open()
            original_doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            replaced_pages = []
            
            for i in range(num_pages):
                original_page = original_doc.load_page(i)
                
                if i in ocr_results:
                    ocr_text = ocr_results[i]
                    
                    if "Error" in ocr_text:
                        # Create error page
                        new_page = new_pdf_doc.new_page(
                            width=original_page.rect.width,
                            height=original_page.rect.height
                        )
                        new_page.insert_text((50, 50), f"OCR Failed: {ocr_text}", 
                                           fontsize=8, fontname="Courier")
                    else:
                        # Create OCR text page
                        new_page = new_pdf_doc.new_page(
                            width=original_page.rect.width,
                            height=original_page.rect.height
                        )
                        margin = 50
                        box = fitz.Rect(margin, margin, 
                                      original_page.rect.width - margin,
                                      original_page.rect.height - margin)
                        
                        inserted = 0
                        font_size = 9
                        while font_size >= 5:
                            inserted = new_page.insert_textbox(
                                box, ocr_text.strip(),
                                fontsize=font_size,
                                fontname="Times-Roman",
                                align=fitz.TEXT_ALIGN_LEFT
                            )
                            if inserted > 0:
                                break
                            font_size -= 1
                    
                    replaced_pages.append(i + 1)
                else:
                    # Keep original page
                    new_pdf_doc.insert_pdf(original_doc, from_page=i, to_page=i)
            
            original_doc.close()
            
            if replaced_pages:
                final_stream = io.BytesIO()
                new_pdf_doc.save(final_stream)
                final_stream.seek(0)
                new_pdf_doc.close()
                return final_stream, replaced_pages
            else:
                new_pdf_doc.close()
                return None, []
                
        except Exception as e:
            self.logger.error(f"Error during OCR processing: {e}")
            return None, []
    
    async def apply_ocr_to_pdf_async(self, pdf_data: bytes, file_key: str) -> Tuple[io.BytesIO, List[int]]:
        """
        Async version of OCR processing with improved parallel execution.
        
        Args:
            pdf_data: PDF file as bytes
            file_key: File identifier for logging
            
        Returns:
            Tuple of (modified_pdf_stream, replaced_page_numbers)
        """
        if pdf_data is None:
            logger.error("Received None data for async OCR processing")
            return None, []
        
        try:
            # Convert to BytesIO for compatibility
            pdf_stream = io.BytesIO(pdf_data)
            
            # Run the existing OCR logic in executor with better error propagation
            loop = asyncio.get_event_loop()
            
            def _ocr_with_error_context():
                try:
                    return self.apply_ocr_to_pdf(pdf_stream, file_key)
                except Exception as e:
                    # Add context to the exception
                    raise Exception(f"OCR processing failed for {file_key}: {str(e)}") from e
            
            result = await loop.run_in_executor(None, _ocr_with_error_context)
            
            if result and result[0]:
                self.logger.info(f"✅ Async OCR completed successfully for: {file_key}")
            else:
                self.logger.warning(f"⚠️ OCR returned no results for: {file_key}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Error during async OCR processing for {file_key}: {e}")
            # Re-raise with context preserved
            raise
