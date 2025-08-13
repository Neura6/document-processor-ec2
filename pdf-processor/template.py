#!/usr/bin/env python3
"""
Template file for manually setting up PDF processor microservices.
Run this file on EC2 to create all directory structure and empty placeholder files.
Then copy the actual code content from the provided files into these placeholders.
"""

import os
import sys

def create_directory_structure():
    """Create the directory structure for microservices."""
    
    # Root directories
    directories = [
        'pdf-processor',
        'pdf-processor/services',
        'pdf-processor/utils',
        'pdf-processor/logs'
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        else:
            print(f"Directory already exists: {directory}")

def create_placeholder_files():
    """Create placeholder files with TODO comments."""
    
    # Configuration file
    config_content = '''

# Configuration file for PDF processor microservices
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
SOURCE_BUCKET = os.getenv('SOURCE_BUCKET', 'rules-repository')
CHUNKED_BUCKET = os.getenv('CHUNKED_BUCKET', 'chunked-rules-repository')

# Processing Configuration
MAX_WORKERS_FILENAME_CLEANING = int(os.getenv('MAX_WORKERS_FILENAME_CLEANING', 10))
MAX_WORKERS_OCR_PAGE = int(os.getenv('MAX_WORKERS_OCR_PAGE', 4))
DEFAULT_DPI_OCR = int(os.getenv('DEFAULT_DPI_OCR', 300))
OCR_TEXT_THRESHOLD = int(os.getenv('OCR_TEXT_THRESHOLD', 50))

# Regex Patterns
REMOVE_TERM_REGEX = r"TMI\s*"
DOUBLE_SPACE_REGEX = r"[\s\u00A0\u2000-\u200B]{2,}"
QUOTE_CHARS_REGEX = r"[‘’”“'\"]+"

# Watermark Terms
WATERMARK_TERMS_TO_REMOVE = [
    "Tax Management India .com",
    "https://www.taxmanagementindia.com",
    "TMI"
]

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
CHECKPOINT_FILE_EXTENSION = "_checkpoint.txt"

'''
    
    # Main entry point
    main_content = '''#!/usr/bin/env python3
"""
Main entry point for PDF processor microservices.
TODO: Copy content from main.py file
"""

import argparse
import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.orchestrator import Orchestrator

def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(description='PDF Processor Microservices')
    parser.add_argument('--folder', type=str, help='Process all PDFs in a folder')
    parser.add_argument('--file', type=str, help='Process a single PDF file')
    parser.add_argument('--config', type=str, help='Configuration file path')
    
    args = parser.parse_args()
    
    if not args.folder and not args.file:
        print("Error: Please specify either --folder or --file")
        sys.exit(1)
    
    # Initialize orchestrator
    orchestrator = Orchestrator()
    
    try:
        if args.file:
            print(f"Processing single file: {args.file}")
            success = orchestrator.process_single_file(args.file)
            print(f"Processing {'successful' if success else 'failed'}")
            
        elif args.folder:
            print(f"Processing folder: {args.folder}")
            results = orchestrator.process_folder(args.folder)
            print(f"Processing completed:")
            print(f"  Total files: {results['total']}")
            print(f"  Successful: {results['success']}")
            print(f"  Failed: {results['failed']}")
            
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
'''
    
    services = {
                'filename_service.py': '''
"""
Filename Cleaning Service
Handles all filename cleaning operations including quote removal, space normalization,
TMI removal, and non-English character conversion.
"""

import re
import unidecode
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

# Regex patterns
REMOVE_TERM_REGEX = r"TMI\s*"
DOUBLE_SPACE_REGEX = r"[\s\u00A0\u2000-\u200B]{2,}"
QUOTE_CHARS_REGEX = r"[‘’”“'\"]+"

class FilenameService:
    """Service for cleaning and normalizing filenames."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def clean_filename(self, original_key: str) -> str:
        """
        Clean filename by removing quotes, normalizing spaces, removing TMI,
        and converting non-English characters.
        
        Args:
            original_key: Original S3 object key
            
        Returns:
            Cleaned key string
        """
        current_key = original_key
        modified = False
        
        # Step 1: Remove problematic quote characters
        cleaned_key_quotes = re.sub(QUOTE_CHARS_REGEX, "", current_key)
        if cleaned_key_quotes != current_key:
            self.logger.debug(f"Removed quotes: {current_key} -> {cleaned_key_quotes}")
            current_key = cleaned_key_quotes
            modified = True
        
        # Step 2: Replace double spaces with single spaces and strip
        cleaned_key_spaces = re.sub(DOUBLE_SPACE_REGEX, " ", current_key)
        cleaned_key_spaces = cleaned_key_spaces.strip()
        if cleaned_key_spaces != current_key:
            self.logger.debug(f"Cleaned spaces: {current_key} -> {cleaned_key_spaces}")
            current_key = cleaned_key_spaces
            modified = True
        
        # Step 3: Remove TMI using regex (case-insensitive)
        cleaned_key_tmi = re.sub(REMOVE_TERM_REGEX, "", current_key, flags=re.IGNORECASE)
        if cleaned_key_tmi != current_key:
            self.logger.debug(f"Removed TMI: {current_key} -> {cleaned_key_tmi}")
            current_key = cleaned_key_tmi
            modified = True
        
        # Step 4: Convert non-English characters using unidecode
        dirname, basename = original_key.rsplit('/', 1) if '/' in original_key else ('', original_key)
        try:
            cleaned_dirname = unidecode.unidecode(dirname)
            cleaned_basename = unidecode.unidecode(basename)
            cleaned_key_non_english = f"{cleaned_dirname}/{cleaned_basename}".replace("//", "/")
            
            if cleaned_key_non_english != current_key:
                self.logger.debug(f"Converted non-English: {current_key} -> {cleaned_key_non_english}")
                current_key = cleaned_key_non_english
                modified = True
        except Exception as e:
            self.logger.error(f"Error during unidecode conversion: {e}")
        
        return current_key if modified else original_key
    
    def needs_cleaning(self, original_key: str) -> bool:
        """Check if filename needs cleaning."""
        return self.clean_filename(original_key) != original_key

''',
        
        'watermark_service.py': '''
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

''',
        
        'ocr_service.py': '''
"""
OCR Processing Service
Handles OCR processing for scanned PDF pages using Tesseract.
"""

import fitz
import pytesseract
from PIL import Image
import io
import logging
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
            
            # Identify pages needing OCR
            for i, page in enumerate(ocr_analysis_doc):
                original_text = page.get_text("text", flags=fitz.TEXT_PRESERVE_WHITESPACE).strip()
                has_images = len(page.get_images()) > 0
                
                if (not original_text or len(original_text) < OCR_TEXT_THRESHOLD) and has_images:
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

''',
        
        'chunking_service.py': '''
"""
PDF Chunking Service
Handles PDF chunking into individual pages with metadata.
"""

import PyPDF2
from io import BytesIO
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class ChunkingService:
    """Service for chunking PDFs into individual pages."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def extract_metadata(self, key: str) -> Dict[str, Any]:
        """
        Extract metadata from S3 key based on folder structure.
        
        Args:
            key: S3 object key
            
        Returns:
            Dictionary with extracted metadata
        """
        parts = key.split('/')
        folder = parts[0]
        metadata = {'standard_type': folder}
        
        if folder in ['accounting-standards', 'Finance Tools', 'GIFT City']:
            if len(parts) > 1: metadata['Standard_type'] = parts[1]
            if len(parts) > 2: metadata['document_type'] = parts[2]
            if len(parts) > 3: metadata['Sub-document_type'] = parts[3]
            metadata['document_name'] = parts[-1].rsplit('.', 1)[0]
        
        elif folder in ['commercial-laws','Banking Regulations','Direct Taxes',
                       'Capital Market Regulations','Auditing Standards','Insurance',
                       'Labour Law', 'commercial-case-laws', 'Indirect-Taxes-case-laws', 
                       'Direct-Taxes-case-laws']:
            if len(parts) > 1: metadata['country'] = parts[1]
            if len(parts) > 2: metadata['document_type'] = parts[2]
            if len(parts) > 3: metadata['document_category'] = parts[3]
            metadata['document_name'] = parts[-1].rsplit('.', 1)[0]
        
        elif folder == 'Indirect Taxes':
            if len(parts) > 1: metadata['country'] = parts[1]
            if len(parts) > 2: metadata['document_type'] = parts[2]
            if len(parts) > 3: metadata['State'] = parts[3]
            if len(parts) > 4: metadata['State_category'] = parts[4]
            metadata['document_name'] = parts[-1].rsplit('.', 1)[0]
        
        elif folder == 'usecase-reports':
            if len(parts) > 1: metadata['country'] = parts[1]
            if len(parts) > 2: metadata['year'] = parts[2]
            metadata['document_name'] = parts[-1].rsplit('.', 1)[0]
        
        else:
            metadata['document_name'] = parts[-1].rsplit('.', 1)[0]
            if len(parts) > 1:
                metadata['folder_path'] = '/'.join(parts[:-1])
        
        return metadata
    
    def create_metadata_page(self, metadata: Dict[str, Any]) -> PyPDF2.PageObject:
        """
        Create a PDF page containing metadata information.
        
        Args:
            metadata: Dictionary with metadata
            
        Returns:
            PyPDF2 PageObject with metadata
        """
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
        
        packet = BytesIO()
        c = canvas.Canvas(packet, pagesize=letter)
        c.setFont("Helvetica", 8)
        y = 750
        
        sorted_keys = sorted(metadata.keys())
        for key in sorted_keys:
            value = str(metadata.get(key, "N/A"))
            c.drawString(100, y, f"{key}: {value}")
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
        Split PDF into individual pages with metadata.
        
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
            
            metadata = self.extract_metadata(s3_key)
            chunks = []
            
            for page_num in range(len(reader.pages)):
                writer = PyPDF2.PdfWriter()
                
                # Add metadata page
                metadata_page = self.create_metadata_page(metadata)
                if metadata_page:
                    writer.add_page(metadata_page)
                
                # Add actual page
                writer.add_page(reader.pages[page_num])
                
                # Update metadata for this chunk
                chunk_metadata = metadata.copy()
                chunk_metadata['page_number'] = page_num + 1
                chunk_metadata['total_pages'] = len(reader.pages)
                
                chunks.append((writer, chunk_metadata))
            
            return chunks
            
        except Exception as e:
            self.logger.error(f"Error chunking PDF: {e}")
            return []

''',
        
        's3_service.py': '''
    
"""
S3 Service
Handles all AWS S3 operations including upload, download, copy, and delete.
"""

import boto3
import logging
from typing import List, Dict, Any
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

logger = logging.getLogger(__name__)

class S3Service:
    """Service for handling S3 operations."""
    
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str = 'us-east-1'):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.s3 = None
        self._setup_s3()
    
    def _setup_s3(self):
        """Setup S3 client."""
        try:
            boto3.setup_default_session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
            self.s3 = boto3.client('s3', region_name=self.region_name)
            self.s3.list_buckets()  # Test connection
            logger.info("S3 client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to setup S3 client: {e}")
            raise
    
    def list_files_in_folder(self, bucket: str, folder: str) -> List[str]:
        """
        List all PDF files in a given S3 folder.
        
        Args:
            bucket: S3 bucket name
            folder: Folder prefix
            
        Returns:
            List of file keys
        """
        try:
            files = []
            paginator = self.s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket, Prefix=folder):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.lower().endswith('.pdf'):
                        files.append(key)
            
            return files
            
        except Exception as e:
            logger.error(f"Error listing files in folder {folder}: {e}")
            raise
    
    def get_object(self, bucket: str, key: str) -> bytes:
        """
        Get object from S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            
        Returns:
            Object bytes
        """
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
            
        except Exception as e:
            logger.error(f"Error getting object {key}: {e}")
            raise
    
    def put_object(self, bucket: str, key: str, body: bytes) -> bool:
        """
        Put object to S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            body: Object bytes
            
        Returns:
            True if successful
        """
        try:
            self.s3.put_object(Bucket=bucket, Key=key, Body=body)
            logger.debug(f"Successfully saved to S3: {bucket}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to S3 {key}: {e}")
            return False
    
    def copy_object(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """
        Copy object within S3.
        
        Args:
            source_bucket: Source bucket
            source_key: Source key
            dest_bucket: Destination bucket
            dest_key: Destination key
            
        Returns:
            True if successful
        """
        try:
            self.s3.copy_object(
                Bucket=dest_bucket,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Key=dest_key
            )
            return True
            
        except Exception as e:
            logger.error(f"Error copying object: {e}")
            return False
    
    def delete_object(self, bucket: str, key: str) -> bool:
        """
        Delete object from S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            
        Returns:
            True if successful
        """
        try:
            self.s3.delete_object(Bucket=bucket, Key=key)
            return True
            
        except Exception as e:
            logger.error(f"Error deleting object: {e}")
            return False
    
    def object_exists(self, bucket: str, key: str) -> bool:
        """
        Check if object exists in S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            
        Returns:
            True if exists
        """
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
            
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
        except Exception as e:
            logger.error(f"Error checking object existence: {e}")
            return False

''',
        
        'orchestrator.py': '''
		
"""
Main Orchestrator Service
Coordinates all services to process PDF files end-to-end.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.filename_service import FilenameService
from services.watermark_service import WatermarkService
from services.ocr_service import OCRService
from services.chunking_service import ChunkingService
from services.s3_service import S3Service
from utils.logger import LoggerService
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SOURCE_BUCKET, CHUNKED_BUCKET
import io
import logging

class Orchestrator:
    """Main orchestrator for PDF processing workflow."""
    
    def __init__(self):
        self.logger_service = LoggerService()
        self.logger = self.logger_service.get_logger()
        
        # Initialize services
        self.s3_service = S3Service(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        self.filename_service = FilenameService()
        self.watermark_service = WatermarkService()
        self.ocr_service = OCRService()
        self.chunking_service = ChunkingService()
        
        self.logger.info("Orchestrator initialized successfully")
    
    def process_single_file(self, file_key: str) -> bool:
        """
        Process a single PDF file end-to-end.
        
        Args:
            file_key: S3 key of the file to process
            
        Returns:
            True if processing successful
        """
        try:
            self.logger.info(f"Starting processing for: {file_key}")
            
            # Step 1: Download file from S3
            self.logger.info("Step 1: Downloading file from S3")
            pdf_bytes = self.s3_service.get_object(SOURCE_BUCKET, file_key)
            pdf_stream = io.BytesIO(pdf_bytes)
            
            # Step 2: Clean filename if needed
            self.logger.info("Step 2: Cleaning filename")
            cleaned_key = self.filename_service.clean_filename(file_key)
            if cleaned_key != file_key:
                # Handle filename cleaning (copy new, delete old)
                if not self.s3_service.object_exists(SOURCE_BUCKET, cleaned_key):
                    self.s3_service.copy_object(SOURCE_BUCKET, file_key, SOURCE_BUCKET, cleaned_key)
                    self.s3_service.delete_object(SOURCE_BUCKET, file_key)
                    file_key = cleaned_key
                    self.logger.info(f"File renamed to: {file_key}")
                else:
                    self.logger.warning(f"Target key {cleaned_key} already exists, skipping rename")
            
            # Step 3: Remove watermarks
            self.logger.info("Step 3: Removing watermarks")
            watermark_result = self.watermark_service.remove_watermarks(pdf_stream, file_key)
            if watermark_result[0]:
                pdf_stream = watermark_result[0]
                if watermark_result[1]:
                    self.logger.info(f"Removed pages: {watermark_result[1]}")
                self.logger.info("Watermark processing completed")
            else:
                self.logger.info("No watermarks found, continuing")
            
            # Step 4: Apply OCR if needed
            self.logger.info("Step 4: Applying OCR")
            ocr_result = self.ocr_service.apply_ocr_to_pdf(pdf_stream, file_key)
            if ocr_result[0]:
                pdf_stream = ocr_result[0]
                if ocr_result[1]:
                    self.logger.info(f"OCR applied to pages: {ocr_result[1]}")
                self.logger.info("OCR processing completed")
            else:
                self.logger.info("No OCR needed, continuing")
            
            # Step 5: Chunk PDF
            self.logger.info("Step 5: Chunking PDF")
            chunks = self.chunking_service.chunk_pdf(pdf_stream, file_key)
            
            # Step 6: Upload chunks to S3
            self.logger.info("Step 6: Uploading chunks to S3")
            success_count = 0
            for writer, metadata in chunks:
                output = io.BytesIO()
                writer.write(output)
                output.seek(0)
                
                chunk_key = f"{file_key.rsplit('.', 1)[0]}_page_{metadata['page_number']}.pdf"
                if self.s3_service.put_object(CHUNKED_BUCKET, chunk_key, output.getvalue()):
                    success_count += 1
                    self.logger.debug(f"Uploaded chunk: {chunk_key}")
            
            self.logger.info(f"Processing completed successfully. Uploaded {success_count} chunks")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing {file_key}: {str(e)}")
            self.logger_service.log_error(file_key, str(e), "orchestrator", "processing")
            return False
    
    def process_folder(self, folder: str) -> Dict[str, int]:
        """
        Process all PDFs in a folder.
        
        Args:
            folder: Folder prefix to process
            
        Returns:
            Dictionary with processing results
        """
        try:
            files = self.s3_service.list_files_in_folder(SOURCE_BUCKET, folder)
            self.logger.info(f"Found {len(files)} PDFs to process")
            
            results = {'total': len(files), 'success': 0, 'failed': 0}
            
            for file_key in files:
                if self.process_single_file(file_key):
                    results['success'] += 1
                else:
                    results['failed'] += 1
            
            self.logger.info(f"Processing completed: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error processing folder {folder}: {str(e)}")
            return {'total': 0, 'success': 0, 'failed': 0}

''',
        
        'logger.py': '''
		
"""
Logging and Error Handling Service
Centralized logging with file and console handlers.
"""

import logging
import os
import time
from datetime import datetime
import csv

class LoggerService:
    """Service for centralized logging and error handling."""
    
    def __init__(self, log_dir: str = 'logs'):
        self.log_dir = log_dir
        self.setup_logging()
        self.setup_error_log()
    
    def setup_logging(self):
        """Configure logging with both file and console handlers."""
        # Create logs directory if it doesn't exist
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Create logger
        self.logger = logging.getLogger('PDFProcessor')
        self.logger.setLevel(logging.DEBUG)
        
        # File handler
        file_handler = logging.FileHandler(
            f'{self.log_dir}/pdf_processor_{time.strftime("%Y%m%d_%H%M%S")}.log'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def setup_error_log(self):
        """Setup CSV error logging."""
        self.error_log_file = f'{self.log_dir}/errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.error_fields = ['timestamp', 'file', 'error', 'service', 'stage']
        
        # Create CSV file with headers
        with open(self.error_log_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.error_fields)
            writer.writeheader()
    
    def log_error(self, file: str, error: str, service: str = 'unknown', stage: str = 'unknown'):
        """
        Log error to CSV file.
        
        Args:
            file: File being processed
            error: Error message
            service: Service where error occurred
            stage: Processing stage
        """
        with open(self.error_log_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.error_fields)
            writer.writerow({
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'file': file,
                'error': str(error),
                'service': service,
                'stage': stage
            })
    
    def get_logger(self, name: str = None) -> logging.Logger:
        """Get logger instance."""
        if name:
            return logging.getLogger(name)
        return self.logger

'''

}
    
    requirements_content = '''
boto3==1.28.85
PyPDF2==3.0.1
reportlab==4.0.7
PyMuPDF==1.23.8
pytesseract==0.3.10
Pillow==10.0.1
python-dotenv==1.0.0
unidecode==1.3.7

'''
   
    # Create all files
    files_to_create = {
        'config.py': config_content,
        'main.py': main_content,
        'requirements.txt': requirements_content,
        'services/filename_service.py': services['filename_service.py'],
        'services/watermark_service.py': services['watermark_service.py'],
        'services/ocr_service.py': services['ocr_service.py'],
        'services/chunking_service.py': services['chunking_service.py'],
        'services/s3_service.py': services['s3_service.py'],
        'services/orchestrator.py': services['orchestrator.py'],
        'utils/logger.py': services['logger.py']
    }
    
    return files_to_create

def main():
    """Main function to create all directories and files."""
    print("Creating PDF Processor Microservices Structure...")
    
    # Create directory structure
    create_directory_structure()
    
    # Get all files to create
    files_to_create = create_placeholder_files()
    
    # Create all files with placeholders
    for filepath, content in files_to_create.items():
        full_path = os.path.join('pdf-processor', filepath)
        
        # Ensure directory exists
        directory = os.path.dirname(full_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        
        # Create file with placeholder content
        with open(full_path, 'w') as f:
            f.write(content)
        print(f"Created: {full_path}")
    
    print("\n Directory structure and placeholder files created successfully!")
    print("\n Next steps:")
    print("1. Copy the actual code content from the provided files into these placeholders")
    print("2. Update AWS credentials in config.py")
    print("3. Install dependencies: pip install -r requirements.txt")
    print("4. Run: python main.py --folder folder_name  OR  python main.py --file file_name")

if __name__ == "__main__":
    main()
