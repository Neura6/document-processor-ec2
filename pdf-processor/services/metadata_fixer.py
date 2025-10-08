#!/usr/bin/env python3
"""
Metadata Fixer Script for Truncated chunk_s3_uri Values
Fixes PDF files where chunk_s3_uri ends with "..." by replacing the first page with corrected metadata.
"""

import boto3
import csv
import io
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.utils import simpleSplit
from threading import Lock
import urllib3
from typing import Dict, List, Tuple, Any
from io import BytesIO
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, CHUNKED_BUCKET

# Suppress urllib3 warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# Thread-safe counters
processed_count = 0
fixed_count = 0
skipped_count = 0
error_count = 0
processed_lock = Lock()

# Use existing logger
logger = logging.getLogger(__name__)

class MetadataFixer:
    """Class to handle PDF metadata correction operations."""
    
    def __init__(self, s3_service=None, bucket_name=None):
        self.results = []
        self.results_lock = Lock()
        
        # Use provided S3 service or create default
        if s3_service:
            self.s3_service = s3_service
            self.bucket_name = bucket_name or CHUNKED_BUCKET
        else:
            # Fallback to standalone mode
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION,
            )
            self.bucket_name = CHUNKED_BUCKET
    
    def extract_metadata_from_first_page(self, s3_key: str) -> Dict[str, Any]:
        """Extract metadata from the first page of a PDF file."""
        try:
            logger.info(f"🔍 DEBUG: hasattr s3_service: {hasattr(self, 's3_service')}")
            if hasattr(self, 's3_service'):
                logger.info(f"🔍 DEBUG: s3_service type: {type(self.s3_service)}")
                logger.info(f"🔍 DEBUG: s3_service dir: {[attr for attr in dir(self.s3_service) if not attr.startswith('_')]}")
                try:
                    # Use orchestrator's S3 service
                    logger.info(f"🔍 DEBUG: Attempting s3_service.s3.get_object...")
                    response = self.s3_service.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
                    logger.info(f"🔍 DEBUG: S3 get_object successful!")
                except Exception as e:
                    logger.error(f"🔍 DEBUG: S3 get_object failed: {e}")
                    raise
            else:
                logger.info(f"🔍 DEBUG: Using standalone s3_client")
                # Use standalone S3 client
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            pdf_content = response['Body'].read()
            
            with io.BytesIO(pdf_content) as pdf_stream:
                pdf_reader = PdfReader(pdf_stream)
                
                if len(pdf_reader.pages) == 0:
                    return {}
                
                first_page = pdf_reader.pages[0]
                text = first_page.extract_text()
                
                # DEBUG: Log the extracted text to see what we're working with
                logger.info(f"=== EXTRACTED TEXT FROM FIRST PAGE ===")
                logger.info(f"Text length: {len(text)}")
                logger.info(f"Raw text:\n{text}")
                logger.info(f"=== END EXTRACTED TEXT ===")

                metadata = {}
                
                # Try multiple patterns to extract metadata
                # Split text into lines and process each line individually
                lines = text.split('\n')
                matches = []
                
                for i, line in enumerate(lines):
                    line = line.strip()
                    # Look for lines that end with ':' (field names)
                    if line.endswith(':') and i + 1 < len(lines):
                        field_name = line[:-1].strip()  # Remove the ':'
                        field_value = lines[i + 1].strip()  # Next line is the value
                        
                        # Skip table headers and empty values
                        if (field_name and field_value and 
                            field_name not in ['Field', 'Value', 'Document Metadata'] and
                            field_value not in ['Field', 'Value']):
                            matches.append((field_name, field_value))
                
                # DEBUG: Log what regex matches we found
                logger.info(f"=== REGEX MATCHES FOUND ===")
                logger.info(f"Total matches: {len(matches)}")
                for i, (key, value) in enumerate(matches):
                    logger.info(f"Match {i+1}: '{key}' -> '{value}'")
                logger.info(f"=== END REGEX MATCHES ===")
                
                for key, value in matches:
                    key = key.strip().replace(' ', '_').lower()
                    value = value.strip()
                    if key and value:
                        metadata[key] = value
                
                # Special handling for chunk_s3_uri with different possible names
                chunk_uri_patterns = [
                    r'chunk_s3_uri:\s*(s3://[^\s\n]+)',
                    r'Chunk\s+S3\s+Uri:\s*(s3://[^\s\n]+)',
                    r'chunk\s+s3\s+uri:\s*(s3://[^\s\n]+)',
                ]
                
                for pattern in chunk_uri_patterns:
                    uri_match = re.search(pattern, text, re.IGNORECASE)
                    if uri_match:
                        metadata['chunk_s3_uri'] = uri_match.group(1).strip()
                        break
                
                # DEBUG: Log final extracted metadata
                logger.info(f"=== FINAL EXTRACTED METADATA ===")
                logger.info(f"Total fields extracted: {len(metadata)}")
                for key, value in metadata.items():
                    logger.info(f"'{key}': '{value}'")
                logger.info(f"=== END FINAL METADATA ===")
                
                return metadata
                
        except Exception as e:
            logger.error(f"Error extracting metadata from {s3_key}: {e}")
            return {}
    
    def create_corrected_metadata_page(self, metadata: Dict[str, Any]) -> bytes:
        """Create a new metadata page with corrected chunk_s3_uri in table format (landscape)."""
        try:
            packet = BytesIO()
            # Custom page size: wider and shorter to fit S3 URIs on single line
            # Standard landscape letter is 792x612, we'll use 1000x500 (much wider, shorter)
            custom_page_size = (1000, 500)  # (width, height) in points
            c = canvas.Canvas(packet, pagesize=custom_page_size)
            
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
            
            # Define field display order and labels
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
                    
                    # Use simpleSplit for intelligent text wrapping
                    # Calculate available width (table_width - field name column width - padding)
                    max_width = table_width - (col2_x - col1_x) - 20  # ~800 pixels available
                    
                    # Split text intelligently using reportlab's simpleSplit
                    lines = simpleSplit(value_str, "Helvetica", 10, max_width)
                    
                    # Draw all lines with proper spacing
                    for line in lines:
                        c.drawString(col2_x, y, line)
                        y -= 14  # Move to next line
                    
                    y -= row_height
                    
                    # Add separator line between rows
                    c.setStrokeColorRGB(0.8, 0.8, 0.8)
                    c.line(col1_x, y + 10, col1_x + table_width, y + 10)
                    c.setStrokeColorRGB(0, 0, 0)  # Reset to black
                    
            # Draw table border
            c.rect(col1_x - 10, y, table_width + 20, y_start - y + 20)
            
            # Add timestamp in IST - positioned for custom wide page
            # c.setFont("Helvetica", 8)
            # ist = timezone(timedelta(hours=5, minutes=30))  # IST is UTC+5:30
            # current_time_ist = datetime.now(ist)
            # c.drawString(col1_x, y - 30, f"Generated: {current_time_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
            # c.drawString(col1_x + 500, y - 30, f"Format: Wide Metadata Page (1000x500) - Single Line URIs")
            
            c.showPage()
            c.save()
            packet.seek(0)
            
            # Convert to PDF page
            metadata_pdf = PdfReader(packet)
            return metadata_pdf.pages[0]
            
        except Exception as e:
            logger.error(f"Error creating metadata page: {e}")
            return None
    
    def generate_expected_uri(self, chunk_path: str) -> str:
        """Generate the expected S3 URI from chunk file path."""
        return f"s3://{self.bucket_name}/{chunk_path}"
    
    def fix_single_file(self, s3_key: str) -> Dict[str, Any]:
        """Fix a single PDF file's metadata if it has truncated chunk_s3_uri."""
        global processed_count, fixed_count, skipped_count, error_count
        
        result = {
            'file_path': s3_key,
            'status': 'processing',
            'original_uri': '',
            'corrected_uri': '',
            'action_taken': '',
            'error': '',
            'processing_time': 0
        }
        
        start_time = time.time()
        
        try:
            with processed_lock:
                processed_count += 1
                current_count = processed_count
            
            logger.info(f"Processing file {current_count}: {s3_key}")
            
            # Extract current metadata
            metadata = self.extract_metadata_from_first_page(s3_key)
            
            if not metadata:
                result.update({
                    'status': 'skipped',
                    'action_taken': 'No metadata found',
                    'error': 'Could not extract metadata'
                })
                with processed_lock:
                    skipped_count += 1
                return result
            
            current_chunk_uri = metadata.get('chunk_s3_uri', '')
            expected_uri = self.generate_expected_uri(s3_key)
            
            result['original_uri'] = current_chunk_uri
            result['corrected_uri'] = expected_uri
            
            # Check if URI needs fixing (ends with ... or doesn't match expected)
            needs_fixing = (
                current_chunk_uri.endswith('...') or 
                current_chunk_uri != expected_uri
            )
            
            if not needs_fixing:
                result.update({
                    'status': 'skipped',
                    'action_taken': 'URI already correct',
                })
                with processed_lock:
                    skipped_count += 1
                return result
            
            # Download original PDF
            if hasattr(self, 's3_service'):
                # Use orchestrator's S3 service
                response = self.s3_service.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
            else:
                # Use standalone S3 client
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            original_pdf_content = response['Body'].read()
            
            # Read original PDF
            original_reader = PdfReader(io.BytesIO(original_pdf_content))
            
            if len(original_reader.pages) < 2:
                result.update({
                    'status': 'error',
                    'action_taken': 'PDF has less than 2 pages',
                    'error': 'Cannot fix single-page PDF'
                })
                with processed_lock:
                    error_count += 1
                return result
            
            # Create new PDF with corrected metadata
            writer = PdfWriter()
            
            # Update metadata with correct URI
            corrected_metadata = metadata.copy()
            corrected_metadata['chunk_s3_uri'] = expected_uri
            
            # Create new metadata page
            new_metadata_page = self.create_corrected_metadata_page(corrected_metadata)
            
            if new_metadata_page is None:
                result.update({
                    'status': 'error',
                    'action_taken': 'Failed to create metadata page',
                    'error': 'Metadata page creation failed'
                })
                with processed_lock:
                    error_count += 1
                return result
            
            # Add corrected metadata page
            writer.add_page(new_metadata_page)
            
            # Add all content pages (skip original metadata page)
            for page_num in range(1, len(original_reader.pages)):
                writer.add_page(original_reader.pages[page_num])
            
            # Write corrected PDF to memory
            output_stream = io.BytesIO()
            writer.write(output_stream)
            output_stream.seek(0)
            corrected_pdf_content = output_stream.getvalue()
            
            # Upload corrected PDF back to S3
            if hasattr(self, 's3_service'):
                # Use orchestrator's S3 service
                self.s3_service.put_object(self.bucket_name, s3_key, corrected_pdf_content)
            else:
                # Use standalone S3 client
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=corrected_pdf_content,
                    ContentType='application/pdf'
                )
            
            result.update({
                'status': 'fixed',
                'action_taken': 'Replaced first page with corrected metadata',
            })
            
            with processed_lock:
                fixed_count += 1
            
            logger.info(f"✅ Fixed: {s3_key}")
            
        except Exception as e:
            result.update({
                'status': 'error',
                'action_taken': 'Processing failed',
                'error': str(e)
            })
            with processed_lock:
                error_count += 1
            logger.error(f"❌ Error fixing {s3_key}: {e}")
        
        finally:
            result['processing_time'] = round(time.time() - start_time, 2)
        
        return result
    
    def find_files_needing_fix(self) -> List[str]:
        """Find all files in the target folder that need metadata fixing."""
        files_to_fix = []
        
        logger.info(f"Scanning {TARGET_FOLDER} for files with truncated URIs...")
        
        paginator = s3.get_paginator("list_objects_v2")
        folder_path = TARGET_FOLDER if TARGET_FOLDER.endswith('/') else TARGET_FOLDER + '/'
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=folder_path)
        
        total_scanned = 0
        
        for page in page_iterator:
            contents = page.get("Contents", [])
            for obj in contents:
                key = obj["Key"]
                total_scanned += 1
                
                # Only process PDF files
                if key.lower().endswith('.pdf') and not key.endswith('/'):
                    metadata = self.extract_metadata_from_first_page(key)
                    current_uri = metadata.get('chunk_s3_uri', '')
                    expected_uri = self.generate_expected_uri(key)
                    
                    # Check if needs fixing
                    if (current_uri.endswith('...') or 
                        (current_uri and current_uri != expected_uri)):
                        files_to_fix.append(key)
                
                if total_scanned % 1000 == 0:
                    print(f"📊 Scanned {total_scanned:,} files, found {len(files_to_fix):,} needing fixes")
        
        logger.info(f"Scan complete: {len(files_to_fix)} files need fixing out of {total_scanned} scanned")
        return files_to_fix
    
    def process_files_parallel(self, files_to_fix: List[str], max_workers: int = 8) -> List[Dict[str, Any]]:
        """Process multiple files in parallel."""
        results = []
        
        print(f"🔧 Starting parallel processing with {max_workers} workers...")
        print(f"📁 Files to process: {len(files_to_fix):,}")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.fix_single_file, file_path): file_path 
                for file_path in files_to_fix
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_file):
                result = future.result()
                results.append(result)
                
                # Show progress every 10 files
                if len(results) % 10 == 0:
                    print(f"🔄 Processed {len(results):,}/{len(files_to_fix):,} files...")
        
        return results
    
    def display_results_table(self, results: List[Dict[str, Any]]):
        """Display results in a formatted table."""
        print("\n" + "="*150)
        print("📊 METADATA FIXING RESULTS TABLE")
        print("="*150)
        
        # Summary statistics
        status_counts = {}
        for result in results:
            status = result['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print(f"\n📈 SUMMARY:")
        print(f"   Total Processed: {len(results):,}")
        print(f"   ✅ Fixed: {status_counts.get('fixed', 0):,}")
        print(f"   ⏭️  Skipped: {status_counts.get('skipped', 0):,}")
        print(f"   ❌ Errors: {status_counts.get('error', 0):,}")
        
        # Detailed table with URI information
        print(f"\n📋 DETAILED RESULTS:")
        print("-"*150)
        print(f"{'#':<3} {'Status':<8} {'File Path':<40} {'Original URI':<35} {'Corrected URI':<35} {'Time':<6}")
        print("-"*150)
        
        for i, result in enumerate(results, 1):
            status_emoji = {
                'fixed': '✅',
                'skipped': '⏭️',
                'error': '❌'
            }.get(result['status'], '❓')
            
            # Truncate file path
            file_path = result['file_path']
            if len(file_path) > 37:
                file_path = "..." + file_path[-34:]
            
            # Truncate URIs for display
            original_uri = result.get('original_uri', '')
            if len(original_uri) > 32:
                original_uri = original_uri[:29] + "..."
            
            corrected_uri = result.get('corrected_uri', '')
            if len(corrected_uri) > 32:
                corrected_uri = corrected_uri[:29] + "..."
            
            print(f"{i:<3} {status_emoji} {result['status']:<6} {file_path:<40} {original_uri:<35} {corrected_uri:<35} {result['processing_time']}s")
        
        print("-"*150)
        
        # Show URI comparison for fixed files
        fixed_files = [r for r in results if r['status'] == 'fixed']
        if fixed_files:
            print(f"\n🔧 URI CORRECTIONS MADE:")
            print("-"*150)
            for i, result in enumerate(fixed_files, 1):
                print(f"{i}. File: {result['file_path']}")
                print(f"   Before: {result['original_uri']}")
                print(f"   After:  {result['corrected_uri']}")
                print(f"   Status: ✅ Fixed in {result['processing_time']}s")
                print()
    
    def export_results_to_csv(self, results: List[Dict[str, Any]]) -> str:
        """Export results to CSV file."""
        ist = timezone(timedelta(hours=5, minutes=30))
        timestamp = datetime.now(ist).strftime("%Y%m%d_%H%M%S")
        csv_filename = f"{TARGET_FOLDER.replace('/', '_')}_metadata_fixes_{timestamp}.csv"
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'file_path', 'status', 'original_uri', 'corrected_uri', 
                'action_taken', 'error', 'processing_time'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n💾 Results exported to: {csv_filename}")
        return csv_filename

def main():
    """Main execution function."""
    print(f"🔧 METADATA FIXER - {TARGET_FOLDER}")
    print(f"🪣 S3 Bucket: {BUCKET_NAME}")
    ist = timezone(timedelta(hours=5, minutes=30))
    print(f"📅 Start time: {datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("="*80)
    
    fixer = MetadataFixer()
    
    # Phase 1: Find files needing fixes
    print("🔍 PHASE 1: Scanning for files with truncated URIs...")
    files_to_fix = fixer.find_files_needing_fix()
    
    if not files_to_fix:
        print("✅ No files found that need metadata fixing!")
        return
    
    print(f"📋 Found {len(files_to_fix):,} files that need fixing")
    
    # Phase 2: Process files
    print(f"\n🔧 PHASE 2: Processing files...")
    results = fixer.process_files_parallel(files_to_fix, max_workers=8)
    
    # Phase 3: Display results
    print(f"\n📊 PHASE 3: Results...")
    fixer.display_results_table(results)
    
    # Phase 4: Export results
    fixer.export_results_to_csv(results)
    
    print(f"\n🏁 Processing completed at: {datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"📊 Final Stats: {fixed_count} fixed, {skipped_count} skipped, {error_count} errors")

if __name__ == "__main__":
    main()
