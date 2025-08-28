import json
import boto3
import uuid
from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
# from PyPDF2._page import PageObject # Not directly needed for our logic, but kept in original code
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer # For creating text-based PDFs
from reportlab.lib.styles import getSampleStyleSheet
import time
import os
from botocore.config import Config
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import re


# ... existing imports ...

# Configure Tesseract OCR paths
# IMPORTANT: These paths assume Tesseract and Poppler are available in a Lambda Layer at /opt
# You MUST ensure your Lambda Layer is correctly configured to place these executables.


# OCR specific imports for Tesseract
try:
    from PIL import Image # Pillow library for image manipulation
    import pytesseract
    from pdf2image import convert_from_bytes # For converting PDF pages to images
    from pdf2image.exceptions import PDFPageCountError
except ImportError:
    logging.warning("Tesseract OCR libraries (Pillow, pytesseract, pdf2image) not found. OCR functionality will be disabled.")
    # Define dummy functions or set a flag to disable OCR if libraries are missing
    Image = None
    pytesseract = None
    convert_from_bytes = None
    PDFPageCountError = Exception # Define a fallback for the exception type
if pytesseract:
    try:
        logger.info("Attempting to configure Tesseract paths...")
        # Set Tesseract command path
        pytesseract.tesseract_cmd = '/opt/bin/tesseract'
        logger.info(f"Tesseract command path set to: {pytesseract.tesseract_cmd}")

        # Configure PATH and LD_LIBRARY_PATH for Tesseract and Poppler executables/libraries
        # This is crucial for Lambda to find the binaries within the /opt directory (Lambda Layer)
        os.environ['PATH'] = os.environ.get('PATH', '') + os.pathsep + '/opt/bin'
        logger.info(f"Updated PATH: {os.environ.get('PATH')}")

        os.environ['LD_LIBRARY_PATH'] = os.environ.get('LD_LIBRARY_PATH', '') + os.pathsep + '/opt/lib'
        logger.info(f"Updated LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH')}")


        # Tesseract data path (for language files like eng.traineddata)
        os.environ['TESSDATA_PREFIX'] = os.environ.get('TESSDATA_PREFIX', '/opt/tessdata')
        logger.info(f"TESSDATA_PREFIX set to: {os.environ.get('TESSDATA_PREFIX')}")

        logger.info("Tesseract path configuration completed.")

    except Exception as e:
        logger.error(f"Error configuring Tesseract paths: {e}. OCR might not work.")
        pytesseract = None # Disable OCR if configuration fails

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
config = Config(connect_timeout=5, read_timeout=500, max_pool_connections=200, retries={'max_attempts': 3})
bedrock = boto3.client('bedrock-agent', config=config, region_name='us-east-1')

# Define S3 buckets and Knowledge Base mappings
SOURCE_BUCKET = 'rules-repository'
CHUNKED_BUCKET = 'chunked-rules-repository'
UNPROCESSED_BUCKET = 'unprocessed-files-error-on-pdf-processing' # New bucket for failed files
UNPROCESSED_FOLDER = 'to_further_process' # New subfolder within unprocessed bucket

# Configure Tesseract OCR paths
# IMPORTANT: These paths assume Tesseract and Poppler are available in a Lambda Layer at /opt
# You MUST ensure your Lambda Layer is correctly configured to place these executables.
if pytesseract:
    try:
        # Set Tesseract command path
        pytesseract.tesseract_cmd = '/opt/bin/tesseract' 
        
        # Configure PATH and LD_LIBRARY_PATH for Tesseract and Poppler executables/libraries
        # This is crucial for Lambda to find the binaries within the /opt directory (Lambda Layer)
        os.environ['PATH'] = os.environ.get('PATH', '') + os.pathsep + '/opt/bin'
        os.environ['LD_LIBRARY_PATH'] = os.environ.get('LD_LIBRARY_PATH', '') + os.pathsep + '/opt/lib'

        # Tesseract data path (for language files like eng.traineddata)
        os.environ['TESSDATA_PREFIX'] = os.environ.get('TESSDATA_PREFIX', '/opt/tessdata')
        
        logger.info(f"Tesseract command path set to: {pytesseract.tesseract_cmd}")
        logger.info(f"TESSDATA_PREFIX set to: {os.environ.get('TESSDATA_PREFIX')}")
        logger.info(f"Updated PATH: {os.environ.get('PATH')}")
        logger.info(f"Updated LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH')}")
    except Exception as e:
        logger.error(f"Error configuring Tesseract paths: {e}. OCR might not work.")
        pytesseract = None # Disable OCR if configuration fails

# Folders that require metadata file creation
METADATA_REQUIRED_FOLDERS = [
    'commercial-laws',
    'Banking Regulations',
    'Capital Market Regulations',
    'Direct Taxes',
    'Indirect Taxes',
    'Insurance',
    'Labour Law',
    'accounting-standards',
    'Auditing Standards'
]

KB_MAPPING = {
    'accounting-standards': {'id': '1XYVHJWXEA', 'data_source_id': 'VQ53AYFFGO'},
    'accounting-global': {'id': 'BPFC6I50NY', 'data_source_id': 'C7NTSCZJPA'},
    'commercial-laws': {'id': 'CAQ6E0JJBW', 'data_source_id': 'AY5EWVXYF6'},
    'usecase-reports': {'id': 'WHF3OXB1MQ', 'data_source_id': 'CN63WXZKZW'},
    'Auditing Standards': {'id': 'XXFFTJYAD1', 'data_source_id': 'OH8L2PTHYU'},
    'Auditing-global': {'id': 'GQOXRJAYPO', 'data_source_id': 'WWPU0ELXBR'},
    'Banking Regulations': {'id': 'VDMWYPSOYO', 'data_source_id': 'I6QTPRZOSP'},
    'Banking Regulations-test': {'id': 'S0X541AD9P', 'data_source_id': 'CIHLQL7Q5E'},
    'Capital Market Regulations': {'id': 'UI4DH8O8GX', 'data_source_id': '8JKIFZD7HF'},
    'Direct Taxes': {'id': 'PV2IGEHKRK', 'data_source_id': 'XNBPX5WR3B'},
    'Indirect Taxes': {'id': 'QTTHYYFCZ9', 'data_source_id': 'JDT9KAXQJL'},
    'Insurance': {'id': 'ECWHGFSH1R', 'data_source_id': 'LPBCRYCSEM'},
    'Labour Law': {'id': 'CAQ6E0JJBW', 'data_source_id': 'PLIPBARA5R'},
    'Finance Tools': {'id': '1XYVHJWXEA', 'data_source_id': '18HMESLJIY'},
    'GIFT City': {'id': 'VDMWYPSOYO', 'data_source_id': 'RL7KXCTETU'},
    'usecase-reports-2': {'id': 'WHF3OXB1MQ', 'data_source_id': '8UK78XZFIF'},
    'Direct-Taxes-case-laws': {'id': 'UJNOWHQEQ9', 'data_source_id': '9OUZYLFVCV'},
    'Indirect-Taxes-case-laws': {'id': 'PMF8OY8ZSG', 'data_source_id': 'WRRCYQD3R0'},
    'Insurance-caselaws': {'id': 'IUB7BI5IXQ', 'data_source_id': 'CB2LPPFQUW'},
    'usecase-reports-4': {'id': 'WHF3OXB1MQ', 'data_source_id': 'CN63WXZKZW'},
    'commercial-case-laws': {'id': 'DQ6AIARMNQ', 'data_source_id': 'NXNCQID5NX'},

}

# Regex for extracting metadata fields from the metadata page text (kept for process_single_file)
META_FIELDS = ["standard_type", "country", "document_type", "document_category", "document_sub-category", "document_name", "chunk_s3_uri", "page_number", "year", "State", "State_category"]
META_REGEX = {field: re.compile(rf"^{field}:\s*(.*)$", re.MULTILINE | re.IGNORECASE) for field in META_FIELDS}


def create_response(status_code, status, message):
    """Helper function to create standardized response format."""
    return {
        'statusCode': status_code,
        'body': json.dumps({
            'status': status,
            'message': message
        })
    }

def lambda_handler(event, context):
    """Lambda function entry point for batch processing."""
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        files = event.get('files', [])
        operation_type = event.get('type', 'upload').lower()

        if not files:
            logger.error("No files or folders provided in the request.")
            return create_response(400, 'Error', 'No files or folders provided in the request')

        if operation_type == 'upload':
            return handle_upload(files)
        elif operation_type == 'delete':
            return handle_delete(files)
        else:
            logger.error(f"Invalid operation type: {operation_type}")
            return create_response(400, 'Error', f'Invalid operation type: {operation_type}')
    except Exception as e:
        logger.exception(f"Exception in lambda_handler: {str(e)}")
        return create_response(500, 'Error', f'Error processing batch: {str(e)}')



def handle_upload(files):
    """Handle the upload operation."""
    if len(files) > 50:
        return create_response(400, 'Error', 'Batch size exceeds limit of 50 files')

    all_files_to_process = []
    root_folders = set()

    for file_path in files:
        parts = file_path.split('/')
        root_folder = parts[0]

        # if root_folder not in KB_MAPPING:
        #     return create_response(400, 'Error', f'Invalid root folder: {root_folder}')

        root_folders.add(root_folder)

        if file_path.endswith('.pdf'):
            all_files_to_process.append(file_path)
        else:
            # It's a folder or subfolder
            folder_files = list_files_in_folder(SOURCE_BUCKET, file_path)
            all_files_to_process.extend(folder_files)

    # Process all files in parallel (chunking)
    try:
        process_files_in_parallel(all_files_to_process) # Chunking now just saves to S3
    except Exception as e:
           # If file processing (chunking) fails, return an error response
           logger.error(f"Error during initial file processing (chunking): {str(e)}")
           return create_response(500, 'Error', f'Error during file processing: {str(e)}')

    # Sync to knowledge base for each root folder, handling failed files by moving
    sync_results = {}
    total_unprocessed_files_moved = [] # Track files successfully moved to unprocessed bucket

    # Process folders sequentially for simpler state management
    for folder in root_folders:
        logger.info(f"Starting sync process for folder: {folder}")
        try:
            # Perform sync with simplified failed file handling
            sync_result = sync_and_handle_failed_files(folder)
            sync_results[folder] = sync_result
            if 'files_moved_to_unprocessed' in sync_result:
                total_unprocessed_files_moved.extend(sync_result['files_moved_to_unprocessed'])

        except Exception as e:
            logger.error(f"Sync process failed for folder {folder}: {str(e)}")
            sync_results[folder] = {'status': 'Error', 'message': str(e)}

    # Check for any folder sync failures and report them
    failed_folders = {f: res for f, res in sync_results.items() if res.get('status') not in ['COMPLETE', 'Completed with Failed Files']}
    if failed_folders:
        error_message = "Some folders failed to sync completely: " + json.dumps(failed_folders)
        # Include files moved to unprocessed found so far in the report
        return create_response(500, 'Error', error_message + f". Files moved to unprocessed bucket ({UNPROCESSED_FOLDER}/): {total_unprocessed_files_moved}")


    if total_unprocessed_files_moved:
        success_message = f"Processing completed. Some files could not be processed by Bedrock and were moved to the unprocessed bucket ({UNPROCESSED_FOLDER}/)."
        return create_response(200, 'Completed with Unprocessed Files', success_message + f": {total_unprocessed_files_moved}")


    return create_response(200, 'Success', 'All files processed successfully.')


def handle_delete(folders):
    """Handle the delete operation."""
    for folder in folders:
        if folder not in KB_MAPPING:
            return create_response(400, 'Error', f'Invalid folder: {folder}')

        # Start a deletion sync for the folder
        try:
            # Deletion sync doesn't involve splitting/moving problematic files in the same way
            sync_result = sync_to_knowledge_base_simple(folder, is_delete=True)
            if sync_result.get('status') != 'COMPLETE':
                logger.error(f"Deletion sync failed for folder {folder}: {sync_result.get('message', 'Unknown error')}")
                return create_response(500, 'Error', f"Deletion sync failed for folder {folder}")

        except Exception as e:
            logger.error(f"Delete sync failed for folder {folder}: {str(e)}")
            return create_response(500, 'Error', f'Error during delete sync for {folder}: {str(e)}')

    return create_response(200, 'Success', 'The deletion operation has been Succeeded')

def list_files_in_folder(bucket, folder):
    """List all files in a given folder in the S3 bucket."""
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    # Ensure the prefix ends with a '/' to list contents within the folder
    prefix = folder if folder.endswith('/') else folder + '/'
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            # Exclude the folder itself if it's returned as an object
            if obj['Key'].endswith('.pdf') and obj['Key'] != prefix:
                files.append(obj['Key'])
    return files


def process_files_in_parallel(files):
    """Process multiple files in parallel using ThreadPoolExecutor (Chunking and saving to S3)."""
    all_chunked_files = []
    failed_files = []

    with ThreadPoolExecutor(max_workers=40) as executor:
        future_to_file = {
            executor.submit(process_single_file, file_path): file_path
            for file_path in files
        }
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                chunked_files = future.result()
                if chunked_files:
                    all_chunked_files.extend(chunked_files)
                    logger.info(f"Successfully chunked {file_path}")
                else:
                    logger.warning(f"File {file_path} did not produce any chunks.")
                    failed_files.append(file_path)
            except Exception as e:
                logger.error(f"Exception during chunking {file_path}: {str(e)}")
                failed_files.append(file_path)


    if failed_files:
        raise Exception(f"The following files failed to process during chunking: {failed_files}")

    return all_chunked_files


def extract_country_from_key(key):
    """Extract country from S3 key path (2nd element in path after the root folder)."""
    parts = key.split('/')
    # Check if there are at least 2 parts after splitting by '/'
    if len(parts) > 1:
        return parts[1]
    return "unknown"


def create_metadata_file(bucket, key, country):
    """Create a metadata file for the given S3 object."""
    try:
        metadata = {
            "metadataAttributes": {
                "country": country
            }
        }

        # Generate metadata filename (filename.metadata.json)
        metadata_key = f"{key}.metadata.json"

        # Check if metadata file already exists to avoid overwriting if the PDF chunk is skipped
        try:
            s3.head_object(Bucket=bucket, Key=metadata_key)
            logger.info(f"Metadata file already exists for {key}. Skipping creation.")
            return True # Metadata file already exists
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Metadata file does not exist, proceed to create
                pass
            else:
                # Other S3 client error
                logger.error(f"S3 error checking for existing metadata file {metadata_key}: {str(e)}")
                return False

        s3.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata)
        )
        logger.info(f"Created metadata file: {metadata_key}")
        return True
    except Exception as e:
        logger.error(f"Error creating metadata file for {key}: {str(e)}")
        return False

# --- Tesseract OCR Helper Functions ---

def is_scanned_pdf_page(page_object):
    """
    Checks if a PDF page is likely a scanned image by attempting to extract text.
    If very little or no text is found, it's considered scanned.
    """
    try:
        text = page_object.extract_text()
        # A page is considered scanned if it has very few "visible" characters
        # This threshold might need tuning based on document types
        if text and len(text.strip()) > 50: # Arbitrary threshold, adjust as needed
            logger.debug(f"Page contains significant text ({len(text.strip())} chars), likely not scanned.")
            return False
        else:
            logger.debug(f"Page contains minimal or no text ({len(text.strip()) if text else 0} chars), likely scanned.")
            return True
    except Exception as e:
        logger.warning(f"Error extracting text to check if page is scanned: {e}. Assuming scanned.")
        return True # Default to true if text extraction fails

def perform_ocr_with_tesseract(image_bytes):
    """
    Performs OCR on the given image bytes using Tesseract and returns the extracted text.
    """
    if not pytesseract or not Image:
        logger.error("Tesseract OCR libraries not initialized. Cannot perform OCR.")
        return None
    try:
        # Open the image from bytes
        image = Image.open(BytesIO(image_bytes))
        # Perform OCR
        text = pytesseract.image_to_string(image)
        logger.info(f"Tesseract OCR performed. Extracted text length: {len(text)}")
        return text
    except pytesseract.TesseractNotFoundError:
        logger.error("Tesseract executable not found. Ensure it's correctly installed and configured in Lambda Layer at /opt/bin/tesseract.")
        return None
    except Exception as e:
        logger.error(f"Error during Tesseract OCR process: {e}")
        return None

def create_text_pdf_page(text_content):
    """
    Creates a new PDF page from plain text content using ReportLab.
    Returns a PyPDF2 PageObject.
    """
    if not text_content:
        return None

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    # Split text into paragraphs
    for para_text in text_content.split('\n'):
        if para_text.strip():
            story.append(Paragraph(para_text.strip(), styles['Normal']))
            story.append(Spacer(1, 0.2 * letter[1])) # Add some spacing between paragraphs

    if not story: # If no text was valid to add
        return None

    try:
        doc.build(story)
        buffer.seek(0)
        # Read the generated PDF and return its first page
        return PdfReader(buffer).pages[0]
    except Exception as e:
        logger.error(f"Error creating text PDF page with ReportLab: {e}")
        return None

# --- End Tesseract OCR Helper Functions ---


def process_single_file(file_path):
    """Process a single PDF file (from source bucket) into chunks, performing OCR if scanned."""
    try:
        logger.info(f"Starting processing for file: {file_path}")

        root_folder = file_path.split('/')[0]
        original_filename_base = os.path.basename(file_path).replace('.pdf', '')
        original_file_dir = os.path.dirname(file_path)
        chunked_file_prefix = f"{original_file_dir}/{original_filename_base}_page_"

        # Check if already chunked (this check prevents reprocessing existing chunks)
        response = s3.list_objects_v2(Bucket=CHUNKED_BUCKET, Prefix=chunked_file_prefix, MaxKeys=1)
        if 'Contents' in response and len(response.get('Contents', [])) > 0:
            logger.info(f"File {file_path} already chunked. Skipping.")
            return []

        # Read PDF from source bucket
        pdf_obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=file_path)
        pdf_content = pdf_obj['Body'].read()
        logger.info(f"Downloaded {file_path} from S3 for processing.")

        try:
            pdf_reader = PdfReader(BytesIO(pdf_content))
        except Exception as e:
            logger.error(f"Failed to read PDF: {file_path} — possibly corrupt. Error: {str(e)}")
            raise

        if not pdf_reader.pages:
            logger.error(f"PDF has no pages: {file_path}")
            raise Exception("Empty PDF")

        chunked_files = []
        base_key_path = os.path.dirname(file_path)
        metadata = extract_metadata_from_source_key(file_path)

        for i, page in enumerate(pdf_reader.pages, 1):
            try:
                pdf_writer = PdfWriter()
                chunk_key = f"{base_key_path}/{original_filename_base}_page_{i}.pdf"
                chunk_s3_uri = f"s3://{CHUNKED_BUCKET}/{chunk_key}"

                chunk_metadata = metadata.copy()
                chunk_metadata['chunk_s3_uri'] = chunk_s3_uri
                chunk_metadata['page_number'] = i

                # Create and prepend metadata page
                metadata_page = create_metadata_page_content(chunk_metadata)
                pdf_writer.add_page(metadata_page)

                # --- OCR Logic Integration with Tesseract ---
                processed_page = page # Default to original page if OCR not performed or fails

                if pytesseract and convert_from_bytes: # Check if OCR libraries are available
                    if is_scanned_pdf_page(page):
                        logger.info(f"Page {i} of {file_path} identified as scanned. Performing OCR with Tesseract...")
                        
                        # To convert a single PyPDF2 page object to image bytes:
                        # 1. Create a temporary BytesIO for a single-page PDF
                        single_page_pdf_writer = PdfWriter()
                        single_page_pdf_writer.add_page(page)
                        single_page_bytes_io = BytesIO()
                        single_page_pdf_writer.write(single_page_bytes_io)
                        single_page_pdf_bytes = single_page_bytes_io.getvalue()

                        try:
                            # Use pdf2image to convert the single-page PDF bytes to an image
                            # Use /tmp for temp image files, dpi for OCR quality
                            images = convert_from_bytes(
                                single_page_pdf_bytes,
                                dpi=300, 
                                fmt='png',
                                paths_to_images_dir='/tmp' # Crucial for Lambda's writable directory
                            )
                            if images:
                                image_bytes_io = BytesIO()
                                images[0].save(image_bytes_io, format='PNG')
                                image_bytes_io.seek(0)
                                
                                ocr_text = perform_ocr_with_tesseract(image_bytes_io.getvalue())
                                if ocr_text:
                                    # Create a new PDF page with the extracted text
                                    new_text_page = create_text_pdf_page(ocr_text)
                                    if new_text_page:
                                        processed_page = new_text_page
                                        logger.info(f"Successfully OCR'd page {i} and created text PDF.")
                                    else:
                                        logger.warning(f"Failed to create text PDF page for OCR'd content of page {i}. Original page will be used.")
                                else:
                                    logger.warning(f"Tesseract OCR failed or returned no text for page {i}. Original page will be used.")
                            else:
                                logger.warning(f"Failed to convert page {i} to image for OCR. Original page will be used.")
                        except PDFPageCountError:
                            logger.warning(f"PDFPageCountError when converting page {i} to image. Likely a corrupt page. Original page will be used.")
                        except Exception as img_conv_err:
                            logger.error(f"Error converting page {i} to image for OCR: {img_conv_err}. Original page will be used.")
                    else:
                        logger.info(f"Page {i} of {file_path} identified as text-based. No OCR needed.")
                else:
                    logger.warning(f"Tesseract OCR functionality is disabled or not fully initialized (missing libraries/config). Skipping OCR for page {i}.")

                pdf_writer.add_page(processed_page) # Add the original or OCR'd page
                # --- End OCR Logic Integration ---

                save_pdf_writer_to_s3(pdf_writer, CHUNKED_BUCKET, chunk_key)
                chunked_files.append(chunk_key)
                logger.info(f"Created and uploaded chunk: {chunk_key}")

                # Create metadata file if needed
                if root_folder in METADATA_REQUIRED_FOLDERS:
                    country = extract_country_from_key(file_path)
                    create_metadata_file(CHUNKED_BUCKET, chunk_key, country)
            except Exception as page_err:
                logger.error(f"Failed to chunk page {i} of {file_path}: {str(page_err)}")
                raise

        logger.info(f"Finished processing {file_path}. Total chunks: {len(chunked_files)}")
        logger.info(f"Created {len(chunked_files)} chunks for {file_path}")
        return chunked_files

    except Exception as e:
        logger.error(f"process_single_file failed for {file_path}: {str(e)}")
        raise   # This will bubble up to process_files_in_parallel

def extract_metadata_from_source_key(key):
    """Extracts metadata from the source file's S3 key."""
    parts = key.split('/')
    folder = parts[0]
    metadata = {'standard_type': folder}

    # Ensure enough parts exist before accessing indices
    if folder in ['Auditing-global', 'Finance Tools', 'GIFT City']:
        if len(parts) > 1:
            if len(parts) > 2:
                metadata['Standard_type'] = parts[1] # Note: inconsistent key capitalization Standard_type vs standard_type
            if len(parts) > 3:
                metadata['document_type'] = parts[2]
            if len(parts) > 4:
                # This logic was a bit inconsistent, leaving as is based on original code structure.
                pass
            if len(parts) > 1:
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
    elif folder == 'accounting-global':
        if len(parts) > 1:
            metadata['complexity'] = parts[1]
            if len(parts) > 2:
                metadata['Standard_type'] = parts[2] # Note: inconsistent key capitalization Standard_type vs standard_type
            if len(parts) > 3:
                metadata['document_type'] = parts[3]
            if len(parts) > 4:
                # This logic was a bit inconsistent, leaving as is based on original code structure.
                pass
            if len(parts) > 1:
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
    elif folder == 'Banking Regulations-test' and parts[1] == 'Bahrain':
        if len(parts) > 2:
            metadata['country'] = parts[1]
            metadata['complexity'] = parts[2]
            if len(parts) > 4:
                metadata['document_type'] = parts[3]
            if len(parts) > 5:
                metadata['document_category'] = parts[4]
            if len(parts) > 6:
                metadata['document_sub-category'] = parts[5]
            if len(parts) > 7:
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
    elif folder in ['accounting-standards','commercial-laws','Banking Regulations','Direct Taxes','Capital Market Regulations','Auditing Standards','Insurance','Labour Law']:
        if len(parts) > 1:
            metadata['country'] = parts[1]
            if len(parts) > 3:
                metadata['document_type'] = parts[2]
            if len(parts) > 4:
                metadata['document_category'] = parts[3]
            if len(parts) > 5:
                metadata['document_sub-category'] = parts[4]
            if len(parts) > 1:
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
    elif folder == 'Indirect Taxes':
        if len(parts) > 1:
            metadata['country'] = parts[1]
            if len(parts) > 3:
                metadata['document_type'] = parts[2]
            if len(parts) > 4:
                metadata['State'] = parts[3]
            if len(parts) > 5:
                metadata['State_category'] = parts[4]
            if len(parts) > 1:
                metadata['document_name'] = os.path.splitext(parts[-1])[0]
    elif folder == 'usecase-reports-4':
        if len(parts) > 1:
            metadata['country'] = parts[1]
            if len(parts) > 2:
                metadata['year'] = parts[2]
            if len(parts) > 1:
                metadata['document_name'] = os.path.splitext(parts[-1])[0]

    return metadata


def extract_metadata_from_chunked_pdf(pdf_reader):
    """Extracts metadata from the first page of a chunked PDF."""
    if not pdf_reader.pages:
        return {}
    first_page_text = pdf_reader.pages[0].extract_text()
    metadata = {}
    if first_page_text:
        for field, pattern in META_REGEX.items():
            match = pattern.search(first_page_text)
            if match:
                metadata[field] = match.group(1).strip()
    return metadata


def create_metadata_page_content(meta_dict):
    """Generates a metadata page content (BytesIO) to prepend to each PDF chunk."""
    packet = BytesIO()
    c = canvas.Canvas(packet, pagesize=letter)
    # Set font and starting position
    c.setFont("Helvetica", 8)
    y = 750 # Starting y coordinate

    # Draw each metadata item
    for key, value in meta_dict.items():
        # Handle None values by converting to string "None" or skipping
        value_str = str(value) if value is not None else "None"
        c.drawString(100, y, f"{key}: {value_str}")
        y -= 12 # Move down for the next line (reduced spacing)

    c.showPage()
    c.save()
    packet.seek(0)
    # Return the BytesIO object containing the PDF metadata page content
    return PdfReader(packet).pages[0]


def save_pdf_writer_to_s3(pdf_writer, bucket, key):
    """Saves a PDF writer object to S3."""
    try:
        output = BytesIO()
        pdf_writer.write(output)
        output.seek(0)
        s3.put_object(Bucket=bucket, Key=key, Body=output.getvalue())
        logger.info(f"Saved PDF to S3: s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Error saving PDF to S3 s3://{bucket}/{key}: {str(e)}")
        raise

def delete_s3_object(bucket, key):
    """Deletes an object from S3."""
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Deleted S3 object: s3://{bucket}/{key}")
        return True
    except Exception as e:
        # Log error but don't raise if deletion fails, as the main goal is to proceed
        logger.error(f"Error deleting S3 object s3://{bucket}/{key}: {str(e)}")
        return False

def move_s3_object(source_bucket, source_key, destination_bucket, destination_key):
    """Moves an object from one S3 location to another."""
    try:
        # Use s3.copy_object and s3.delete_object for a move operation
        s3.copy_object(
            Bucket=destination_bucket,
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Key=destination_key
        )
        # Only delete from source if copy was successful
        delete_success = delete_s3_object(source_bucket, source_key)
        if not delete_success:
            logger.warning(f"Failed to delete source object {source_key} after successful copy to {destination_key}.")
        logger.info(f"Moved S3 object from s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}")
        return True
    except Exception as e:
        logger.error(f"Error moving S3 object from s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}: {str(e)}")
        return False


def sync_and_handle_failed_files(folder):
    """
    Starts an ingestion job, identifies failed files due to token limits,
    moves them to the unprocessed bucket's to_further_process folder,
    and starts a second sync job.
    """
    kb_info = KB_MAPPING[folder]
    failed_files_initial_sync = [] # Files that failed the first sync due to token limits
    files_successfully_moved = [] # Files successfully moved to unprocessed

    # --- Step 1: Initial Sync Attempt ---
    job_id_step1 = None
    try:
        client_token_step1 = str(uuid.uuid4())
        description_step1 = f"Initial batch sync for {folder}"
        response = bedrock.start_ingestion_job(
            clientToken=client_token_step1,
            dataSourceId=kb_info['data_source_id'],
            knowledgeBaseId=kb_info['id'],
            description=description_step1
        )
        job_id_step1 = response['ingestionJob']['ingestionJobId']
        logger.info(f"Started initial ingestion job {job_id_step1} for folder {folder}")

        wait_result_step1 = wait_for_ingestion_job(kb_info, job_id_step1)

        if 'failed_files' in wait_result_step1 and wait_result_step1['failed_files']:
            failed_files_initial_sync.extend(wait_result_step1['failed_files'])
            logger.warning(f"Initial sync failed for these files due to token limits: {failed_files_initial_sync}")


    except Exception as e:
        logger.error(f"Error during initial sync job start or wait for folder {folder}: {str(e)}")
        # If the initial job itself fails completely, we cannot get specific failed files.
        # Report this folder as failed.
        return {'status': 'Error', 'message': f"Initial sync job failed to start or complete successfully: {str(e)}"}

    # --- Step 2: Move Failed Files to Unprocessed Bucket's to_further_process folder in Parallel ---
    if failed_files_initial_sync:
        logger.info(f"Moving {len(failed_files_initial_sync)} files to unprocessed bucket ({UNPROCESSED_FOLDER}/) in parallel.")
        moved_files_status = {} # Track success/failure of each move operation
        # Increased max_workers for parallel moving - Increased to 20
        with ThreadPoolExecutor(max_workers=20) as executor:
            future_to_file = {}
            for failed_file_key in failed_files_initial_sync:
                # Construct the new destination key: "to_further_process/filename.pdf"
                destination_key = f"{UNPROCESSED_FOLDER}/{os.path.basename(failed_file_key)}"
                future_to_file[executor.submit(move_s3_object, CHUNKED_BUCKET, failed_file_key, UNPROCESSED_BUCKET, destination_key)] = failed_file_key

            for future in as_completed(future_to_file):
                original_failed_file_key = future_to_file[future]
                try:
                    move_success = future.result()
                    if move_success:
                        # Get the actual destination key used
                        destination_key_used = f"{UNPROCESSED_FOLDER}/{os.path.basename(original_failed_file_key)}"
                        moved_files_status[original_failed_file_key] = f'MOVED_TO:{destination_key_used}'
                        files_successfully_moved.append(destination_key_used)
                    else:
                        # move_s3_object already logs the error and attempts deletion from source
                        moved_files_status[original_failed_file_key] = 'MOVE_FAILED_DELETE_ATTEMPTED'
                        logger.error(f"Failed to move {original_failed_file_key} to unprocessed bucket. Check logs for move_s3_object errors.")
                except Exception as e:
                    logger.error(f"Exception during parallel move to unprocessed for {original_failed_file_key}: {str(e)}")
                    moved_files_status[original_failed_file_key] = 'MOVE_EXCEPTION'
                    # If an exception occurred during the move attempt, try to delete from chunked as a last resort
                    try:
                        delete_s3_object(CHUNKED_BUCKET, original_failed_file_key)
                        logger.warning(f"Attempted to delete file {original_failed_file_key} from chunked bucket after move exception.")
                    except Exception as delete_e:
                        logger.error(f"Failed to delete {original_failed_file_key} from chunked bucket after move exception: {str(delete_e)}")

        # Log the status of moving files
        logger.info(f"Status of moving failed files to unprocessed: {moved_files_status}")


    # --- Step 3: Start a Second Sync Attempt for Remaining Files ---
    # This sync job will process the files that were NOT moved to the unprocessed bucket.
    job_id_step2 = None
    try:
        client_token_step2 = str(uuid.uuid4())
        description_step2 = f"Retry sync after moving failed files for {folder}"
        response = bedrock.start_ingestion_job(
            clientToken=client_token_step2,
            dataSourceId=kb_info['data_source_id'],
            knowledgeBaseId=kb_info['id'],
            description=description_step2
        )
        job_id_step2 = response['ingestionJob']['ingestionJobId']
        logger.info(f"Started retry ingestion job {job_id_step2} for folder {folder}")

        # Wait for this second sync job to complete
        # We don't expect token errors here for the files that remain, but other errors could occur.
        wait_result_step2 = wait_for_ingestion_job(kb_info, job_id_step2)
        if wait_result_step2.get('status') != 'COMPLETE':
            logger.warning(f"Retry sync job {job_id_step2} completed with status: {wait_result_step2.get('status')}. Message: {wait_result_step2.get('message', 'No message')}")
            # Any *new* failed files from this second sync are unexpected,
            # but we've already moved the originally problematic ones.
            # We report the original set of unprocessed files regardless.


    except Exception as e:
        logger.error(f"Error during retry sync job start or wait for folder {folder}: {str(e)}")
        # If the second sync fails completely, report the files that were successfully moved
        # and the sync error.
        return {'status': 'Completed with Errors and Unprocessed Files', 'files_moved_to_unprocessed': files_successfully_moved, 'retry_sync_error': str(e)}

    # --- Final Result ---
    if files_successfully_moved:
        logger.warning(f"Processing completed for folder {folder}. The following files failed initial ingestion due to token limits and were moved to {UNPROCESSED_BUCKET}/{UNPROCESSED_FOLDER}: {files_successfully_moved}")
        return {'status': 'Completed with Failed Files', 'files_moved_to_unprocessed': files_successfully_moved}
    else:
        logger.info(f"Processing completed successfully for folder {folder}. No files moved to unprocessed bucket.")
        return {'status': 'COMPLETE'}


def sync_to_knowledge_base_simple(folder, is_delete=False):
    """Starts and waits for a single Bedrock ingestion job for a folder."""
    kb_info = KB_MAPPING[folder]
    client_token = str(uuid.uuid4())
    description = f"{'Deletion sync' if is_delete else 'Batch sync'} for {folder}"

    try:
        response = bedrock.start_ingestion_job(
            clientToken=client_token,
            dataSourceId=kb_info['data_source_id'],
            knowledgeBaseId=kb_info['id'],
            description=description
        )
        job_id = response['ingestionJob']['ingestionJobId']
        logger.info(f"Started {'deletion' if is_delete else 'ingestion'} job {job_id} for folder {folder}")

        # Wait for job to complete
        wait_result = wait_for_ingestion_job(kb_info, job_id)
        return wait_result # Returns status and potentially failed files

    except Exception as e:
        logger.error(f"Error during sync job start or wait for folder {folder}: {str(e)}")
        raise # Re-raise the exception


def wait_for_ingestion_job(kb_info, job_id):
    """
    Polls the knowledge base for ingestion job status, extracts failed files
    due to token limits if the job fails with relevant reasons.
    """
    max_attempts = 60 # Increased attempts to wait longer
    interval = 30   # seconds
    failed_files_due_to_tokens = [] # List to collect file paths from token errors
    # Corrected regex pattern to match the file path correctly
    token_error_pattern = re.compile(r"Issue occurred while processing file: (.*?\.\w+)\.")

    logger.info(f"Waiting for ingestion job {job_id} to complete...")

    for attempt in range(max_attempts):
        try:
            response = bedrock.get_ingestion_job(
                dataSourceId=kb_info['data_source_id'],
                ingestionJobId=job_id,
                knowledgeBaseId=kb_info['id']
            )
            status = response['ingestionJob']['status']
            failure_reasons = response['ingestionJob'].get('failureReasons', [])

            if status == 'COMPLETE':
                logger.info(f"Ingestion job {job_id} completed successfully")
                # Return any failed files identified during polling even if final status is COMPLETE
                return {'status': 'COMPLETE', 'failed_files': failed_files_due_to_tokens}

            elif status == 'FAILED':
                logger.error(f"Ingestion job {job_id} failed: {failure_reasons}")

                # Check for "Too many input tokens" error in failure reasons
                token_error_found_in_this_poll = False
                for reason in failure_reasons:
                    # Check if the reason is a string before using regex search
                    if isinstance(reason, str):
                        # Use the corrected regex pattern
                        match = token_error_pattern.search(reason)
                        if match:
                            failed_file = match.group(1)
                            if failed_file not in failed_files_due_to_tokens: # Avoid duplicates
                                failed_files_due_to_tokens.append(failed_file)
                                logger.warning(f"Found problematic file: {failed_file} due to token limit in job {job_id}.")
                            token_error_found_in_this_poll = True

                # If the job failed and we found token errors, return the list of failed files.
                # The caller will handle moving these.
                if failed_files_due_to_tokens:
                    logger.warning(f"Ingestion job {job_id} failed with token errors. Returning failed files list.")
                    return {'status': 'FAILED_TOKEN_ERROR', 'failed_files': failed_files_due_to_tokens}
                else:
                    # If failure reasons are not related to token limits, raise an exception
                    # or return a specific error status if we want the caller to handle non-token failures differently.
                    # For this simplified flow, let's return a generic failed status.
                    return {'status': 'FAILED_OTHER_ERROR', 'message': f'Ingestion job failed: {failure_reasons}'}


            logger.info(f"Ingestion job {job_id} status: {status}. Waiting...")
            time.sleep(interval)

        except Exception as e:
            logger.error(f"Error checking ingestion job status for job {job_id}: {str(e)}")
            # If an error occurs while waiting/polling, and we have identified files
            # that previously caused token errors in this job, return those for processing.
            # Otherwise, re-raise the exception.
            if failed_files_due_to_tokens:
                logger.warning(f"Error during polling for job {job_id}. Returning identified failed files.")
                # Return current state of failed_files_due_to_tokens
                return {'status': 'ERROR_DURING_WAIT', 'failed_files': failed_files_due_to_tokens, 'polling_error': str(e)}
            else:
                # If no token errors were identified before the polling error, re-raise
                raise

    # If loop completes without returning, it timed out
    logger.error(f"Ingestion job {job_id} timed out after {max_attempts} attempts")
    # Even on timeout, check if we found problematic files during polling
    if failed_files_due_to_tokens:
        logger.warning(f"Ingestion job {job_id} timed out. Returning identified failed files.")
        return {'status': 'TIMEOUT_TOKEN_ERROR', 'failed_files': failed_files_due_to_tokens, 'timeout': True}
    else:
        raise Exception(f"Ingestion job {job_id} timed out after {max_attempts} attempts")