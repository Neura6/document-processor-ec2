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
