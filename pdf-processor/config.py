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
DIRECT_CHUNKED_BUCKET = os.getenv('DIRECT_CHUNKED_BUCKET', 'rules-repository-alpha')
UNPROCESSED_BUCKET = os.getenv('UNPROCESSED_BUCKET', 'unprocessed-files-error-on-pdf-processing')

# Processing Configuration - Optimized for c5ad.8xlarge (32 vCPUs, 64GB RAM)
MAX_WORKERS_FILENAME_CLEANING = int(os.getenv('MAX_WORKERS_FILENAME_CLEANING', 32))
MAX_WORKERS_OCR_PAGE = int(os.getenv('MAX_WORKERS_OCR_PAGE', 16))
MAX_CONCURRENT_FILES = int(os.getenv('MAX_CONCURRENT_FILES', 32))
MAX_WORKERS_PER_STAGE = int(os.getenv('MAX_WORKERS_PER_STAGE', 16))
MAX_PARALLEL_FILES = int(os.getenv('MAX_PARALLEL_FILES', 10))
DEFAULT_DPI_OCR = int(os.getenv('DEFAULT_DPI_OCR', 300))
OCR_TEXT_THRESHOLD = int(os.getenv('OCR_TEXT_THRESHOLD', 50))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))
ASYNC_PROCESSING = os.getenv('ASYNC_PROCESSING', 'true').lower() == 'true'

# SQS Configuration
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
VISIBILITY_TIMEOUT = int(os.getenv('VISIBILITY_TIMEOUT', 1800))
MAX_MESSAGES = int(os.getenv('MAX_MESSAGES', 10))
WAIT_TIME = int(os.getenv('WAIT_TIME', 20))

# Public IP Configuration for monitoring
PUBLIC_IP = os.getenv('PUBLIC_IP', '18.210.191.199')

# S3 Connection Pool Configuration
S3_MAX_POOL_CONNECTIONS = int(os.getenv('S3_MAX_POOL_CONNECTIONS', 100))
S3_READ_TIMEOUT = int(os.getenv('S3_READ_TIMEOUT', 60))
S3_CONNECT_TIMEOUT = int(os.getenv('S3_CONNECT_TIMEOUT', 60))

# Metrics Configuration
METRICS_PORT = int(os.getenv('METRICS_PORT', 8000))

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
