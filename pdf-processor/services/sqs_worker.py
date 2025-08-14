#!/usr/bin/env python3
"""
PDF Processing SQS Worker with Prometheus Metrics
Monitors SQS queue for S3 upload events and processes PDF files
"""

import os
import json
import time
import boto3
import logging
from pathlib import Path
from prometheus_client import start_http_server, Counter, Gauge, Histogram
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.orchestrator_instrumented import process_single_file
from services.metrics_service import PDF_PROCESSING_DURATION, PDF_FILES_PROCESSED, SQS_MESSAGES_IN_QUEUE

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/your-account-id/your-queue')

# Ensure AWS credentials are set
if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
    logger.warning("AWS credentials not found in environment variables")

# Initialize AWS clients
sqs = boto3.client('sqs', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)

def get_queue_attributes():
    """Get current queue depth"""
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(response['Attributes']['ApproximateNumberOfMessages'])
    except Exception as e:
        logger.error(f"Error getting queue attributes: {e}")
        return 0

def process_s3_event(record):
    """Process S3 event and extract file info"""
    try:
        s3_event = json.loads(record['body'])
        
        if 'Records' not in s3_event:
            logger.warning("No Records in S3 event")
            return None
            
        s3_record = s3_event['Records'][0]
        
        if s3_record['eventName'] != 'ObjectCreated:Put':
            logger.info(f"Skipping event: {s3_record['eventName']}")
            return None
            
        bucket = s3_record['s3']['bucket']['name']
        key = s3_record['s3']['object']['key']
        
        # Skip non-PDF files
        if not key.lower().endswith('.pdf'):
            logger.info(f"Skipping non-PDF file: {key}")
            return None
            
        return {'bucket': bucket, 'key': key}
        
    except Exception as e:
        logger.error(f"Error processing S3 event: {e}")
        return None

def download_from_s3(bucket, key, local_path):
    """Download file from S3"""
    try:
        logger.info(f"Downloading s3://{bucket}/{key} to {local_path}")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket, key, local_path)
        return local_path
    except Exception as e:
        logger.error(f"Error downloading from S3: {e}")
        return None

def process_message(message):
    """Process a single SQS message"""
    receipt_handle = message['ReceiptHandle']
    
    try:
        # Extract S3 event info
        s3_info = process_s3_event(message)
        if not s3_info:
            # Delete message if not relevant
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return
            
        bucket = s3_info['bucket']
        key = s3_info['key']
        
        # Download file
        local_path = f"/tmp/{os.path.basename(key)}"
        downloaded_path = download_from_s3(bucket, key, local_path)
        
        if not downloaded_path:
            logger.error(f"Failed to download {key}")
            return
            
        # Process the PDF
        logger.info(f"Processing PDF: {key}")
        
        start_time = time.time()
        
        try:
            # Process the file
            process_single_file(downloaded_path)
            
            # Record success
            PDF_FILES_PROCESSED.labels(status='success').inc()
            PDF_PROCESSING_DURATION.labels(step='total').observe(time.time() - start_time)
            
            logger.info(f"Successfully processed: {key}")
            
        except Exception as e:
            logger.error(f"Error processing {key}: {e}")
            PDF_FILES_PROCESSED.labels(status='error').inc()
            
        finally:
            # Clean up local file
            if os.path.exists(local_path):
                os.remove(local_path)
            
            # Delete message from queue
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    """Main SQS worker loop"""
    logger.info("Starting PDF Processing SQS Worker")
    logger.info(f"Queue URL: {SQS_QUEUE_URL}")
    
    # Start Prometheus metrics server
    start_http_server(8000)
    logger.info("Metrics server started on port 8000")
    
    message_count = 0
    
    while True:
        try:
            # Update queue depth metric
            queue_depth = get_queue_attributes()
            SQS_MESSAGES_IN_QUEUE.set(queue_depth)
            
            # Receive messages
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                AttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            
            if messages:
                logger.info(f"Received {len(messages)} messages")
                
                for message in messages:
                    process_message(message)
                    message_count += 1
                    
                    # Log progress every 10 messages
                    if message_count % 10 == 0:
                        logger.info(f"Processed {message_count} messages total")
            else:
                logger.debug("No messages received, waiting...")
                
        except KeyboardInterrupt:
            logger.info("Shutting down worker...")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
