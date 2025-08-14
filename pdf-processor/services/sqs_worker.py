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

from services.orchestrator_instrumented import InstrumentedOrchestrator
from services.metrics_service import metrics

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
        # Handle both direct S3 events and SQS messages
        body = record.get('body', '')
        
        # Try to parse as JSON
        try:
            s3_event = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in message body")
            return None
            
        # Handle direct S3 event format
        if 'Records' in s3_event and s3_event['Records']:
            s3_record = s3_event['Records'][0]
            
            if s3_record.get('eventName') != 'ObjectCreated:Put':
                logger.info(f"Skipping event: {s3_record.get('eventName')}")
                return None
                
            bucket = s3_record['s3']['bucket']['name']
            key = s3_record['s3']['object']['key']
            
            # Skip non-PDF files
            if not key.lower().endswith('.pdf'):
                logger.info(f"Skipping non-PDF file: {key}")
                return None
                
            return {'bucket': bucket, 'key': key}
        
        # Handle test messages or other formats
        logger.info("Skipping non-S3 event message")
        return None
        
    except KeyError as e:
        logger.error(f"Missing key in S3 event: {e}")
        return None
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
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return
            
        bucket = s3_info['bucket']
        key = s3_info['key']
        
        # Process the PDF directly from S3
        logger.info(f"Processing PDF from S3: s3://{bucket}/{key}")
        
        start_time = time.time()
        
        try:
            # Initialize orchestrator and process the file
            orchestrator = InstrumentedOrchestrator()
            success = orchestrator.process_single_file(key)
            
            if success:
                metrics.files_processed_total.labels(status='success', folder='sqs', step='total').inc()
                logger.info(f"Successfully processed: {key}")
            else:
                metrics.files_processed_total.labels(status='error', folder='sqs', step='total').inc()
                logger.error(f"Failed to process: {key}")
            
            metrics.processing_duration.labels(step='total', folder='sqs').observe(time.time() - start_time)
        
        except Exception as e:
            logger.error(f"Error processing {key}: {e}")
            metrics.files_processed_total.labels(status='error', folder='sqs', step='total').inc()
            
        finally:
            # Delete message from queue regardless of success/failure
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    """Main SQS worker loop"""
    logger.info("Starting PDF Processing SQS Worker")
    logger.info(f"Queue URL: {SQS_QUEUE_URL}")
    
    # Start Prometheus metrics server on different port
    start_http_server(8001)
    logger.info("Metrics server started on port 8001")
    
    message_count = 0
    
    while True:
        try:
            # Update queue depth metric
            queue_depth = get_queue_attributes()
            # Use a simple counter for now since we don't have SQS queue metric
            metrics.files_processed_total.labels(status='queue_check', folder='sqs', step='monitoring').inc()
            
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
