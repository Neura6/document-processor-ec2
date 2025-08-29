#!/usr/bin/env python3
"""
SQS Worker for PDF Processing Pipeline
Monitors SQS queue for S3 upload events and processes PDFs
"""

import os
import json
import logging
import time
from typing import List, Dict, Any
import boto3
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
from services.orchestrator import Orchestrator
from services.sqs_monitor import SQSMonitor
from monitoring.metrics import start_metrics_server, messages_processed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQSWorker:
    def __init__(self):
        self.sqs = boto3.client('sqs', region_name=os.getenv('AWS_REGION', 'us-east-1'))
        self.queue_url = os.getenv('SQS_QUEUE_URL')
        self.visibility_timeout = int(os.getenv('VISIBILITY_TIMEOUT', '300'))  # 5 minutes
        self.max_messages = int(os.getenv('MAX_MESSAGES', '10'))
        self.wait_time = int(os.getenv('WAIT_TIME', '20'))  # Long polling
        self.max_retries = 2  # Maximum number of retry attempts
        self.retry_delay = 30  # 30 seconds delay between retries
        self.orchestrator = Orchestrator()
        self.sqs_monitor = SQSMonitor(self.queue_url, os.getenv('AWS_REGION', 'us-east-1'))
        
        if not self.queue_url:
            raise ValueError("SQS_QUEUE_URL environment variable not set")
        
        logger.info("SQS Worker initialized")
    
    def get_queue_depth(self) -> int:
        """Get current queue depth for metrics"""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            return int(response['Attributes']['ApproximateNumberOfMessages'])
        except Exception as e:
            logger.error(f"Error getting queue depth: {e}")
            return 0
    
    def process_messages(self, messages):
        """Process messages from SQS queue
        
        Args:
            messages: List of SQS messages to process
        """
        if not messages:
            return
            
        logger.info(f"Processing {len(messages)} messages")
        
        for message in messages:
            try:
                body = json.loads(message['Body'])
                records = body.get('Records', [])
                
                for record in records:
                    s3_record = record.get('s3', {})
                    bucket_name = s3_record.get('bucket', {}).get('name')
                    object_key = s3_record.get('object', {}).get('key')
                    
                    if not all([bucket_name, object_key]):
                        logger.error(f"Invalid S3 event format: {body}")
                        continue
                    
                    # URL decode the object key to handle spaces properly
                    object_key = unquote_plus(object_key)
                    
                    # Log with proper path formatting
                    display_path = object_key.replace('+', ' ')
                    
                    # Clean the filename before processing
                    filename_service = FilenameService()
                    
                    folder_path = '/'.join(object_key.split('/')[:-1]) if '/' in object_key else ''
                    original_filename = object_key.split('/')[-1]
                    cleaned_filename_only = filename_service.clean_filename(original_filename)
                    
                    if folder_path:
                        cleaned_object_key = f"{folder_path}/{cleaned_filename_only}"
                    else:
                        cleaned_object_key = cleaned_filename_only
                    
                    logger.info(f"Processing file: s3://{bucket_name}/{object_key}")
                    logger.info(f"Cleaned filename: {original_filename} -> {cleaned_filename_only}")
                    
                    # Process the file
                    result = self.orchestrator.process_single_file(bucket_name, object_key)
                    
                    if result:
                        logger.info(f"Successfully processed: {object_key}")
                        self._delete_message(message, object_key)
                    else:
                        logger.error(f"Failed to process: {object_key}")
                        self._delete_message(message, object_key)
            
            except KeyError as e:
                logger.error(f"Error processing message - missing key: {e}")
                logger.error(f"Message content: {message}")
                self._delete_message(message, "unknown")
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON: {e}")
                logger.error(f"Raw message body: {message.get('Body', 'No body')}")
                self._delete_message(message, "unknown")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                logger.error(f"Message content: {message}")
                self._delete_message(message, "unknown")
    
    def _delete_message(self, message, object_key):
        """Helper method to delete a message from SQS"""
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            logger.debug(f"Deleted SQS message for: {object_key}")
        except Exception as e:
            logger.error(f"Failed to delete message for {object_key}: {e}")
        
        return True
        
    def delete_messages(self, receipt_handles: List[str]):
        """Delete processed messages from queue"""
        for receipt_handle in receipt_handles:
            try:
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle
                )
                logger.debug(f"Deleted message with receipt handle: {receipt_handle}")
            except Exception as e:
                logger.error(f"Error deleting message: {e}")
                raise
    
    def poll_sqs(self, max_messages: int = 10) -> List[Dict]:
        """Poll SQS for messages"""
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=10
            )
            return response.get('Messages', [])
        except Exception as e:
            logger.error(f"Error polling SQS: {str(e)}")
            return []
    
    def run(self):
        """Main worker loop with batch processing"""
        logger.info("Starting SQS Worker...")
        start_metrics_server(port=8000)
        
        # Start SQS monitoring
        self.sqs_monitor.start_monitoring()
        
        while True:
            try:
                # Poll for up to 10 messages
                messages = self.poll_sqs(max_messages=10)
                
                if messages:
                    logger.info(f"Received {len(messages)} messages from queue")
                    
                    # Process messages
                    processed_receipts = self.process_messages(messages)
                    
                    # Delete processed messages
                    if processed_receipts:
                        self.delete_messages(processed_receipts)
                else:
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"Worker error: {str(e)}")
                time.sleep(10)
                
            except KeyboardInterrupt:
                logger.info("Worker stopped by user")
                break
                time.sleep(5)
    

if __name__ == "__main__":
    worker = SQSWorker()
    worker.run()
