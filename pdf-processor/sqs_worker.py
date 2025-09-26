#!/usr/bin/env python3
"""
SQS Worker for PDF Processing Pipeline
Monitors SQS queue for S3 upload events and processes PDFs in PARALLEL
"""

import os
import json
import logging
import time
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
from services.orchestrator import Orchestrator
from services.filename_service import FilenameService
from services.sqs_monitor import SQSMonitor
from monitoring.metrics_collector import metrics, start_metrics_server

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
        self.visibility_timeout = int(os.getenv('VISIBILITY_TIMEOUT', '1800'))  # 30 minutes for parallel processing
        self.max_messages = int(os.getenv('MAX_MESSAGES', '10'))  # Maximum SQS batch size
        self.wait_time = int(os.getenv('WAIT_TIME', '20'))  # Long polling
        self.max_retries = 2  # Maximum number of retry attempts
        self.retry_delay = 30  # 30 seconds delay between retries
        self.orchestrator = Orchestrator()
        self.sqs_monitor = SQSMonitor(self.queue_url, os.getenv('AWS_REGION', 'us-east-1'))
        
        # Parallel processing configuration
        self.max_workers = int(os.getenv('MAX_PARALLEL_FILES', '10'))  # Process 10 files simultaneously
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        if not self.queue_url:
            raise ValueError("SQS_QUEUE_URL environment variable not set")
        
        logger.info(f"SQS Worker initialized with {self.max_workers} parallel workers")
    
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
    
    def process_single_file_wrapper(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """Wrapper to process a single file in parallel thread"""
        try:
            message = message_data['message']
            record = message_data['record']
            
            s3_record = record.get('s3', {})
            bucket_name = s3_record.get('bucket', {}).get('name')
            object_key = s3_record.get('object', {}).get('key')
            
            if not all([bucket_name, object_key]):
                logger.error(f"Invalid S3 event format: {record}")
                return {'success': False, 'message': 'Invalid format', 'receipt_handle': message['ReceiptHandle']}
            
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
            
            logger.info(f"[PARALLEL] Processing file: s3://{bucket_name}/{object_key}")
            logger.info(f"[PARALLEL] Cleaned filename: {original_filename} -> {cleaned_filename_only}")
            
            # Process the file
            result = self.orchestrator.process_single_file(object_key)
            
            if result:
                logger.info(f"[PARALLEL] Successfully processed: {object_key}")
                return {
                    'success': True,
                    'file': object_key,
                    'receipt_handle': message['ReceiptHandle']
                }
            else:
                logger.error(f"[PARALLEL] Failed to process: {object_key}")
                return {
                    'success': False,
                    'file': object_key,
                    'receipt_handle': message['ReceiptHandle']
                }
                
        except Exception as e:
            logger.error(f"[PARALLEL] Error processing file: {e}")
            return {
                'success': False,
                'error': str(e),
                'receipt_handle': message.get('ReceiptHandle', 'unknown')
            }
    
    def process_messages_parallel(self, messages: List[Dict]) -> List[str]:
        """Process messages in parallel using ThreadPoolExecutor"""
        if not messages:
            return []
            
        logger.info(f"[PARALLEL] Processing {len(messages)} messages with {self.max_workers} workers")
        
        # Prepare message data for parallel processing
        message_data_list = []
        for message in messages:
            try:
                body = json.loads(message['Body'])
                records = body.get('Records', [])
                
                for record in records:
                    message_data_list.append({
                        'message': message,
                        'record': record
                    })
            except Exception as e:
                logger.error(f"[PARALLEL] Error preparing message: {e}")
        
        if not message_data_list:
            return []
        
        # Process files in parallel
        successful_receipts = []
        failed_receipts = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_message = {
                executor.submit(self.process_single_file_wrapper, msg_data): msg_data
                for msg_data in message_data_list
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_message):
                try:
                    result = future.result()
                    if result['success']:
                        successful_receipts.append(result['receipt_handle'])
                    else:
                        failed_receipts.append(result['receipt_handle'])
                        logger.error(f"[PARALLEL] Processing failed: {result}")
                except Exception as e:
                    logger.error(f"[PARALLEL] Thread execution error: {e}")
        
        logger.info(f"[PARALLEL] Completed: {len(successful_receipts)} successful, {len(failed_receipts)} failed")
        return successful_receipts + failed_receipts  # Delete all processed messages
    
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
        if not receipt_handles:
            return
            
        # Delete in batches of 10 (SQS limit)
        for i in range(0, len(receipt_handles), 10):
            batch = receipt_handles[i:i+10]
            try:
                entries = [
                    {'Id': str(idx), 'ReceiptHandle': receipt}
                    for idx, receipt in enumerate(batch)
                ]
                
                self.sqs.delete_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )
                logger.debug(f"Deleted batch of {len(batch)} messages")
            except Exception as e:
                logger.error(f"Error deleting message batch: {e}")
                # Fallback to individual deletions
                for receipt_handle in batch:
                    try:
                        self.sqs.delete_message(
                            QueueUrl=self.queue_url,
                            ReceiptHandle=receipt_handle
                        )
                    except Exception as e2:
                        logger.error(f"Error deleting individual message: {e2}")
    
    def poll_sqs(self, max_messages: int = 10) -> List[Dict]:
        """Poll SQS for messages"""
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=10,
                VisibilityTimeout=self.visibility_timeout
            )
            return response.get('Messages', [])
        except Exception as e:
            logger.error(f"Error polling SQS: {str(e)}")
            return []
    
    def run(self):
        """Main worker loop with parallel processing"""
        logger.info("Starting SQS Worker with PARALLEL processing...")
        logger.info(f"Max parallel workers: {self.max_workers}")
        start_metrics_server(port=8000)
        
        # Start SQS monitoring
        self.sqs_monitor.start_monitoring()
        
        while True:
            try:
                # Poll for up to 10 messages
                messages = self.poll_sqs(max_messages=10)
                
                if messages:
                    logger.info(f"[PARALLEL] Received {len(messages)} messages from queue")
                    
                    # Process messages in parallel
                    processed_receipts = self.process_messages_parallel(messages)
                    
                    # Delete processed messages
                    if processed_receipts:
                        self.delete_messages(processed_receipts)
                else:
                    logger.debug("No messages, sleeping...")
                    time.sleep(5)
                    
            except KeyboardInterrupt:
                logger.info("Worker stopped by user")
                self.executor.shutdown(wait=True)
                break
            except Exception as e:
                logger.error(f"Worker error: {str(e)}")
                time.sleep(10)


if __name__ == "__main__":
    worker = SQSWorker()
    worker.run()
