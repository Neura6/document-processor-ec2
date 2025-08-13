#!/usr/bin/env python3
"""
SQS Worker for PDF Processing
Listens to S3 events via SQS and processes PDF files automatically
"""

import json
import boto3
import logging
import time
import os
import sys
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.orchestrator import Orchestrator
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('sqs_worker.log')
    ]
)
logger = logging.getLogger(__name__)

class SQSWorker:
    """SQS Worker that processes S3 events for PDF processing"""
    
    def __init__(self, queue_url: str, batch_size: int = 10):
        self.queue_url = queue_url
        self.batch_size = batch_size
        self.orchestrator = Orchestrator()
        
        # Initialize AWS clients
        self.sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        logger.info(f"SQS Worker initialized with queue: {queue_url}")
    
    def parse_s3_event(self, message_body: str) -> List[Dict[str, str]]:
        """Parse S3 event from SQS message"""
        try:
            message_data = json.loads(message_body)
            
            # Handle both direct S3 events and SNS-wrapped events
            if 'Records' in message_data:
                records = message_data['Records']
            elif 'Message' in message_data:
                # SNS-wrapped message
                sns_message = json.loads(message_data['Message'])
                records = sns_message.get('Records', [])
            else:
                records = []
            
            s3_events = []
            for record in records:
                if record.get('eventSource') == 'aws:s3' and record.get('eventName', '').startswith('ObjectCreated:'):
                    s3_info = record.get('s3', {})
                    bucket_name = s3_info.get('bucket', {}).get('name')
                    object_key = s3_info.get('object', {}).get('key')
                    
                    if bucket_name and object_key:
                        # URL decode the object key
                        import urllib.parse
                        object_key = urllib.parse.unquote_plus(object_key)
                        
                        s3_events.append({
                            'bucket': bucket_name,
                            'key': object_key,
                            'event_time': record.get('eventTime', ''),
                            'event_name': record.get('eventName', '')
                        })
            
            return s3_events
            
        except Exception as e:
            logger.error(f"Error parsing S3 event: {e}")
            return []
    
    def is_pdf_file(self, key: str) -> bool:
        """Check if file is a PDF or convertible format"""
        pdf_extensions = ['.pdf', '.docx', '.doc', '.txt', '.jpg', '.jpeg', '.png', '.tiff', '.tif']
        file_extension = os.path.splitext(key)[1].lower()
        return file_extension in pdf_extensions
    
    def process_single_file(self, bucket: str, key: str) -> bool:
        """Process a single file using the orchestrator"""
        try:
            logger.info(f"Processing file: s3://{bucket}/{key}")
            
            # Check if it's a PDF or convertible format
            if not self.is_pdf_file(key):
                logger.info(f"Skipping non-PDF file: {key}")
                return True
            
            # Process using orchestrator
            success = self.orchestrator.process_single_file(key)
            
            if success:
                logger.info(f"Successfully processed: {key}")
            else:
                logger.error(f"Failed to process: {key}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing file {key}: {str(e)}")
            return False
    
    def process_batch(self, messages: List[Dict]) -> List[str]:
        """Process a batch of SQS messages"""
        processed_receipts = []
        failed_receipts = []
        
        # Extract all S3 events from messages
        all_files = []
        for message in messages:
            message_body = message.get('Body', '')
            s3_events = self.parse_s3_event(message_body)
            
            for event in s3_events:
                all_files.append({
                    'bucket': event['bucket'],
                    'key': event['key'],
                    'receipt_handle': message['ReceiptHandle']
                })
        
        if not all_files:
            logger.info("No PDF files to process in this batch")
            return [msg['ReceiptHandle'] for msg in messages]
        
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_file = {
                executor.submit(self.process_single_file, file_info['bucket'], file_info['key']): file_info
                for file_info in all_files
            }
            
            for future in as_completed(future_to_file):
                file_info = future_to_file[future]
                receipt_handle = file_info['receipt_handle']
                
                try:
                    success = future.result()
                    if success:
                        processed_receipts.append(receipt_handle)
                    else:
                        failed_receipts.append(receipt_handle)
                except Exception as e:
                    logger.error(f"Exception processing {file_info['key']}: {e}")
                    failed_receipts.append(receipt_handle)
        
        # Return receipts for successful messages to be deleted
        return list(set(processed_receipts))
    
    def poll_and_process(self):
        """Main polling loop that processes messages in batches"""
        logger.info("Starting SQS worker polling loop...")
        
        while True:
            try:
                # Receive messages from SQS
                response = self.sqs_client.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=self.batch_size,
                    WaitTimeSeconds=20,  # Long polling
                    VisibilityTimeout=300  # 5 minutes
                )
                
                messages = response.get('Messages', [])
                
                if not messages:
                    logger.debug("No messages received, continuing...")
                    time.sleep(1)
                    continue
                
                logger.info(f"Received {len(messages)} messages from SQS")
                
                # Process the batch
                processed_receipts = self.process_batch(messages)
                
                # Delete successfully processed messages
                if processed_receipts:
                    self.delete_messages(processed_receipts)
                
                # Log failed messages (they'll reappear after visibility timeout)
                failed_count = len(messages) - len(processed_receipts)
                if failed_count > 0:
                    logger.warning(f"{failed_count} messages failed processing, will retry")
                
            except Exception as e:
                logger.error(f"Error in polling loop: {str(e)}")
                time.sleep(5)  # Wait before retrying
    
    def delete_messages(self, receipt_handles: List[str]):
        """Delete processed messages from SQS"""
        try:
            # SQS can only delete 10 messages at a time
            for i in range(0, len(receipt_handles), 10):
                batch = receipt_handles[i:i+10]
                entries = [
                    {'Id': str(idx), 'ReceiptHandle': receipt_handle}
                    for idx, receipt_handle in enumerate(batch)
                ]
                
                response = self.sqs_client.delete_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )
                
                successful_deletes = len(response.get('Successful', []))
                failed_deletes = len(response.get('Failed', []))
                
                if failed_deletes > 0:
                    logger.warning(f"Failed to delete {failed_deletes} messages")
                
                logger.info(f"Deleted {successful_deletes} messages from SQS")
                
        except Exception as e:
            logger.error(f"Error deleting messages: {str(e)}")

def main():
    """Main entry point for the SQS worker"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SQS Worker for PDF Processing')
    parser.add_argument('--queue-url', required=True, help='SQS queue URL')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of messages to process per batch')
    
    args = parser.parse_args()
    
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        logger.error("AWS credentials not configured. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
        sys.exit(1)
    
    worker = SQSWorker(queue_url=args.queue_url, batch_size=args.batch_size)
    
    try:
        worker.poll_and_process()
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as e:
        logger.error(f"Worker crashed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
