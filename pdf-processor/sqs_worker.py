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
from monitoring.metrics import start_metrics_server, queue_depth, messages_processed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class SQSWorker:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.sqs = boto3.client('sqs', region_name='us-east-1')
        self.orchestrator = Orchestrator()
        self.queue_url = os.getenv('SQS_QUEUE_URL')
        self.batch_size = 10  # Process 10 files in parallel
        self.max_workers = 10  # Thread pool for parallel processing
        
        if not self.queue_url:
            raise ValueError("SQS_QUEUE_URL environment variable not set")
        
        self.logger.info("SQS Worker initialized")
    
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
    
    def process_messages(self, messages: List[Dict]) -> List[str]:
        """Process batch of SQS messages in parallel"""
        processed_receipts = []
        
        # Process messages in batches of 10
        message_batches = [messages[i:i+self.batch_size] for i in range(0, len(messages), self.batch_size)]
        
        for batch in message_batches:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for message in batch:
                    try:
                        body = json.loads(message['Body'])
                        
                        # Extract S3 event details
                        records = body.get('Records', [])
                        if not records:
                            logger.error(f"No Records found in message: {body}")
                            continue
                        
                        s3_record = records[0].get('s3', {})
                        bucket_name = s3_record.get('bucket', {}).get('name')
                        object_key = s3_record.get('object', {}).get('key')
                        
                        if not bucket_name or not object_key:
                            logger.error(f"Invalid S3 event format: {body}")
                            continue
                        
                        # URL decode the object key
                        object_key = unquote_plus(object_key)
                        
                        # Submit to thread pool for parallel processing
                        future = executor.submit(self.process_single_file_parallel, bucket_name, object_key, message)
                        futures.append(future)
                        
                    except Exception as e:
                        logger.error(f"Error preparing batch: {e}")
                
                # Wait for all parallel processes to complete
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result['success']:
                            processed_receipts.append(result['receipt_handle'])
                            messages_processed.inc()
                        else:
                            logger.error(f"Failed to process: {result['file_key']}")
                    except Exception as e:
                        logger.error(f"Error in parallel processing: {e}")
        
        return processed_receipts
    
    def process_single_file_parallel(self, bucket_name: str, object_key: str, message: Dict) -> Dict:
        """Process single file in parallel"""
        try:
            logger.info(f"Parallel processing: s3://{bucket_name}/{object_key}")
            success = self.orchestrator.process_single_file_parallel(object_key)
            return {
                'success': success,
                'file_key': object_key,
                'receipt_handle': message['ReceiptHandle']
            }
        except Exception as e:
            logger.error(f"Error in parallel processing: {e}")
            return {'success': False, 'file_key': object_key, 'receipt_handle': message['ReceiptHandle']}
    
    def delete_messages(self, receipt_handles: List[str]):
        """Delete processed messages from queue"""
        for receipt_handle in receipt_handles:
            try:
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle
                )
                logger.info(f"Deleted message: {receipt_handle[:20]}...")
            except Exception as e:
                logger.error(f"Error deleting message: {str(e)}")
    
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
