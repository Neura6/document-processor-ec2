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
from services.orchestrator import Orchestrator
from monitoring.metrics import start_metrics_server, queue_depth, messages_processed

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
        self.orchestrator = Orchestrator()
        
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
    
    def process_messages(self, messages: List[Dict]) -> List[str]:
        """Process batch of SQS messages"""
        processed_receipts = []
        
        for message in messages:
            try:
                # Parse the message body
                body = json.loads(message['Body'])
                
                # Handle both direct S3 events and SNS-wrapped events
                if 'Records' in body:
                    records = body['Records']
                else:
                    # Direct S3 event
                    records = [body]
                
                for record in records:
                    if record.get('eventName') == 'ObjectCreated:Put':
                        s3_info = record.get('s3', {})
                        bucket = s3_info.get('bucket', {}).get('name')
                        key = s3_info.get('object', {}).get('key')
                        
                        if bucket and key:
                            logger.info(f"Processing file: {bucket}/{key}")
                            
                            if bucket == os.getenv('SOURCE_BUCKET'):
                                success = self.orchestrator.process_single_file(key)
                                
                                if success:
                                    logger.info(f"Successfully processed: {key}")
                                else:
                                    logger.error(f"Failed to process: {key}")
                            else:
                                logger.info(f"Skipping file from bucket: {bucket}")
                
                processed_receipts.append(message['ReceiptHandle'])
                messages_processed.inc()
                
            except KeyError as e:
                logger.error(f"Error processing message - missing key: {e}")
                logger.error(f"Message content: {message}")
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON: {e}")
                logger.error(f"Raw message body: {message.get('Body', 'No body')}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                logger.error(f"Message content: {message}")
        
        return processed_receipts
    
    def delete_messages(self, receipt_handles: List[str]):
        """Delete processed messages from queue"""
        for receipt_handle in receipt_handles:
            try:
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle
                )
            except ClientError as e:
                logger.error(f"Error deleting message: {e}")
    
    def run(self):
        """Main worker loop"""
        logger.info("Starting SQS Worker...")
        
        # Start metrics server
        start_metrics_server(port=8000)
        logger.info("Metrics server started on port 8000")
        
        while True:
            try:
                # Update queue depth metric
                current_depth = self.get_queue_depth()
                queue_depth.set(current_depth)
                
                # Poll for messages
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=300
                )
                
                messages = response.get('Messages', [])
                
                if messages:
                    logger.info(f"Received {len(messages)} messages from queue")
                    
                    # Process messages
                    processed_receipts = self.process_messages(messages)
                    
                    # Delete processed messages
                    if processed_receipts:
                        self.delete_messages(processed_receipts)
                        logger.info(f"Deleted {len(processed_receipts)} processed messages")
                
                # Small delay to prevent tight loop
                time.sleep(1)
                
            except KeyboardInterrupt:
                logger.info("Worker stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                time.sleep(5)

if __name__ == "__main__":
    worker = SQSWorker()
    worker.run()
