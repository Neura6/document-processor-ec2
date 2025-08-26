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
from monitoring.metrics import start_metrics_server, queue_depth, messages_processed, active_processing_jobs

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
            depth = int(response['Attributes']['ApproximateNumberOfMessages'])
            queue_depth.set(depth)  # Update real-time metric
            return depth
        except Exception as e:
            logger.error(f"Error getting queue depth: {e}")
            return 0
    
    def update_queue_depth(self) -> None:
        """Update queue depth metric in real-time"""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            depth = int(response['Attributes']['ApproximateNumberOfMessages'])
            queue_depth.set(depth)
        except Exception as e:
            logger.error(f"Error updating queue depth: {e}")
    
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
                success = self.process_message(message)
                if success:
                    self._delete_message(message, "unknown")
                    
                    messages_processed.inc()
            
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
                logger.debug(f"Deleted message with receipt handle: {receipt_handle[:20]}...")
            except Exception as e:
                logger.error(f"Error deleting message: {e}")
    
    def run(self):
        """Main worker loop with real-time metrics"""
        logger.info("Starting SQS Worker...")
        start_metrics_server(port=8000)
        
        while True:
            try:
                # Update queue depth before processing
                self.update_queue_depth()
                
                # Get messages from SQS
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=self.max_messages,
                    WaitTimeSeconds=self.wait_time,
                    VisibilityTimeout=self.visibility_timeout
                )
                
                if 'Messages' not in response:
                    logger.debug("No messages received, waiting...")
                    time.sleep(5)
                    continue
                
                messages = response['Messages']
                logger.info(f"Received {len(messages)} messages")
                
                # Process messages
                receipt_handles_to_delete = []
                
                for message in messages:
                    try:
                        success = self.process_message(message)
                        if success:
                            receipt_handles_to_delete.append(message['ReceiptHandle'])
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                
                # Delete successfully processed messages
                if receipt_handles_to_delete:
                    self.delete_messages(receipt_handles_to_delete)
                
                # Update queue depth after batch processing
                self.update_queue_depth()
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                time.sleep(10)
    
if __name__ == "__main__":
    worker = SQSWorker()
    worker.run()
