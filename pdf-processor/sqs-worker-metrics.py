#!/usr/bin/env python3
"""
SQS Worker with Prometheus Metrics Integration
Polls SQS queue for S3 events and processes PDF files with metrics collection.
"""

import boto3
import json
import logging
import time
import os
import sys
from datetime import datetime
from services.orchestrator_instrumented import InstrumentedOrchestrator
from services.metrics_service import metrics
from config import (
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, 
    AWS_REGION, 
    SQS_QUEUE_URL,
    SOURCE_BUCKET
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SQSWorkerWithMetrics:
    """SQS Worker with integrated metrics collection"""
    
    def __init__(self):
        self.sqs = boto3.client(
            'sqs',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        self.orchestrator = InstrumentedOrchestrator()
        self.queue_url = SQS_QUEUE_URL
        
        logger.info("SQS Worker with metrics initialized")
    
    def get_queue_attributes(self):
        """Get current queue attributes for metrics"""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            return int(response['Attributes'].get('ApproximateNumberOfMessages', 0))
        except Exception as e:
            logger.error(f"Error getting queue attributes: {e}")
            return 0
    
    def process_message(self, message):
        """Process a single SQS message with metrics"""
        try:
            # Parse S3 event
            body = json.loads(message['Body'])
            
            if 'Records' not in body:
                logger.warning("No Records found in message")
                return False
            
            for record in body['Records']:
                if record['eventName'] == 'ObjectCreated:Put':
                    s3_info = record['s3']
                    bucket_name = s3_info['bucket']['name']
                    file_key = s3_info['object']['key']
                    
                    # Skip if not in source bucket
                    if bucket_name != SOURCE_BUCKET:
                        logger.info(f"Skipping file from bucket: {bucket_name}")
                        continue
                    
                    # Skip non-PDF files
                    if not file_key.lower().endswith(('.pdf', '.docx', '.doc', '.txt', '.rtf')):
                        logger.info(f"Skipping non-PDF file: {file_key}")
                        continue
                    
                    logger.info(f"Processing S3 event: {bucket_name}/{file_key}")
                    
                    # Process file with orchestrator
                    start_time = time.time()
                    success = self.orchestrator.process_single_file(file_key)
                    processing_time = time.time() - start_time
                    
                    if success:
                        logger.info(f"Successfully processed {file_key} in {processing_time:.1f}s")
                    else:
                        logger.error(f"Failed to process {file_key}")
                    
                    return success
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            metrics.record_error('sqs_worker', 'message_processing_error', 'sqs')
            return False
    
    def poll_and_process(self):
        """Main polling loop with metrics"""
        logger.info("Starting SQS polling loop...")
        
        while True:
            try:
                # Update queue metrics
                queue_size = self.get_queue_attributes()
                metrics.update_queue_messages('pdf-processing-queue', queue_size)
                
                # Receive messages
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=300
                )
                
                messages = response.get('Messages', [])
                
                if messages:
                    logger.info(f"Received {len(messages)} messages from queue")
                    
                    for message in messages:
                        # Process message
                        success = self.process_message(message)
                        
                        # Delete message if processed successfully
                        if success:
                            try:
                                self.sqs.delete_message(
                                    QueueUrl=self.queue_url,
                                    ReceiptHandle=message['ReceiptHandle']
                                )
                                logger.info("Message deleted successfully")
                            except Exception as e:
                                logger.error(f"Error deleting message: {e}")
                else:
                    logger.debug("No messages received, continuing...")
                
                # Small delay to prevent tight loop
                time.sleep(1)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                time.sleep(5)  # Wait before retrying
    
    def run(self):
        """Run the worker"""
        logger.info("Starting SQS Worker with metrics...")
        
        try:
            self.poll_and_process()
        except Exception as e:
            logger.error(f"Fatal error in worker: {e}")
            sys.exit(1)

if __name__ == "__main__":
    worker = SQSWorkerWithMetrics()
    worker.run()
