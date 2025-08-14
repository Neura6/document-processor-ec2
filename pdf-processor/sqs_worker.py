#!/usr/bin/env python3
"""
SQS Worker with Metrics Integration
Processes SQS messages with full monitoring
"""

import os
import time
import logging
import json
from services.orchestrator import Orchestrator
from services.s3_service import S3Service
from services.metrics_service import metrics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQSWorker:
    def __init__(self):
        self.orchestrator = Orchestrator()
        self.s3_service = S3Service()
        self.logger = logging.getLogger(__name__)
        
    def process_message(self, message):
        """Process a single SQS message"""
        try:
            body = json.loads(message['Body'])
            records = body.get('Records', [])
            
            for record in records:
                if record.get('eventName') == 'ObjectCreated:Put':
                    bucket = record['s3']['bucket']['name']
                    key = record['s3']['object']['key']
                    
                    self.logger.info(f"Processing file: {bucket}/{key}")
                    
                    # Record metrics
                    metrics.set_active_processing(1)
                    
                    # Process file
                    start_time = time.time()
                    success = self.orchestrator.process_file(key)
                    duration = time.time() - start_time
                    
                    # Update metrics
                    metrics.set_active_processing(0)
                    
                    if success:
                        self.logger.info(f"Successfully processed: {key} in {duration:.1f}s")
                        return True
                    else:
                        self.logger.error(f"Failed to process: {key}")
                        return False
                        
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            return False
    
    def run(self):
        """Main worker loop"""
        self.logger.info("Starting SQS Worker with metrics...")
        
        # This is a simplified version - integrate with your actual SQS polling
        while True:
            try:
                # Get messages from SQS (implement your actual SQS polling here)
                messages = []  # Replace with actual SQS polling
                
                if messages:
                    metrics.set_queue_depth(len(messages))
                    
                    for message in messages:
                        if self.process_message(message):
                            # Delete message from queue
                            pass
                        else:
                            # Handle failed message
                            pass
                
                time.sleep(1)  # Poll interval
                
            except KeyboardInterrupt:
                self.logger.info("Shutting down worker...")
                break
            except Exception as e:
                self.logger.error(f"Worker error: {str(e)}")
                time.sleep(5)

if __name__ == "__main__":
    worker = SQSWorker()
    worker.run()
