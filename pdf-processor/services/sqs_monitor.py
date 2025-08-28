"""
Real-time SQS Queue Monitoring Service
Continuously monitors SQS queue depth and updates metrics
"""

import boto3
import time
import threading
import logging
from typing import Optional
from monitoring.metrics import sqs_messages_available, sqs_messages_in_flight
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

class SQSMonitor:
    """Monitors SQS queue in real-time and updates metrics"""
    
    def __init__(self, queue_url: str, region: str = 'us-east-1', poll_interval: int = 5):
        self.queue_url = queue_url
        self.poll_interval = poll_interval
        self.running = False
        self.monitor_thread = None
        self.logger = logging.getLogger(__name__)
        
        # Initialize SQS client
        self.sqs = boto3.client(
            'sqs',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=region
        )
        
    def start_monitoring(self):
        """Start the SQS monitoring thread"""
        if self.running:
            return
            
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        self.logger.info(f"Started SQS monitoring for queue: {self.queue_url}")
        
    def stop_monitoring(self):
        """Stop the SQS monitoring thread"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        self.logger.info("Stopped SQS monitoring")
        
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                self._update_queue_metrics()
                time.sleep(self.poll_interval)
            except Exception as e:
                self.logger.error(f"Error monitoring SQS queue: {e}")
                time.sleep(self.poll_interval)
                
    def _update_queue_metrics(self):
        """Get queue attributes and update metrics"""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=[
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesNotVisible'
                ]
            )
            
            attributes = response.get('Attributes', {})
            
            # Messages available in queue
            messages_available = int(attributes.get('ApproximateNumberOfMessages', 0))
            sqs_messages_available.set(messages_available)
            
            # Messages in flight (being processed)
            messages_in_flight = int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0))
            sqs_messages_in_flight.set(messages_in_flight)
            
            self.logger.debug(f"Queue metrics - Available: {messages_available}, In-flight: {messages_in_flight}")
            
        except Exception as e:
            self.logger.error(f"Failed to get queue attributes: {e}")
            
    def get_queue_depth(self) -> Optional[int]:
        """Get current queue depth synchronously"""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            return int(response['Attributes']['ApproximateNumberOfMessages'])
        except Exception as e:
            self.logger.error(f"Failed to get queue depth: {e}")
            return None
