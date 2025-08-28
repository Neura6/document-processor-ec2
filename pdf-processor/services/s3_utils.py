"""
S3 Utilities Service
Provides utility functions for S3 operations including file movement and copying.
"""

import boto3
import logging

logger = logging.getLogger(__name__)

class S3Utils:
    """Utility class for advanced S3 operations."""
    
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str = 'us-east-1'):
        """Initialize S3 utilities with AWS credentials."""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.logger = logging.getLogger(__name__)
    
    def move_s3_object(self, source_bucket: str, source_key: str, destination_bucket: str, destination_key: str) -> bool:
        """
        Move an S3 object from source to destination location.
        
        Args:
            source_bucket: Source S3 bucket name
            source_key: Source S3 object key
            destination_bucket: Destination S3 bucket name
            destination_key: Destination S3 object key
            
        Returns:
            bool: True if move operation successful, False otherwise
        """
        try:
            # Copy object to destination location
            self.s3_client.copy_object(
                Bucket=destination_bucket,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Key=destination_key
            )
            
            # Remove from source after successful copy
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            
            self.logger.info(f"Moved S3 object: s3://{source_bucket}/{source_key} -> s3://{destination_bucket}/{destination_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error moving S3 object from s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}: {str(e)}")
            return False
    
    def copy_s3_object(self, source_bucket: str, source_key: str, destination_bucket: str, destination_key: str) -> bool:
        """
        Copy an S3 object to destination without removing source.
        
        Args:
            source_bucket: Source S3 bucket name
            source_key: Source S3 object key
            destination_bucket: Destination S3 bucket name
            destination_key: Destination S3 object key
            
        Returns:
            bool: True if copy operation successful, False otherwise
        """
        try:
            self.s3_client.copy_object(
                Bucket=destination_bucket,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Key=destination_key
            )
            
            self.logger.info(f"Copied S3 object: s3://{source_bucket}/{source_key} -> s3://{destination_bucket}/{destination_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error copying S3 object from s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}: {str(e)}")
            return False
