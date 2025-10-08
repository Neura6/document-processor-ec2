"""
S3 Service
Handles all AWS S3 operations including upload, download, copy, and delete.
"""

import boto3
import os
import logging
from typing import List, Dict, Any
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from botocore.config import Config

logger = logging.getLogger(__name__)

class S3Service:
    """Service for handling S3 operations."""
    
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str = 'us-east-1'):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.s3 = None
        self._setup_s3()
    
    def _setup_s3(self):
        """Setup S3 client with optimized connection pool."""
        try:
            boto3.setup_default_session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
            
            # Configure S3 client with increased connection pool for high-throughput uploads
            config = Config(
                region_name=self.region_name,
                retries={'max_attempts': 3, 'mode': 'adaptive'},
                max_pool_connections=50,  # Increased from default 10 to 50
                connect_timeout=60,
                read_timeout=60
            )
            
            self.s3 = boto3.client('s3', config=config)
            self.s3.list_buckets()  # Test connection
            logger.info("S3 client initialized successfully with optimized connection pool (50 connections)")
        except Exception as e:
            logger.error(f"Failed to setup S3 client: {e}")
            raise
    
    def list_files_in_folder(self, bucket: str, folder: str) -> List[str]:
        """
        List all supported files in a given S3 folder (PDF, DOC, DOCX, TXT).
        
        Args:
            bucket: S3 bucket name
            folder: Folder prefix
            
        Returns:
            List of file keys
        """
        try:
            files = []
            paginator = self.s3.get_paginator('list_objects_v2')
            
            supported_extensions = {'.pdf', '.doc', '.docx', '.txt'}
            
            for page in paginator.paginate(Bucket=bucket, Prefix=folder):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    extension = os.path.splitext(key)[1].lower()
                    if extension in supported_extensions:
                        files.append(key)
            
            return files
            
        except Exception as e:
            logger.error(f"Error listing files in folder {folder}: {e}")
            raise
    
    def get_object(self, bucket: str, key: str) -> bytes:
        """
        Get object from S3 with enhanced error handling and logging.
        
        Args:
            bucket: S3 bucket name
            key: Object key (URL-decoded)
            
        Returns:
            Object bytes if found, None otherwise
        """
        try:
            # First try with the exact key
            try:
                response = self.s3.get_object(Bucket=bucket, Key=key)
                logger.debug(f"Successfully retrieved {key} from {bucket}")
                return response['Body'].read()
            except self.s3.exceptions.NoSuchKey:
                logger.warning(f"File not found with key: {key}")
                return None
            except self.s3.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    logger.warning(f"File not found (NoSuchKey): {key}")
                    return None
                # For other client errors, log and re-raise
                logger.error(f"S3 ClientError for key {key}: {str(e)}")
                raise
                
        except Exception as e:
            logger.error(f"Unexpected error getting object {key}: {str(e)}")
            raise
    
    def put_object(self, bucket: str, key: str, body: bytes) -> bool:
        """
        Put object to S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            body: Object bytes
            
        Returns:
            True if successful
        """
        try:
            self.s3.put_object(Bucket=bucket, Key=key, Body=body)
            logger.debug(f"Successfully saved to S3: {bucket}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to S3 {key}: {e}")
            return False
    
    def copy_object(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """
        Copy object within S3.
        
        Args:
            source_bucket: Source bucket
            source_key: Source key
            dest_bucket: Destination bucket
            dest_key: Destination key
            
        Returns:
            True if successful
        """
        try:
            self.s3.copy_object(
                Bucket=dest_bucket,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Key=dest_key
            )
            return True
            
        except Exception as e:
            logger.error(f"Error copying object: {e}")
            return False
    
    def delete_object(self, bucket: str, key: str) -> bool:
        """
        Delete object from S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            
        Returns:
            True if successful
        """
        try:
            self.s3.delete_object(Bucket=bucket, Key=key)
            return True
            
        except Exception as e:
            logger.error(f"Error deleting object: {e}")
            return False
    
    def object_exists(self, bucket: str, key: str) -> bool:
        """
        Check if object exists in S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            
        Returns:
            True if exists
        """
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
            
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
        except Exception as e:
            logger.error(f"Error checking object existence: {e}")
            return False
