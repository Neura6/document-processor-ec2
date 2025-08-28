"""
Metadata Service
Creates metadata files for chunked PDFs in S3 based on folder structure rules.
"""

import boto3
import json
import logging
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
import os

logger = logging.getLogger(__name__)

class MetadataService:
    """Service for creating metadata files for chunked PDFs."""
    
    # Lists for rule-based handling
    ONLY_COUNTRY_LIST = [
        'accounting-standards',
        'Capital Market Regulations',
        'Direct Taxes',
        'Indirect Taxes',
        'Insurance',
        'Labour Law',
        'Banking Regulations'
    ]
    
    def __init__(self):
        """Initialize the metadata service with S3 client."""
        self.s3_client = boto3.client('s3')
        self.logger = logging.getLogger(__name__)
    
    def create_metadata_file(self, bucket: str, key: str, metadata_dict: Dict) -> bool:
        """
        Create a metadata file for the given S3 object.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            metadata_dict: Dictionary containing metadata attributes
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            metadata = {"metadataAttributes": metadata_dict}
            metadata_key = f"{key}.metadata.json"

            # Always create or replace the metadata file
            self.s3_client.put_object(
                Bucket=bucket,
                Key=metadata_key,
                Body=json.dumps(metadata)
            )
            self.logger.info(f"Created metadata file: {metadata_key} with {metadata_dict}")
            return True

        except Exception as e:
            self.logger.error(f"Error creating metadata file for {key}: {str(e)}")
            return False
    
    def determine_metadata_attributes(self, document_type: str, country: str, complexity: Optional[str] = None) -> Dict:
        """
        Determine metadata attributes based on document type and folder structure.
        
        Args:
            document_type: Document type from folder structure
            country: Country from folder structure
            complexity: Complexity level from folder structure
            
        Returns:
            Dict: Metadata attributes to add
        """
        metadata_to_add = {}

        if document_type == 'accounting-global':
            if complexity:
                metadata_to_add = {"complexity": complexity}

        elif document_type == 'Banking Regulations-test':
            metadata_to_add = {"country": country}
            if complexity:
                metadata_to_add["complexity"] = complexity

        elif document_type in self.ONLY_COUNTRY_LIST:
            metadata_to_add = {"country": country}

        else:
            self.logger.info(f"No metadata rules matched for {document_type}")

        return metadata_to_add
    
    def create_metadata_for_file(self, s3_key: str, bucket: str = 'chunked-rules-repository') -> bool:
        """
        Create metadata file for a single chunked PDF file.
        
        Args:
            s3_key: S3 key of the chunked PDF file
            bucket: S3 bucket name
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Parse the S3 key to extract folder structure
            parts = s3_key.split('/')
            if len(parts) < 2:
                self.logger.warning(f"Invalid S3 key format: {s3_key}")
                return False
            
            document_type = parts[0]
            country = parts[1] if len(parts) > 1 else None
            complexity = parts[2] if len(parts) > 2 else None
            
            # Determine metadata attributes
            metadata_attributes = self.determine_metadata_attributes(document_type, country, complexity)
            
            if not metadata_attributes:
                self.logger.info(f"No metadata attributes to add for {s3_key}")
                return True
            
            # Create metadata file
            return self.create_metadata_file(bucket, s3_key, metadata_attributes)
            
        except Exception as e:
            self.logger.error(f"Error creating metadata for {s3_key}: {str(e)}")
            return False
    
    def generate_metadata_for_folder(self, s3_path: str, max_workers: int = 10) -> None:
        """
        Generate metadata for all files in a folder using concurrent threads.
        
        Args:
            s3_path: S3 path like s3://bucket/document-type/country/complexity/
            max_workers: Maximum number of concurrent workers
        """
        try:
            parsed = urlparse(s3_path)
            bucket = parsed.netloc
            prefix = parsed.path.lstrip('/')

            parts = prefix.strip('/').split('/')
            if len(parts) < 2:
                self.logger.error("Invalid path format. Expecting at least: bucket/document-type/country/")
                return

            document_type = parts[0]
            country = parts[1]
            complexity = parts[2] if len(parts) > 2 else None

            self.logger.info(f"Scanning S3 folder: bucket={bucket}, prefix={prefix}")
            self.logger.info(f"Extracted document_type={document_type}, country={country}, complexity={complexity}")

            # Determine metadata based on rules
            metadata_to_add = self.determine_metadata_attributes(document_type, country, complexity)

            if not metadata_to_add:
                self.logger.info("No metadata attributes to add. Exiting...")
                return

            # Get all files in the folder
            keys = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith('.metadata.json') or key.endswith('/'):
                        continue
                    keys.append(key)

            self.logger.info(f"Found {len(keys)} files. Starting concurrent metadata creation...")

            # Create metadata files concurrently
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(self.create_metadata_file, bucket, key, metadata_to_add)
                    for key in keys
                ]
                for future in as_completed(futures):
                    future.result()  # Force exception raise if any

            self.logger.info("All metadata creation tasks completed.")
            
        except Exception as e:
            self.logger.error(f"Error generating metadata for folder {s3_path}: {str(e)}")
