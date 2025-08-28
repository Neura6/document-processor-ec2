"""
Knowledge Base Sync Service
Manages Bedrock ingestion jobs with error handling and failed file recovery.
"""

import json
import boto3
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time
import logging
import os
from typing import Dict, List, Any, Optional

# Configure logging
logger = logging.getLogger(__name__)

# Unprocessed file storage configuration
UNPROCESSED_BUCKET = 'unprocessed-files-error-on-pdf-processing'
UNPROCESSED_FOLDER = 'to_further_process'

class KBMappingConfig:
    """Knowledge Base mapping configuration for all document types."""
    
    KB_MAPPING = {
    'accounting-standards': {'id': '1XYVHJWXEA', 'data_source_id': 'VQ53AYFFGO'},
    'accounting-global': {'id': 'BPFC6I50NY', 'data_source_id': 'C7NTSCZJPA'},
    'commercial-laws': {'id': 'CAQ6E0JJBW', 'data_source_id': 'AY5EWVXYF6'},
    'usecase-reports': {'id': 'WHF3OXB1MQ', 'data_source_id': 'X4FZFSK7QC'},
    'Auditing Standards': {'id': 'XXFFTJYAD1', 'data_source_id': 'OH8L2PTHYU'},
    'Auditing-global': {'id': 'GQOXRJAYPO', 'data_source_id': 'WWPU0ELXBR'},
    'Banking Regulations': {'id': 'VDMWYPSOYO', 'data_source_id': 'I6QTPRZOSP'},
    'Banking Regulations-test': {'id': 'S0X541AD9P', 'data_source_id': 'CIHLQL7Q5E'},
    'Capital Market Regulations': {'id': 'UI4DH8O8GX', 'data_source_id': '8JKIFZD7HF'},
    'Direct Taxes': {'id': 'PV2IGEHKRK', 'data_source_id': 'XNBPX5WR3B'},
    'Indirect Taxes': {'id': 'QTTHYYFCZ9', 'data_source_id': 'JDT9KAXQJL'},
    'Insurance': {'id': 'ECWHGFSH1R', 'data_source_id': 'LPBCRYCSEM'},
    'Labour Law': {'id': 'CAQ6E0JJBW', 'data_source_id': 'PLIPBARA5R'},
    'Finance Tools': {'id': '1XYVHJWXEA', 'data_source_id': '18HMESLJIY'},
    'GIFT City': {'id': 'VDMWYPSOYO', 'data_source_id': 'RL7KXCTETU'},
    'usecase-reports-2': {'id': 'WHF3OXB1MQ', 'data_source_id': 'DVNHB5JJT7'},
    'Direct-Taxes-case-laws': {'id': 'UJNOWHQEQ9', 'data_source_id': '9OUZYLFVCV'},
    'Indirect-Taxes-case-laws': {'id': 'PMF8OY8ZSG', 'data_source_id': 'WRRCYQD3R0'},
    'Insurance-caselaws': {'id': 'IUB7BI5IXQ', 'data_source_id': 'CB2LPPFQUW'},
    'usecase-reports-4': {'id': 'WHF3OXB1MQ', 'data_source_id': 'CN63WXZKZW'},
    'commercial-case-laws': {'id': 'DQ6AIARMNQ', 'data_source_id': 'NXNCQID5NX'},

}
    
    CHUNKED_BUCKET = 'chunked-rules-repository'
    UNPROCESSED_BUCKET = 'unprocessed-files-error-on-pdf-processing'
    UNPROCESSED_FOLDER = 'to_further_process'

class KBIngestionService:
    """Service for managing Bedrock Knowledge Base ingestion jobs with error recovery."""
    
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str = 'us-east-1'):
        """Initialize KB sync service with AWS credentials."""
        import boto3
        from .s3_utils import S3Utils
        
        # Initialize Bedrock agent client
        self.bedrock_client = boto3.client(
            'bedrock-agent',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.s3_utils = S3Utils(aws_access_key_id, aws_secret_access_key, region_name)
        self.config = KBMappingConfig()
    
    def wait_for_ingestion_job(self, kb_info: Dict[str, str], job_id: str) -> Dict[str, Any]:
        """
        Polls the knowledge base for ingestion job status, extracts failed files
        due to token limits if the job fails with relevant reasons.
        """
        max_attempts = 60  # Maximum polling attempts (30 minutes)
        interval = 30      # Polling interval in seconds
        failed_files_due_to_tokens = []
        # Pattern to extract file paths from token error messages
        token_error_pattern = re.compile(r"Issue occurred while processing file: (.*?\.\w+)\.")

        logger.info(f"Monitoring ingestion job {job_id} for completion")

        for attempt in range(max_attempts):
            try:
                response = self.bedrock_client.get_ingestion_job(
                    dataSourceId=kb_info['data_source_id'],
                    ingestionJobId=job_id,
                    knowledgeBaseId=kb_info['id']
                )
                status = response['ingestionJob']['status']
                failure_reasons = response['ingestionJob'].get('failureReasons', [])

                if status == 'COMPLETE':
                    logger.info(f"Ingestion job {job_id} completed successfully")
                    return {'status': 'COMPLETE', 'failed_files': failed_files_due_to_tokens}

                elif status == 'FAILED':
                    logger.error(f"Ingestion job {job_id} failed: {failure_reasons}")

                    # Extract files that failed due to token limits
                    for reason in failure_reasons:
                        if isinstance(reason, str):
                            match = token_error_pattern.search(reason)
                            if match:
                                failed_file = match.group(1)
                                if failed_file not in failed_files_due_to_tokens:
                                    failed_files_due_to_tokens.append(failed_file)
                                    logger.warning(f"Token limit exceeded for file: {failed_file}")

                    # Return failed files if token errors detected
                    if failed_files_due_to_tokens:
                        logger.warning(f"Job {job_id} failed with token errors, returning failed files")
                        return {'status': 'FAILED_TOKEN_ERROR', 'failed_files': failed_files_due_to_tokens}
                    else:
                        return {'status': 'FAILED_OTHER_ERROR', 'message': f'Ingestion job failed: {failure_reasons}'}

                logger.info(f"Job {job_id} status: {status}, continuing to monitor")
                time.sleep(interval)

            except Exception as e:
                logger.error(f"Error checking ingestion job status for job {job_id}: {str(e)}")
                if failed_files_due_to_tokens:
                    logger.warning(f"Polling error for job {job_id}, returning identified failed files")
                    return {'status': 'ERROR_DURING_WAIT', 'failed_files': failed_files_due_to_tokens, 'polling_error': str(e)}
                else:
                    raise

        # Handle timeout scenario
        logger.error(f"Job {job_id} timed out after {max_attempts} attempts")
        if failed_files_due_to_tokens:
            logger.warning(f"Job {job_id} timed out, returning identified failed files")
            return {'status': 'TIMEOUT_TOKEN_ERROR', 'failed_files': failed_files_due_to_tokens, 'timeout': True}
        else:
            raise Exception(f"Ingestion job {job_id} timed out after {max_attempts} attempts")
        
    def sync_and_handle_failed_files(self, folder: str) -> Dict[str, Any]:
        """
        Execute ingestion job and handle files that fail due to token limits.
        
        Args:
            folder: Document folder name to sync
            
        Returns:
            Dict containing sync status and any moved files
        """
        kb_info = self.config.KB_MAPPING[folder]
        failed_files_initial_sync = []
        files_successfully_moved = []

        # Execute initial ingestion job
        job_id_step1 = None
        try:
            client_token_step1 = str(uuid.uuid4())
            description_step1 = f"Initial batch sync for {folder}"
            response = self.bedrock_client.start_ingestion_job(
                clientToken=client_token_step1,
                dataSourceId=kb_info['data_source_id'],
                knowledgeBaseId=kb_info['id'],
                description=description_step1
            )
            job_id_step1 = response['ingestionJob']['ingestionJobId']
            logger.info(f"Started ingestion job {job_id_step1} for folder: {folder}")

            wait_result_step1 = self.wait_for_ingestion_job(kb_info, job_id_step1)

            if 'failed_files' in wait_result_step1 and wait_result_step1['failed_files']:
                failed_files_initial_sync.extend(wait_result_step1['failed_files'])
                logger.warning(f"Token limit failures detected: {len(failed_files_initial_sync)} files")

        except Exception as e:
            logger.error(f"Error during initial sync job start or wait for folder {folder}: {str(e)}")
            return {'status': 'Error', 'message': f"Sync job failed to start or complete: {str(e)}"}

        # Move failed files to unprocessed bucket
        if failed_files_initial_sync:
            logger.info(f"Moving {len(failed_files_initial_sync)} failed files to unprocessed storage")
            
            for failed_file_key in failed_files_initial_sync:
                # Generate destination path for unprocessed file
                destination_key = f"{UNPROCESSED_FOLDER}/{os.path.basename(failed_file_key)}"
                
                # Move problematic file to unprocessed bucket
                move_success = self.s3_utils.move_s3_object(
                    self.config.CHUNKED_BUCKET, 
                    failed_file_key, 
                    UNPROCESSED_BUCKET, 
                    destination_key
                )
                
                if move_success:
                    files_successfully_moved.append(destination_key)
                    logger.info(f"Moved to unprocessed: {failed_file_key}")
                else:
                    logger.error(f"Failed to move: {failed_file_key}")

        # Return final sync results
        if files_successfully_moved:
            logger.warning(f"Sync completed for {folder} with {len(files_successfully_moved)} files moved to unprocessed storage")
            return {'status': 'Completed with Failed Files', 'files_moved_to_unprocessed': files_successfully_moved}
        else:
            logger.info(f"Sync completed successfully for folder: {folder}")
            return {'status': 'COMPLETE'}

    def sync_to_knowledge_base_simple(self, folder: str, is_delete: bool = False) -> Dict[str, Any]:
        """
        Execute a simple ingestion job for a folder.
        
        Args:
            folder: Document folder name to sync
            is_delete: Whether this is a deletion operation
            
        Returns:
            Dict containing sync status and results
        """
        kb_info = self.config.KB_MAPPING[folder]
        client_token = str(uuid.uuid4())
        description = f"{'Deletion sync' if is_delete else 'Batch sync'} for {folder}"

        try:
            # Log knowledge base configuration
            logger.info(f"Starting KB sync for folder: {folder}")
            logger.info(f"Knowledge Base ID: {kb_info['id']}")
            logger.info(f"Data Source ID: {kb_info['data_source_id']}")
            logger.info(f"Sync Type: {'Deletion' if is_delete else 'Ingestion'}")
            
            # Count files to be processed
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.config.CHUNKED_BUCKET,
                    Prefix=folder + '/'
                )
                file_count = len(response.get('Contents', []))
                logger.info(f"Files to process: {file_count} in s3://{self.config.CHUNKED_BUCKET}/{folder}/")
            except Exception as e:
                logger.warning(f"Could not count files in folder {folder}: {str(e)}")

            response = self.bedrock_client.start_ingestion_job(
                clientToken=client_token,
                dataSourceId=kb_info['data_source_id'],
                knowledgeBaseId=kb_info['id'],
                description=description
            )
            job_id = response['ingestionJob']['ingestionJobId']
            logger.info(f"Starting ingestion job for {folder}")
            logger.info(f"Job ID: {job_id}")

            # Monitor job completion
            wait_result = self.wait_for_ingestion_job(kb_info, job_id)
            
            # Log final status
            if wait_result.get('status') == 'COMPLETE':
                logger.info(f"Knowledge Base sync completed successfully for {folder}")
            else:
                logger.warning(f"Knowledge Base sync completed with status: {wait_result.get('status')}")
                if wait_result.get('failed_files'):
                    logger.warning(f"Failed files: {len(wait_result.get('failed_files', []))}")
            
            return wait_result

        except Exception as e:
            logger.error(f"Sync job error for folder {folder}: {str(e)}")
            raise

    def wait_for_ingestion_job(self, kb_info: Dict[str, str], job_id: str) -> Dict[str, Any]:
        """
        Polls the knowledge base for ingestion job status, extracts failed files
        due to token limits if the job fails with relevant reasons.
        """
        max_attempts = 60
        interval = 30
        failed_files_due_to_tokens = []

        logger.info(f"Monitoring ingestion job {job_id}")
        logger.info(f"Maximum wait time: {max_attempts * interval / 60:.1f} minutes")

        start_time = time.time()
        last_status = None

        for attempt in range(max_attempts):
            try:
                response = self.bedrock_client.get_ingestion_job(
                    dataSourceId=kb_info['data_source_id'],
                    ingestionJobId=job_id,
                    knowledgeBaseId=kb_info['id']
                )
                status = response['ingestionJob']['status']
                failure_reasons = response['ingestionJob'].get('failureReasons', [])
                
                # Log status changes
                if status != last_status:
                    logger.info(f"Job status changed to: {status}")
                    last_status = status
                
                # Progress update every 5 minutes
                if attempt % 10 == 0 and attempt > 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Still processing, elapsed time: {elapsed:.0f}s")

                if status == 'COMPLETE':
                    elapsed = time.time() - start_time
                    logger.info(f"Job completed successfully in {elapsed:.0f}s")
                    
                    # Log final statistics
                    stats = response['ingestionJob'].get('statistics', {})
                    processed = stats.get('documentsProcessed', 0)
                    failed = stats.get('documentsFailed', 0)
                    if processed or failed:
                        logger.info(f"Final stats - Processed: {processed}, Failed: {failed}")
                    
                    return {'status': 'COMPLETE', 'failed_files': failed_files_due_to_tokens, 'duration': elapsed}

                elif status == 'FAILED':
                    elapsed = time.time() - start_time
                    logger.error(f"KB_SYNC: Job failed after {elapsed:.0f}s")
                    logger.error(f"KB_SYNC: Failure reasons: {failure_reasons}")

                    # Check for token limit errors
                    for reason in failure_reasons:
                        if isinstance(reason, str):
                            match = self.token_error_pattern.search(reason)
                            if match:
                                failed_file = match.group(1)
                                if failed_file not in failed_files_due_to_tokens:
                                    failed_files_due_to_tokens.append(failed_file)
                                    logger.warning(f"KB_SYNC: Token limit exceeded for file: {failed_file}")

                    if failed_files_due_to_tokens:
                        logger.warning(f"KB_SYNC: {len(failed_files_due_to_tokens)} files failed due to token limits")
                        return {'status': 'FAILED_TOKEN_ERROR', 'failed_files': failed_files_due_to_tokens, 'duration': elapsed}
                    else:
                        return {'status': 'FAILED_OTHER_ERROR', 'message': f'Ingestion job failed: {failure_reasons}', 'duration': elapsed}

                elif status == 'IN_PROGRESS':
                    # Show progress every 5 minutes
                    if attempt % 10 == 0:
                        logger.info(f"[KB-SYNC] 🔄 Job {job_id} is IN_PROGRESS... ({attempt+1}/{max_attempts})")

                time.sleep(interval)

            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"[KB-SYNC] 🔥 Error checking job status for {job_id}: {str(e)} (after {elapsed:.0f}s)")
                if failed_files_due_to_tokens:
                    return {'status': 'ERROR_DURING_WAIT', 'failed_files': failed_files_due_to_tokens, 'polling_error': str(e), 'duration': elapsed}
                else:
                    raise

        # Timeout
        elapsed = time.time() - start_time
        logger.error(f"[KB-SYNC] ⏰ TIMEOUT! Job {job_id} timed out after {elapsed:.0f}s ({max_attempts} attempts)")
        if failed_files_due_to_tokens:
            return {'status': 'TIMEOUT_TOKEN_ERROR', 'failed_files': failed_files_due_to_tokens, 'timeout': True, 'duration': elapsed}
        else:
            raise Exception(f"Ingestion job {job_id} timed out after {max_attempts} attempts")

    def move_s3_object(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """Move S3 object from source to destination"""
        try:
            # Copy object
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
            
            # Delete original
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            
            logger.info(f"Successfully moved s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error moving S3 object: {str(e)}")
            return False

    def delete_s3_object(self, bucket: str, key: str) -> bool:
        """Delete S3 object"""
        try:
            self.s3.delete_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            logger.error(f"Error deleting S3 object: {str(e)}")
            return False

    def get_kb_mapping(self) -> Dict[str, Dict[str, str]]:
        """Get the KB mapping configuration"""
        return self.config.KB_MAPPING
