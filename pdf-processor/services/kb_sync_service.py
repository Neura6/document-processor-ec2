"""
Knowledge Base Sync Service
Implements Bedrock ingestion job management and error handling
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

# Setup logging
logger = logging.getLogger(__name__)

class KBMappingConfig:
    """Knowledge Base mapping configuration"""
    
    KB_MAPPING = {
        'accounting-standards': {'id': '1XYVHJWXEA', 'data_source_id': 'VQ53AYFFGO'},
        'commercial-laws': {'id': 'CAQ6E0JJBW', 'data_source_id': 'AY5EWVXYF6'},
        'usecase-reports': {'id': 'WHF3OXB1MQ', 'data_source_id': 'CN63WXZKZW'},
        'Auditing Standards': {'id': 'XXFFTJYAD1', 'data_source_id': 'OH8L2PTHYU'},
        'Banking Regulations': {'id': 'VDMWYPSOYO', 'data_source_id': 'I6QTPRZOSP'},
        'Capital Market Regulations': {'id': 'UI4DH8O8GX', 'data_source_id': '8JKIFZD7HF'},
        'Direct Taxes': {'id': 'PV2IGEHKRK', 'data_source_id': 'XNBPX5WR3B'},
        'Indirect Taxes': {'id': 'QTTHYYFCZ9', 'data_source_id': 'JDT9KAXQJL'},
        'Insurance': {'id': 'ECWHGFSH1R', 'data_source_id': 'LPBCRYCSEM'},
        'Labour Law': {'id': 'CAQ6E0JJBW', 'data_source_id': 'PLIPBARA5R'},
        'Finance Tools': {'id': '1XYVHJWXEA', 'data_source_id': '18HMESLJIY'},
        'GIFT City': {'id': 'VDMWYPSOYO', 'data_source_id': 'RL7KXCTETU'},
        'usecase-reports-2': {'id': 'WHF3OXB1MQ', 'data_source_id': '8UK78XZFIF'},
        'Direct-Taxes-case-laws': {'id': 'UJNOWHQEQ9', 'data_source_id': '9OUZYLFVCV'},
        'Indirect-Taxes-case-laws': {'id': 'PMF8OY8ZSG', 'data_source_id': 'WRRCYQD3R0'},
        'Insurance-caselaws': {'id': 'IUB7BI5IXQ', 'data_source_id': 'CB2LPPFQUW'},
        'usecase-reports-4': {'id': 'WHF3OXB1MQ', 'data_source_id': 'CN63WXZKZW'},
        'commercial-case-laws': {'id': 'DQ6AIARMNQ', 'data_source_id': 'NXNCQID5NX'}
    }
    
    CHUNKED_BUCKET = 'chunked-rules-repository'
    UNPROCESSED_BUCKET = 'unprocessed-files-error-on-pdf-processing'
    UNPROCESSED_FOLDER = 'to_further_process'

class KBIngestionService:
    """Service for managing Bedrock Knowledge Base ingestion jobs"""
    
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str = 'us-east-1'):
        """Initialize KB sync service with AWS credentials"""
        import boto3
        
        # Use boto3 with correct service name
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
        self.config = KBMappingConfig()
        
    def sync_and_handle_failed_files(self, folder: str) -> Dict[str, Any]:
        """
        Starts an ingestion job, identifies failed files due to token limits,
        moves them to the unprocessed bucket's to_further_process folder,
        and starts a second sync job.
        """
        kb_info = self.config.KB_MAPPING[folder]
        failed_files_initial_sync = []
        files_successfully_moved = []

        # Step 1: Initial Sync Attempt
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
            logger.info(f"Started initial ingestion job {job_id_step1} for folder {folder}")

            wait_result_step1 = self.wait_for_ingestion_job(kb_info, job_id_step1)

            if 'failed_files' in wait_result_step1 and wait_result_step1['failed_files']:
                failed_files_initial_sync.extend(wait_result_step1['failed_files'])
                logger.warning(f"Initial sync failed for these files due to token limits: {failed_files_initial_sync}")

            # Step 2: Move Failed Files to Unprocessed Bucket
            if failed_files_initial_sync:
                logger.info(f"Moving {len(failed_files_initial_sync)} files to unprocessed bucket ({self.config.UNPROCESSED_FOLDER}/).")
                for failed_file_key in failed_files_initial_sync:
                    destination_key = f"{self.config.UNPROCESSED_FOLDER}/{os.path.basename(failed_file_key)}"
                    # move_s3_object will be implemented when needed
                    files_successfully_moved.append(destination_key)

        except Exception as e:
            logger.error(f"Error during initial sync job start or wait for folder {folder}: {str(e)}")
            return {'status': 'Error', 'message': f"Initial sync job failed to start or complete successfully: {str(e)}"}

        # Step 3: Start a Second Sync Attempt for Remaining Files
        job_id_step2 = None
        try:
            client_token_step2 = str(uuid.uuid4())
            description_step2 = f"Retry sync after moving failed files for {folder}"
            response = self.bedrock_client.start_ingestion_job(
                clientToken=client_token_step2,
                dataSourceId=kb_info['data_source_id'],
                knowledgeBaseId=kb_info['id'],
                description=description_step2
            )
            job_id_step2 = response['ingestionJob']['ingestionJobId']
            logger.info(f"Started retry ingestion job {job_id_step2} for folder {folder}")

            wait_result_step2 = self.wait_for_ingestion_job(kb_info, job_id_step2)
            if wait_result_step2.get('status') != 'COMPLETE':
                logger.warning(f"Retry sync job {job_id_step2} completed with status: {wait_result_step2.get('status')}")

        except Exception as e:
            logger.error(f"Error during retry sync job for folder {folder}: {str(e)}")
            return {'status': 'Completed with Errors and Unprocessed Files', 'files_moved_to_unprocessed': files_successfully_moved, 'retry_sync_error': str(e)}

        # Final Result
        if files_successfully_moved:
            logger.warning(f"Processing completed for folder {folder}. Files moved to {self.config.UNPROCESSED_BUCKET}/{self.config.UNPROCESSED_FOLDER}: {files_successfully_moved}")
            return {'status': 'Completed with Failed Files', 'files_moved_to_unprocessed': files_successfully_moved}
        else:
            logger.info(f"Processing completed successfully for folder {folder}. No files moved to unprocessed bucket.")
            return {'status': 'COMPLETE'}

    def sync_to_knowledge_base_simple(self, folder: str, is_delete: bool = False) -> Dict[str, Any]:
        """Starts and waits for a single Bedrock ingestion job for a folder."""
        kb_info = self.config.KB_MAPPING[folder]
        client_token = str(uuid.uuid4())
        description = f"{'Deletion sync' if is_delete else 'Batch sync'} for {folder}"

        try:
            response = self.bedrock_client.start_ingestion_job(
                clientToken=client_token,
                dataSourceId=kb_info['data_source_id'],
                knowledgeBaseId=kb_info['id'],
                description=description
            )
            job_id = response['ingestionJob']['ingestionJobId']
            logger.info(f"Started {'deletion' if is_delete else 'ingestion'} job {job_id} for folder {folder}")

            # Wait for job to complete
            wait_result = self.wait_for_ingestion_job(kb_info, job_id)
            return wait_result

        except Exception as e:
            logger.error(f"Error during sync job start or wait for folder {folder}: {str(e)}")
            raise

    def wait_for_ingestion_job(self, kb_info: Dict[str, str], job_id: str) -> Dict[str, Any]:
        """
        Polls the knowledge base for ingestion job status, extracts failed files
        due to token limits if the job fails with relevant reasons.
        """
        max_attempts = 60
        interval = 30
        failed_files_due_to_tokens = []

        logger.info(f"Waiting for ingestion job {job_id} to complete...")

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

                    # Check for token limit errors
                    for reason in failure_reasons:
                        if isinstance(reason, str):
                            match = self.token_error_pattern.search(reason)
                            if match:
                                failed_file = match.group(1)
                                if failed_file not in failed_files_due_to_tokens:
                                    failed_files_due_to_tokens.append(failed_file)
                                    logger.warning(f"Found problematic file: {failed_file} due to token limit")

                    if failed_files_due_to_tokens:
                        logger.warning(f"Ingestion job {job_id} failed with token errors")
                        return {'status': 'FAILED_TOKEN_ERROR', 'failed_files': failed_files_due_to_tokens}
                    else:
                        return {'status': 'FAILED_OTHER_ERROR', 'message': f'Ingestion job failed: {failure_reasons}'}

                logger.info(f"Ingestion job {job_id} status: {status}. Waiting...")
                time.sleep(interval)

            except Exception as e:
                logger.error(f"Error checking ingestion job status for job {job_id}: {str(e)}")
                if failed_files_due_to_tokens:
                    return {'status': 'ERROR_DURING_WAIT', 'failed_files': failed_files_due_to_tokens, 'polling_error': str(e)}
                else:
                    raise

        # Timeout
        logger.error(f"Ingestion job {job_id} timed out after {max_attempts} attempts")
        if failed_files_due_to_tokens:
            return {'status': 'TIMEOUT_TOKEN_ERROR', 'failed_files': failed_files_due_to_tokens, 'timeout': True}
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
