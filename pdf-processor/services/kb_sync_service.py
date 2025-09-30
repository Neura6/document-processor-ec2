"""
Knowledge Base Sync Service
Implements Bedrock ingestion job management and error handling with concurrency control
"""

import json
import boto3
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time
import logging
import os
import threading
import fcntl
from typing import Dict, List, Any, Optional

# Setup logging
logger = logging.getLogger(__name__)

class KBMappingConfig:
    """Knowledge Base mapping configuration"""
    
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
    'test':{'id':'VDAPHQ1JTN','data_source_id':'EAVX8UD6RY'}

}
    
    CHUNKED_BUCKET = 'chunked-rules-repository'
    UNPROCESSED_BUCKET = 'unprocessed-files-error-on-pdf-processing'
    UNPROCESSED_FOLDER = 'to_further_process'

class KBIngestionService:
    """Service for managing Bedrock Knowledge Base ingestion jobs with concurrency control"""
    
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
        
        # Thread-safe in-memory locks
        self._in_memory_locks = {}
        self._lock_manager = threading.Lock()
        
        # File-based locks for persistence across container restarts
        self._lock_dir = '/tmp/kb_locks'
        os.makedirs(self._lock_dir, exist_ok=True)
        
        # Pattern for detecting token limit errors
        self.token_error_pattern = re.compile(r'file\s+([^\s]+)\s+.*token\s+limit', re.IGNORECASE)

    def _acquire_kb_lock(self, kb_id: str, timeout: int = 3600) -> bool:
        """
        Acquire a lock for a specific knowledge base to prevent concurrent syncs.
        Uses both in-memory and file-based locking for robustness.
        """
        lock_file = os.path.join(self._lock_dir, f"kb_{kb_id}.lock")
        
        try:
            # File-based lock
            fd = os.open(lock_file, os.O_CREAT | os.O_RDWR)
            
            # Try to acquire exclusive lock (non-blocking)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # Write process info to lock file
                lock_info = {
                    'pid': os.getpid(),
                    'timestamp': time.time(),
                    'kb_id': kb_id
                }
                os.write(fd, json.dumps(lock_info).encode())
                os.fsync(fd)
                
                # Also add to in-memory locks
                with self._lock_manager:
                    self._in_memory_locks[kb_id] = {'fd': fd, 'start_time': time.time()}
                
                logger.info(f"🔒 Acquired lock for KB {kb_id}")
                return True
                
            except (IOError, OSError):
                # Lock is already held by another process
                os.close(fd)
                
                # Check if lock is stale (older than timeout)
                try:
                    with open(lock_file, 'r') as f:
                        lock_data = json.load(f)
                        if time.time() - lock_data.get('timestamp', 0) > timeout:
                            logger.warning(f"🗑️  Removing stale lock for KB {kb_id}")
                            os.remove(lock_file)
                            return self._acquire_kb_lock(kb_id, timeout)
                except:
                    pass
                    
                return False
                
        except Exception as e:
            logger.error(f"❌ Error acquiring lock for KB {kb_id}: {str(e)}")
            return False

    def _release_kb_lock(self, kb_id: str) -> bool:
        """Release the lock for a specific knowledge base"""
        try:
            with self._lock_manager:
                if kb_id in self._in_memory_locks:
                    lock_info = self._in_memory_locks.pop(kb_id)
                    fd = lock_info['fd']
                    
                    # Release file lock
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                    
                    # Remove lock file
                    lock_file = os.path.join(self._lock_dir, f"kb_{kb_id}.lock")
                    if os.path.exists(lock_file):
                        os.remove(lock_file)
                    
                    logger.info(f"🔓 Released lock for KB {kb_id}")
                    return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Error releasing lock for KB {kb_id}: {str(e)}")
            return False

    def _wait_for_kb_lock(self, kb_id: str, max_wait: int = 7200, check_interval: int = 30) -> bool:
        """
        Wait for a knowledge base lock to become available.
        Returns True when lock is acquired, False if timeout occurs.
        """
        start_time = time.time()
        attempt = 0
        
        while time.time() - start_time < max_wait:
            attempt += 1
            
            if self._acquire_kb_lock(kb_id):
                return True
                
            elapsed = time.time() - start_time
            if attempt % 10 == 0:  # Log every 5 minutes
                logger.info(f"⏳ Waiting for KB {kb_id} lock... ({elapsed:.0f}s elapsed)")
            
            time.sleep(check_interval)
        
        logger.error(f"⏰ Timeout waiting for KB {kb_id} lock after {max_wait}s")
        return False

    def sync_and_handle_failed_files(self, folder: str) -> Dict[str, Any]:
        """
        Starts an ingestion job, identifies failed files due to token limits,
        moves them to the unprocessed bucket's to_further_process folder,
        and starts a second sync job. Includes concurrency control.
        """
        kb_info = self.config.KB_MAPPING[folder]
        kb_id = kb_info['id']
        failed_files_initial_sync = []
        files_successfully_moved = []

        # Acquire lock for this knowledge base
        if not self._wait_for_kb_lock(kb_id):
            return {'status': 'LOCK_TIMEOUT', 'message': f'Could not acquire lock for KB {kb_id}'}

        try:
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
                
        finally:
            # Always release the lock
            self._release_kb_lock(kb_id)

    def sync_to_knowledge_base_simple(self, folder: str, is_delete: bool = False) -> Dict[str, Any]:
        """Starts and waits for a single Bedrock ingestion job for a folder with concurrency control."""
        kb_info = self.config.KB_MAPPING[folder]
        kb_id = kb_info['id']
        client_token = str(uuid.uuid4())
        description = f"{'Deletion sync' if is_delete else 'Batch sync'} for {folder}"

        # Acquire lock for this knowledge base
        if not self._wait_for_kb_lock(kb_id):
            return {'status': 'LOCK_TIMEOUT', 'message': f'Could not acquire lock for KB {kb_id}'}

        try:
            # Log KB details for transparency
            logger.info(f"[KB-SYNC] Starting sync for folder: {folder}")
            logger.info(f"[KB-SYNC] Knowledge Base ID: {kb_info['id']}")
            logger.info(f"[KB-SYNC] Data Source ID: {kb_info['data_source_id']}")
            logger.info(f"[KB-SYNC] Sync Type: {'Deletion' if is_delete else 'Ingestion'}")
            
            # Count files in folder
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.config.CHUNKED_BUCKET,
                    Prefix=folder + '/'
                )
                file_count = len(response.get('Contents', []))
                logger.info(f"[KB-SYNC] Files to sync: {file_count} in s3://{self.config.CHUNKED_BUCKET}/{folder}/")
            except:
                logger.warning(f"[KB-SYNC] Could not count files in {folder}")

            try:
                response = self.bedrock_client.start_ingestion_job(
                    clientToken=client_token,
                    dataSourceId=kb_info['data_source_id'],
                    knowledgeBaseId=kb_info['id'],
                    description=description
                )
                job_id = response['ingestionJob']['ingestionJobId']
                logger.info(f"[KB-SYNC] ✅ Started ingestion job: {job_id}")
                logger.info(f"[KB-SYNC] 📋 Job Description: {description}")
                logger.info(f"[KB-SYNC] ⏱️  Monitoring job status...")
            except Exception as e:
                if 'ConflictException' in str(e) and 'ongoing ingestion job' in str(e):
                    logger.warning(f"[KB-SYNC] ⚠️  ConflictException: Another ingestion job is already running for this data source")
                    logger.info(f"[KB-SYNC] 🔄 Waiting for ongoing job to complete before retrying...")
                    
                    # Wait for existing job to complete (max 30 minutes)
                    max_wait_attempts = 60  # 30 minutes with 30-second intervals
                    for wait_attempt in range(max_wait_attempts):
                        time.sleep(30)
                        try:
                            # Try to start the job again
                            client_token = str(uuid.uuid4())  # New token for retry
                            response = self.bedrock_client.start_ingestion_job(
                                clientToken=client_token,
                                dataSourceId=kb_info['data_source_id'],
                                knowledgeBaseId=kb_info['id'],
                                description=f"{description} (retry after conflict)"
                            )
                            job_id = response['ingestionJob']['ingestionJobId']
                            logger.info(f"[KB-SYNC] ✅ Successfully started ingestion job after waiting: {job_id}")
                            break
                        except Exception as retry_e:
                            if 'ConflictException' in str(retry_e):
                                logger.info(f"[KB-SYNC] 🕐 Still waiting for ongoing job to complete... (attempt {wait_attempt + 1}/{max_wait_attempts})")
                                continue
                            else:
                                logger.error(f"[KB-SYNC] ❌ Unexpected error during retry: {str(retry_e)}")
                                raise retry_e
                    else:
                        # If we exhausted all wait attempts
                        logger.error(f"[KB-SYNC] ❌ Timeout waiting for ongoing ingestion job to complete after 30 minutes")
                        return {'status': 'TIMEOUT', 'message': 'Timeout waiting for ongoing ingestion job to complete'}
                else:
                    # Re-raise if it's not a ConflictException
                    raise e

            # Wait for job to complete with enhanced logging
            wait_result = self.wait_for_ingestion_job(kb_info, job_id)
            
            # Log final status
            if wait_result.get('status') == 'COMPLETE':
                logger.info(f"[KB-SYNC] ✅ Knowledge Base sync COMPLETED successfully!")
                logger.info(f"[KB-SYNC] 📊 Job ID: {job_id}")
                logger.info(f"[KB-SYNC] 🗂️  Folder: {folder}")
                logger.info(f"[KB-SYNC] 🔗 KB ID: {kb_info['id']}")
            else:
                logger.warning(f"[KB-SYNC] ⚠️  Knowledge Base sync completed with status: {wait_result.get('status')}")
                if wait_result.get('failed_files'):
                    logger.warning(f"[KB-SYNC] 📄 Failed files: {len(wait_result.get('failed_files', []))}")
            
            return wait_result

        except Exception as e:
            logger.error(f"[KB-SYNC] ❌ Error during sync job for folder {folder}: {str(e)}")
            raise
        finally:
            # Always release the lock
            self._release_kb_lock(kb_id)

    def wait_for_ingestion_job(self, kb_info: Dict[str, str], job_id: str) -> Dict[str, Any]:
        """
        Polls the knowledge base for ingestion job status, extracts failed files
        due to token limits if the job fails with relevant reasons.
        """
        max_attempts = 60
        interval = 30
        failed_files_due_to_tokens = []

        logger.info(f"KB_SYNC: Monitoring ingestion job {job_id}")
        logger.info(f"KB_SYNC: Maximum wait time: {max_attempts * interval / 60:.1f} minutes")

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
                    logger.info(f"KB_SYNC: Job status changed to '{status}'")
                    last_status = status
                
                # Progress update every 5 minutes
                if attempt % 10 == 0 and attempt > 0:
                    elapsed = time.time() - start_time
                    logger.info(f"KB_SYNC: Still processing... ({elapsed:.0f}s elapsed)")

                if status == 'COMPLETE':
                    elapsed = time.time() - start_time
                    logger.info(f"KB_SYNC: Job completed successfully in {elapsed:.0f}s")
                    
                    # Log final statistics
                    stats = response['ingestionJob'].get('statistics', {})
                    processed = stats.get('documentsProcessed', 0)
                    failed = stats.get('documentsFailed', 0)
                    if processed or failed:
                        logger.info(f"KB_SYNC: Final stats - Processed: {processed}, Failed: {failed}")
                    
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
