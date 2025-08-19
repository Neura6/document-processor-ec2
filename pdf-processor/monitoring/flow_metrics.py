#!/usr/bin/env python3
"""
Flow metrics for PDF processing pipeline
Tracks: SQS queue → EC2 processing → KB sync
"""

from prometheus_client import Counter, Gauge, Histogram

# SQS Queue Metrics
sqs_files_queued = Gauge('sqs_files_queued', 'Files currently in SQS queue')
sqs_files_added = Counter('sqs_files_added_total', 'Total files added to SQS queue')
sqs_files_processed = Counter('sqs_files_processed_total', 'Total files processed from SQS queue')

# EC2 Processing Metrics
ec2_files_processing = Gauge('ec2_files_processing', 'Files currently being processed by EC2')
ec2_files_completed = Counter('ec2_files_completed_total', 'Total files completed processing')

# Knowledge Base Sync Metrics
kb_files_synced = Counter('kb_files_synced_total', 'Total files synced to Knowledge Base')
kb_files_pending = Gauge('kb_files_pending_sync', 'Files pending KB sync')

# Processing flow metrics
processing_flow = Gauge('processing_flow_status', 'Current processing flow status', ['stage'])
