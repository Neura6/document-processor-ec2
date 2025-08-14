#!/bin/bash
# PDF Pipeline Live Monitoring Script

echo "=== PDF PIPELINE LIVE MONITORING ==="
echo "Time: $(date)"
echo "=================================="

echo ""
echo "📊 FILES PROCESSED:"
curl -s http://localhost:8001/metrics | grep "pdf_files_processed_total.*SUCCESS" | tail -5

echo ""
echo "⏱️ PROCESSING TIMES:"
curl -s http://localhost:8001/metrics | grep "pdf_processing_duration_seconds_sum" | tail -3

echo ""
echo "📁 S3 OPERATIONS:"
curl -s http://localhost:8001/metrics | grep "s3_operations_total.*success" | tail -3

echo ""
echo "🔄 KB SYNC STATUS:"
curl -s http://localhost:8001/metrics | grep "kb_sync_total.*SUCCESS" | tail -3

echo ""
echo "🎯 LIVE QUEUE STATUS:"
curl -s http://localhost:8001/metrics | grep "queue_check" | tail -1

echo ""
echo "=================================="
echo "Run './monitor.sh' anytime to see live stats"
