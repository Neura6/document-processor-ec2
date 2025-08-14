#!/bin/bash
# WORKING Grafana Dashboard Setup via API

echo "=== SETTING UP WORKING GRAFANA DASHBOARD ==="

# Create dashboard via API (bypass UI issues)
curl -X POST \
  http://admin:admin123@localhost:3000/api/dashboards/db \
  -H 'Content-Type: application/json' \
  -d '{
    "dashboard": {
      "id": null,
      "title": "PDF Pipeline Live Dashboard",
      "tags": ["pdf", "pipeline"],
      "timezone": "browser",
      "panels": [
        {
          "id": 1,
          "title": "Files Processed",
          "type": "stat",
          "targets": [
            {
              "expr": "sum(pdf_files_processed_total{status=\"SUCCESS\"})",
              "refId": "A"
            }
          ],
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
        },
        {
          "id": 2,
          "title": "Processing Duration",
          "type": "graph",
          "targets": [
            {
              "expr": "pdf_processing_duration_seconds_sum",
              "refId": "A"
            }
          ],
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
        },
        {
          "id": 3,
          "title": "S3 Uploads",
          "type": "stat",
          "targets": [
            {
              "expr": "sum(s3_operations_total{operation=\"upload\",status=\"success\"})",
              "refId": "A"
            }
          ],
          "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8}
        },
        {
          "id": 4,
          "title": "KB Sync Success",
          "type": "stat",
          "targets": [
            {
              "expr": "sum(kb_sync_total{status=\"SUCCESS\"})",
              "refId": "A"
            }
          ],
          "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8}
        }
      ],
      "time": {
        "from": "now-1h",
        "to": "now"
      },
      "refresh": "30s"
    }
  }'

echo "Dashboard created successfully!"
echo "Access dashboard at: http://50.17.112.242:3000/d/pdf-pipeline-live-dashboard"
