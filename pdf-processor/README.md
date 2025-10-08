# PDF Processing Pipeline - Enhanced SQS-Based System

## 🎯 Overview

This is a **production-ready, enterprise-grade PDF processing system** designed for EC2 deployment with comprehensive dual chunking capabilities, async processing, and advanced content extraction features.

## 🏗️ Architecture

### **Dual Chunking Strategy**
Every document is processed through **two parallel streams**:

```
📄 Document → PARALLEL PROCESSING:
├── PROCESSED STREAM → Enhanced content → chunked-rules-repository → KB sync
└── DIRECT STREAM → Original content → rules-repository-alpha → Storage only
```

### **Enhanced Processing Pipeline**
1. **SQS Message Reception** - S3 upload event processing
2. **Document Preparation** - Format conversion + watermark removal
3. **Content Enhancement** - OCR + PDF-plumber processing
4. **Dual Chunking** - Parallel stream processing
5. **Parallel Upload** - Both buckets simultaneously
6. **KB Sync** - Background Bedrock integration
7. **Metrics Recording** - Comprehensive monitoring

## 🚀 Key Features

### **✅ Dual Chunking Architecture**
- **Processed Stream**: Enhanced content with OCR + PDF-plumber → KB sync
- **Direct Stream**: Original content → Archive storage
- **Parallel Processing**: Both streams run simultaneously
- **Dual URI System**: Cross-referencing between streams

### **⚡ Async Processing**
- **32 Concurrent Files**: c5ad.8xlarge optimization
- **Semaphore Control**: Resource management
- **Thread Pool Integration**: CPU-intensive tasks
- **Non-blocking Operations**: Async I/O

### **🔍 Advanced Content Extraction**
- **OCR Integration**: Tesseract with smart page detection
- **PDF-Plumber**: Table and form extraction
- **Structured Data**: Preserves formatting and metadata
- **Image Analysis**: Metadata extraction

### **📊 Enhanced Monitoring**
- **Dual Stream Metrics**: Track both processing paths
- **Real-time Dashboards**: Grafana + Prometheus
- **Performance Tracking**: Processing rates, error rates
- **Resource Monitoring**: CPU, memory, queue depth

### **🔧 Production Features**
- **Cross-Platform**: Windows/Linux compatibility
- **Error Handling**: Comprehensive retry mechanisms
- **Health Checks**: Container and service monitoring
- **Scalability**: Horizontal scaling support

## 📁 Directory Structure

```
pdf-processor/
├── services/                    # Microservices
│   ├── orchestrator.py         # Main workflow coordinator (async)
│   ├── chunking_service.py     # Dual chunking implementation
│   ├── pdf_plumber_service.py  # Table/form extraction (NEW)
│   ├── ocr_service.py          # Tesseract OCR processing
│   ├── kb_sync_service.py      # AWS Bedrock integration
│   ├── conversion_service.py   # Document format conversion
│   ├── watermark_service.py    # Watermark removal
│   ├── metadata_service.py     # Metadata management
│   ├── s3_service.py           # S3 operations
│   └── ...                     # Other services
├── monitoring/                  # Monitoring stack
│   ├── metrics.py              # Enhanced metrics definitions
│   ├── metrics_collector.py    # Advanced metrics collection
│   └── grafana/                # Dashboard configurations
├── utils/                       # Utilities
│   └── logger.py               # Centralized logging
├── sqs_worker.py               # Main SQS worker (async support)
├── config.py                   # Enhanced configuration
├── requirements.txt            # Dependencies with async support
├── docker-compose.yml          # Multi-service setup
├── Dockerfile                  # Container definition
├── .env                        # Environment configuration
└── .env.example               # Environment template
```

## ⚙️ Configuration

### **Environment Variables (.env)**

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_here
AWS_SECRET_ACCESS_KEY=your_aws_secret_key_here
AWS_REGION=us-east-1

# S3 Buckets
SOURCE_BUCKET=rules-repository
CHUNKED_BUCKET=chunked-rules-repository
DIRECT_CHUNKED_BUCKET=rules-repository-alpha
UNPROCESSED_BUCKET=unprocessed-files-error-on-pdf-processing

# SQS Configuration
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/025066274604/s3-to-ec2-queue

# Performance Optimization (c5ad.8xlarge)
MAX_CONCURRENT_FILES=32
MAX_WORKERS_PER_STAGE=16
MAX_WORKERS_OCR_PAGE=16
MAX_WORKERS_FILENAME_CLEANING=32
MAX_PARALLEL_FILES=10
BATCH_SIZE=100
ASYNC_PROCESSING=true

# Monitoring
PUBLIC_IP=18.210.191.199
METRICS_PORT=8000

# Processing Configuration
OCR_DPI=300
OCR_TEXT_THRESHOLD=50
LOG_LEVEL=INFO
```

## 🚀 Deployment

### **Option 1: Direct Python Execution**
```bash
# Install dependencies
pip install -r requirements.txt

# Run with async processing (recommended)
python sqs_worker.py
```

### **Option 2: Docker Deployment**
```bash
# Build and run all services
docker-compose up -d

# View logs
docker-compose logs -f pdf-processor
```

### **Option 3: Production Deployment**
```bash
# Upload to EC2
scp -r pdf-processor/ ec2-user@18.210.191.199:/opt/

# SSH and deploy
ssh ec2-user@18.210.191.199
cd /opt/pdf-processor
docker-compose up -d
```

## 📊 Monitoring & Metrics

### **Access URLs**
- **Grafana Dashboard**: http://18.210.191.199:3000
- **Prometheus Metrics**: http://18.210.191.199:9090
- **Application Metrics**: http://18.210.191.199:8000/metrics
- **Node Exporter**: http://18.210.191.199:9100

### **Key Metrics**
- `processed_chunks_created_total` - Processed stream chunks
- `direct_chunks_created_total` - Direct stream chunks
- `pdf_files_processed_total` - Total files processed
- `sqs_messages_available` - Queue depth
- `processing_duration_seconds` - Processing time by stage

## 🔄 Processing Workflow

### **1. SQS Message Reception**
- Monitors `s3-to-ec2-queue` for S3 upload events
- Batch processing: 10 messages at a time
- Parallel processing: 32 concurrent files

### **2. Document Preparation**
- **Format Conversion**: DOC/DOCX/TXT → PDF (LibreOffice)
- **Watermark Removal**: TMI watermark detection and removal
- **File Validation**: Format and integrity checks

### **3. Content Enhancement**
- **OCR Processing**: Tesseract with 300 DPI for scanned pages
- **PDF-Plumber**: Table and form extraction
- **Smart Detection**: Only process pages that need enhancement

### **4. Dual Chunking (Parallel)**
```python
# Both streams process simultaneously
processed_task = asyncio.create_task(
    chunking_service.chunk_pdf_processed(original_data, key, enhanced_data)
)
direct_task = asyncio.create_task(
    chunking_service.chunk_pdf_direct(original_data, key)
)

# Wait for both to complete
processed_chunks, direct_chunks = await asyncio.gather(
    processed_task, direct_task
)
```

### **5. Parallel Upload**
- **Processed Chunks** → `chunked-rules-repository`
- **Direct Chunks** → `rules-repository-alpha`
- **Metadata Files** → `.metadata.json` for each chunk

### **6. Knowledge Base Sync**
- **Batch Processing**: 20-file threshold per folder
- **Background Sync**: Non-blocking KB integration
- **Error Handling**: Failed files moved to unprocessed bucket

## 📈 Performance Specifications

### **c5ad.8xlarge Optimization**
- **Instance**: 32 vCPUs, 64GB RAM
- **Concurrent Files**: 32 simultaneous processing
- **Workers per Stage**: 16 parallel workers
- **Expected Throughput**: 1000+ files/hour
- **Memory Usage**: 50-55GB peak
- **CPU Utilization**: 90-95% target

### **Async Processing Benefits**
- **Non-blocking I/O**: Efficient resource utilization
- **Semaphore Control**: Prevents resource exhaustion
- **Parallel Chunking**: Both streams process simultaneously
- **Background Tasks**: KB sync doesn't block processing

## 🛠️ Dependencies

### **Core Dependencies**
```
boto3>=1.34.0          # AWS SDK
PyPDF2==3.0.1          # PDF processing
PyMuPDF==1.23.8        # Advanced PDF operations
pytesseract==0.3.10    # OCR processing
reportlab==4.0.7       # PDF generation
```

### **Async Dependencies**
```
aiofiles==23.2.0       # Async file operations
asyncio-throttle==1.0.2 # Rate limiting
aioboto3==12.3.0       # Async AWS SDK
```

### **Enhanced Processing**
```
pdfplumber==0.10.3     # Table extraction
python-docx==0.8.11    # Document conversion
unidecode==1.3.7       # Text normalization
```

### **Monitoring**
```
prometheus-client==0.19.0  # Metrics collection
```

## 🔧 Troubleshooting

### **Common Issues**

#### **1. SQS Connection Issues**
```bash
# Check queue URL
echo $SQS_QUEUE_URL

# Verify AWS credentials
aws sts get-caller-identity
```

#### **2. OCR Processing Errors**
```bash
# Check Tesseract installation
tesseract --version

# Verify language data
tesseract --list-langs
```

#### **3. Memory Issues**
```bash
# Monitor memory usage
docker stats pdf-processor

# Adjust concurrent files
export MAX_CONCURRENT_FILES=16
```

#### **4. S3 Upload Failures**
```bash
# Check S3 permissions
aws s3 ls s3://chunked-rules-repository/

# Verify bucket access
aws s3 cp test.txt s3://rules-repository-alpha/
```

### **Logging**
- **Application Logs**: `pdf_processor.log`
- **Docker Logs**: `docker-compose logs -f`
- **Error Tracking**: CSV error logging enabled
- **Metrics Logs**: Prometheus format

## 🔒 Security

### **Best Practices**
- **Non-root Containers**: Security-hardened Docker images
- **Environment Variables**: Sensitive data in .env files
- **Network Isolation**: Container-based networking
- **Access Control**: IAM roles and policies

### **AWS Permissions Required**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::rules-repository/*",
                "arn:aws:s3:::chunked-rules-repository/*",
                "arn:aws:s3:::rules-repository-alpha/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes"
            ],
            "Resource": "arn:aws:sqs:us-east-1:025066274604:s3-to-ec2-queue"
        },
        {
            "Effect": "Allow",
            "Action": [
                "bedrock:StartIngestionJob",
                "bedrock:GetIngestionJob"
            ],
            "Resource": "*"
        }
    ]
}
```

## 📚 API Reference

### **Main Classes**

#### **SQSWorker**
```python
class SQSWorker:
    def run_async()              # Async SQS processing
    def process_messages_async() # Batch message processing
    def process_single_message_async() # Individual message handling
```

#### **Orchestrator**
```python
class Orchestrator:
    async def process_single_file_async() # Main processing method
    def _prepare_document_sync()          # Document preparation
    def _enhance_document_sync()          # OCR + PDF-plumber
```

#### **ChunkingService**
```python
class ChunkingService:
    async def chunk_pdf_processed() # Enhanced content chunking
    async def chunk_pdf_direct()    # Original content chunking
    def extract_metadata()          # Metadata extraction
```

#### **PDFPlumberService**
```python
class PDFPlumberService:
    def detect_tables_or_forms()    # Structure detection
    def page_to_enhanced_data()     # Data extraction
    def apply_pdf_plumber_to_pdf()  # Full document processing
```

## 🎯 Production Checklist

### **Pre-Deployment**
- [ ] AWS credentials configured
- [ ] SQS queue accessible
- [ ] S3 buckets created and accessible
- [ ] EC2 instance sized appropriately (c5ad.8xlarge)
- [ ] Docker and Docker Compose installed
- [ ] Environment variables set

### **Post-Deployment**
- [ ] SQS messages being processed
- [ ] Both S3 buckets receiving chunks
- [ ] Grafana dashboard accessible
- [ ] Prometheus metrics collecting
- [ ] Error logs monitoring
- [ ] KB sync functioning

### **Monitoring Setup**
- [ ] Grafana alerts configured
- [ ] Prometheus retention set
- [ ] Log rotation enabled
- [ ] Disk space monitoring
- [ ] Memory usage alerts

## 📞 Support

### **System Status**
- Monitor queue depth: `sqs_messages_available`
- Check processing rate: `processing_rate_per_hour`
- Verify dual chunking: `processed_chunks_created_total` vs `direct_chunks_created_total`

### **Performance Tuning**
- Adjust `MAX_CONCURRENT_FILES` based on instance size
- Monitor memory usage and adjust accordingly
- Scale horizontally by running multiple instances
- Optimize OCR settings for document types

---

## 🎉 Success!

Your enhanced PDF processing pipeline is now ready for production with:
- **Dual chunking strategy** for comprehensive document processing
- **Async processing** for maximum performance
- **Advanced content extraction** with OCR and PDF-plumber
- **Enterprise monitoring** with real-time dashboards
- **Production-grade reliability** with comprehensive error handling

**Happy Processing!** 🚀
