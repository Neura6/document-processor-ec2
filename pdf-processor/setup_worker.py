#!/usr/bin/env python3
"""
Setup script for PDF Processor SQS Worker
Installs dependencies and configures the worker service
"""

import os
import subprocess
import sys

def run_command(command, check=True):
    """Run shell command"""
    print(f"Running: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"Error: {result.stderr}")
        sys.exit(1)
    return result

def setup_virtual_environment():
    """Create and setup virtual environment"""
    if not os.path.exists('venv'):
        print("Creating virtual environment...")
        run_command(f"{sys.executable} -m venv venv")
    
    # Install dependencies
    print("Installing dependencies...")
    pip_path = 'venv/Scripts/pip' if os.name == 'nt' else 'venv/bin/pip'
    run_command(f"{pip_path} install -r requirements.txt")
    
    # Install additional dependencies for SQS worker
    run_command(f"{pip_path} install boto3 python-dotenv")

def create_env_template():
    """Create .env template"""
    env_content = """# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=us-east-1

# S3 Configuration
SOURCE_BUCKET=rules-repository
CHUNKED_BUCKET=chunked-rules-repository

# SQS Configuration
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/your-account-id/your-queue-name

# Processing Configuration
MAX_WORKERS_FILENAME_CLEANING=10
MAX_WORKERS_OCR_PAGE=4
DEFAULT_DPI_OCR=300
OCR_TEXT_THRESHOLD=50

# Logging
LOG_LEVEL=INFO
"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(env_content)
        print("Created .env template file. Please update with your actual values.")

def create_directories():
    """Create necessary directories"""
    directories = ['logs', 'temp', 'processed']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")

def main():
    """Main setup function"""
    print("Setting up PDF Processor SQS Worker...")
    
    setup_virtual_environment()
    create_env_template()
    create_directories()
    
    print("\nSetup complete!")
    print("\nNext steps:")
    print("1. Edit .env file with your AWS credentials and SQS queue URL")
    print("2. Test the worker: python sqs_worker.py --queue-url YOUR_QUEUE_URL")
    print("3. For production deployment, copy pdf-processor-worker.service to /etc/systemd/system/")
    print("4. Enable service: sudo systemctl enable pdf-processor-worker")
    print("5. Start service: sudo systemctl start pdf-processor-worker")

if __name__ == "__main__":
    main()
