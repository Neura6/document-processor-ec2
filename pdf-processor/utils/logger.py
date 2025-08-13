"""
Logging and Error Handling Service
Centralized logging with file and console handlers.
"""

import logging
import os
import time
from datetime import datetime
import csv

class LoggerService:
    """Service for centralized logging and error handling."""
    
    def __init__(self, log_dir: str = 'logs'):
        self.log_dir = log_dir
        self.setup_logging()
        self.setup_error_log()
    
    def setup_logging(self):
        """Configure logging with both file and console handlers."""
        # Create logs directory if it doesn't exist
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Create logger
        self.logger = logging.getLogger('PDFProcessor')
        self.logger.setLevel(logging.DEBUG)
        
        # File handler
        file_handler = logging.FileHandler(
            f'{self.log_dir}/pdf_processor_{time.strftime("%Y%m%d_%H%M%S")}.log'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def setup_error_log(self):
        """Setup CSV error logging."""
        self.error_log_file = f'{self.log_dir}/errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.error_fields = ['timestamp', 'file', 'error', 'service', 'stage']
        
        # Create CSV file with headers
        with open(self.error_log_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.error_fields)
            writer.writeheader()
    
    def log_error(self, file: str, error: str, service: str = 'unknown', stage: str = 'unknown'):
        """
        Log error to CSV file.
        
        Args:
            file: File being processed
            error: Error message
            service: Service where error occurred
            stage: Processing stage
        """
        with open(self.error_log_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.error_fields)
            writer.writerow({
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'file': file,
                'error': str(error),
                'service': service,
                'stage': stage
            })
    
    def get_logger(self, name: str = None) -> logging.Logger:
        """Get logger instance."""
        if name:
            return logging.getLogger(name)
        return self.logger
