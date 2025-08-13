#!/usr/bin/env python3
"""
Main entry point for PDF processor microservices.
"""

import argparse
import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.orchestrator import Orchestrator

def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(description='PDF Processor Microservices')
    parser.add_argument('--folder', type=str, help='Process all PDFs in a folder')
    parser.add_argument('--file', type=str, help='Process a single PDF file')
    parser.add_argument('--config', type=str, help='Configuration file path')
    
    args = parser.parse_args()
    
    if not args.folder and not args.file:
        print("Error: Please specify either --folder or --file")
        sys.exit(1)
    
    # Initialize orchestrator
    orchestrator = Orchestrator()
    
    try:
        if args.file:
            print(f"Processing single file: {args.file}")
            success = orchestrator.process_single_file(args.file)
            print(f"Processing {'successful' if success else 'failed'}")
            
        elif args.folder:
            print(f"Processing folder: {args.folder}")
            results = orchestrator.process_folder(args.folder)
            print(f"Processing completed:")
            print(f"  Total files: {results['total']}")
            print(f"  Successful: {results['success']}")
            print(f"  Failed: {results['failed']}")
            
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
