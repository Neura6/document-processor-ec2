"""
Filename Cleaning Service
Handles all filename cleaning operations including quote removal, space normalization,
TMI removal, and non-English character conversion.
"""

import re
import os
import logging
import unidecode
from typing import Tuple

logger = logging.getLogger(__name__)

# ULTRA-STRICT regex patterns: ONLY English letters, numbers, underscores
REMOVE_TERM_REGEX = r"TMI\s*"
BRACKET_CONTENT_REGEX = r"\[.*?\]\s*"
PAREN_CONTENT_REGEX = r"\(.*?\)\s*"
QUOTE_CHARS_REGEX = r"[''""'\"]+"
# Allow spaces to be handled separately - don't remove them
ULTRA_STRICT_REGEX = r"[^a-zA-Z0-9_/ ]"
MULTIPLE_UNDERSCORES_REGEX = r"_{2,}"
WHITESPACE_REGEX = r"\s+"

class FilenameService:
    """Service for cleaning and normalizing filenames."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def clean_filename(self, original_key: str) -> str:
        """
        Clean filename with ULTRA-STRICT filtering: ONLY English letters, numbers, underscores.
        Handles ALL unknown characters and converts everything to English.
        
        Args:
            original_key: Original S3 object key
            
        Returns:
            Cleaned key string (ONLY: a-z, A-Z, 0-9, _, / for folders)
        """
        current_key = original_key
        modified = False
        
        # Split into directory and filename for processing
        dirname, basename = current_key.rsplit('/', 1) if '/' in current_key else ('', current_key)
        filename_only = basename
        
        # Extract extension from original filename
        filename_without_ext, ext = os.path.splitext(filename_only)
        if not ext:
            ext = '.pdf'  # Default to PDF if no extension
        
        # Step 1: Remove ALL non-English characters completely - skip unidecode
        # Step 2: Remove special characters but keep spaces
        cleaned_filename = re.sub(ULTRA_STRICT_REGEX, '', filename_only)
        if cleaned_filename != filename_only:
            modified = True
        
        # Step 3: Replace spaces with underscores
        cleaned_filename = re.sub(WHITESPACE_REGEX, '_', cleaned_filename)
        
        # Step 4: Replace multiple underscores with single underscore
        cleaned_filename = re.sub(MULTIPLE_UNDERSCORES_REGEX, '_', cleaned_filename)
        
        # Step 5: Remove leading/trailing underscores and dots
        cleaned_filename = cleaned_filename.strip('_.')
        
        # Ensure we have at least some valid characters
        if not cleaned_filename:
            cleaned_filename = 'unnamed_file'
        
        # Step 6: Add extension back (ensure no duplication)
        # Handle extension properly - don't add .pdf when it's already there
        if ext.lower() == '.pdf':
            # Check if cleaned filename already ends with .pdf (case insensitive)
            if not cleaned_filename.lower().endswith('.pdf'):
                final_filename = f"{cleaned_filename}{ext}"
            else:
                final_filename = cleaned_filename
        else:
            # For other extensions, ensure we don't duplicate
            if not cleaned_filename.lower().endswith(ext.lower()):
                final_filename = f"{cleaned_filename}{ext}"
            else:
                final_filename = cleaned_filename
        
        # Reconstruct the full path
        cleaned_key = f"{dirname}/{final_filename}".replace("//", "/") if dirname else final_filename
        
        return cleaned_key if modified else original_key
    
    def needs_cleaning(self, original_key: str) -> bool:
        """Check if filename needs cleaning."""
        return self.clean_filename(original_key) != original_key
