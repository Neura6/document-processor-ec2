"""
Filename Cleaning Service
Handles all filename cleaning operations including quote removal, space normalization,
TMI removal, and non-English character conversion.
"""

import re
import unidecode
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

# Regex patterns
REMOVE_TERM_REGEX = r"TMI\s*"
DOUBLE_SPACE_REGEX = r"[\s\u00A0\u2000-\u200B]{2,}"
QUOTE_CHARS_REGEX = r"[‘’”“'\"]+"
BRACKET_CONTENT_REGEX = r"\[.*?\]\s*"
PAREN_CONTENT_REGEX = r"\(.*?\)\s*"
SPECIAL_CHARS_REGEX = r"[^\w\s\-._/]"
MULTIPLE_DASHES_REGEX = r"-+"

class FilenameService:
    """Service for cleaning and normalizing filenames."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def clean_filename(self, original_key: str) -> str:
        """
        Clean filename comprehensively including quotes, TMI, brackets, special chars,
        and non-English characters exactly as in data_cleaner.py.
        
        Args:
            original_key: Original S3 object key
            
        Returns:
            Cleaned key string
        """
        current_key = original_key
        modified = False
        
        # Split into directory and filename for processing
        dirname, basename = current_key.rsplit('/', 1) if '/' in current_key else ('', current_key)
        filename_only = basename
        
        # Step 1: Remove bracket content including brackets [content]
        cleaned_filename = re.sub(BRACKET_CONTENT_REGEX, "", filename_only)
        if cleaned_filename != filename_only:
            self.logger.debug(f"Removed brackets: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Step 2: Remove parenthesis content including parentheses (content)
        cleaned_filename = re.sub(PAREN_CONTENT_REGEX, "", filename_only)
        if cleaned_filename != filename_only:
            self.logger.debug(f"Removed parentheses: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Step 3: Remove TMI and related terms (case-insensitive)
        tmi_patterns = [
            r"TMI\s*",
            r"\d+\s*TMI\s*",
            r"\(\d+\)\s*TMI\s*",
            r"\[\d+\]\s*TMI\s*"
        ]
        
        for pattern in tmi_patterns:
            cleaned_filename = re.sub(pattern, "", filename_only, flags=re.IGNORECASE)
            if cleaned_filename != filename_only:
                self.logger.debug(f"Removed TMI pattern: {filename_only} -> {cleaned_filename}")
                filename_only = cleaned_filename
                modified = True
        
        # Step 4: Remove quote characters
        cleaned_filename = re.sub(QUOTE_CHARS_REGEX, "", filename_only)
        if cleaned_filename != filename_only:
            self.logger.debug(f"Removed quotes: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Step 5: Remove special characters except allowed ones
        cleaned_filename = re.sub(SPECIAL_CHARS_REGEX, "", filename_only)
        if cleaned_filename != filename_only:
            self.logger.debug(f"Removed special chars: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Step 6: Normalize multiple dashes and spaces
        cleaned_filename = re.sub(MULTIPLE_DASHES_REGEX, "-", filename_only)
        cleaned_filename = re.sub(DOUBLE_SPACE_REGEX, " ", cleaned_filename)
        cleaned_filename = cleaned_filename.strip(" -_.")
        
        if cleaned_filename != filename_only:
            self.logger.debug(f"Normalized dashes/spaces: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Step 7: Convert non-English characters using unidecode
        try:
            cleaned_filename = unidecode.unidecode(filename_only)
            if cleaned_filename != filename_only:
                self.logger.debug(f"Converted non-English: {filename_only} -> {cleaned_filename}")
                filename_only = cleaned_filename
                modified = True
        except Exception as e:
            self.logger.error(f"Error during unidecode conversion: {e}")
        
        # Reconstruct the full path
        cleaned_key = f"{dirname}/{filename_only}".replace("//", "/") if dirname else filename_only
        
        return cleaned_key if modified else original_key
    
    def needs_cleaning(self, original_key: str) -> bool:
        """Check if filename needs cleaning."""
        return self.clean_filename(original_key) != original_key
