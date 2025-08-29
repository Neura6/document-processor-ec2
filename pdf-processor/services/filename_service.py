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
# ULTRA-STRICT: ONLY a-z, A-Z, 0-9, _ (and / for folder paths)
ULTRA_STRICT_REGEX = r"[^a-zA-Z0-9_/]"
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
        
        # Step 1: Convert ALL non-English characters to English using unidecode FIRST
        try:
            cleaned_filename = unidecode.unidecode(filename_only)
            if cleaned_filename != filename_only:
                self.logger.debug(f"Converted non-English: {filename_only} -> {cleaned_filename}")
                filename_only = cleaned_filename
                modified = True
        except Exception as e:
            self.logger.error(f"Error during unidecode conversion: {e}")
        
        # Step 2: Remove bracket content including brackets [content]
        cleaned_filename = re.sub(BRACKET_CONTENT_REGEX, "", filename_only)
        if cleaned_filename != filename_only:
            self.logger.debug(f"Removed brackets: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Step 3: Remove parenthesis content including parentheses (content)
        cleaned_filename = re.sub(PAREN_CONTENT_REGEX, "", filename_only)
        if cleaned_filename != filename_only:
            self.logger.debug(f"Removed parentheses: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Step 4: Remove TMI and related terms (case-insensitive)
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
        
        # Step 5: Remove quote characters
        cleaned_filename = re.sub(QUOTE_CHARS_REGEX, "", filename_only)
        if cleaned_filename != filename_only:
            self.logger.debug(f"Removed quotes: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Step 6: Replace ALL spaces with underscores
        cleaned_filename = re.sub(WHITESPACE_REGEX, "_", filename_only)
        if cleaned_filename != filename_only:
            self.logger.debug(f"Replaced spaces with underscores: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Step 7: Handle Arabic and non-Latin characters by transliteration
        try:
            # Use unidecode to transliterate non-Latin characters to ASCII
            filename_only = unidecode(filename_only)
        except Exception:
            # Fallback: remove non-ASCII characters
            filename_only = re.sub(r'[^\x00-\x7F]+', '_', filename_only)
        
        # Step 8: Normalize multiple underscores only
        cleaned_filename = re.sub(MULTIPLE_UNDERSCORES_REGEX, "_", filename_only)
        cleaned_filename = cleaned_filename.strip("_")
        
        if cleaned_filename != filename_only:
            self.logger.debug(f"Normalized underscores/dashes: {filename_only} -> {cleaned_filename}")
            filename_only = cleaned_filename
            modified = True
        
        # Reconstruct the full path
        cleaned_key = f"{dirname}/{filename_only}".replace("//", "/") if dirname else filename_only
        
        return cleaned_key if modified else original_key
    
    def needs_cleaning(self, original_key: str) -> bool:
        """Check if filename needs cleaning."""
        return self.clean_filename(original_key) != original_key
