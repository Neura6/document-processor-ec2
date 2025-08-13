"""
Folder Sync Tracker for 20-file batch KB sync
Tracks file counts per folder and triggers KB sync at thresholds
"""

import json
import os
import sys
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class FolderSyncTracker:
    """Tracks file processing counts per folder for smart KB sync"""
    
    SYNC_THRESHOLD = 20  # Sync after every 20 files
    STATE_FILE = 'sync_state.json'
    
    def __init__(self):
        self.state_file = self.STATE_FILE
        self.file_counts = self._load_state()
        # Import config with proper path handling
        # Import config module and access attributes directly
        import os
        import sys
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        sys.path.insert(0, parent_dir)
        
        # Import config module and access attributes directly
        import sys
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        import config
        # Store config module reference
        self.config = config
    
    def _load_state(self) -> Dict[str, int]:
        """Load sync state from file"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"[FolderSyncTracker] Failed to load sync state: {e}")
        return {}
    
    def _save_state(self):
        """Save sync state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.file_counts, f, indent=2)
        except Exception as e:
            logger.error(f"[FolderSyncTracker] Failed to save sync state: {e}")
    
    def increment_and_check_sync(self, folder_name: str) -> bool:
        """
        Increment file count for folder and check if sync should trigger
        
        Args:
            folder_name: Name of the folder being processed
            
        Returns:
            True if sync should trigger (reached threshold)
        """
        if folder_name not in self.file_counts:
            self.file_counts[folder_name] = 0
        
        self.file_counts[folder_name] += 1
        self._save_state()
        
        should_sync = self.file_counts[folder_name] >= self.SYNC_THRESHOLD
        
        if should_sync:
            logger.info(f"[KB-SYNC] Sync threshold reached for {folder_name}: {self.file_counts[folder_name]} files")
            self.reset_count(folder_name)
            self.sync_to_knowledge_base(folder_name)
        
        return should_sync
    
    def sync_to_knowledge_base(self, folder_name: str):
        """Perform KB sync with token limit handling"""
        # Import KB service
        from services.kb_sync_service import KBIngestionService
        
        kb_service = KBIngestionService(
            aws_access_key_id=self.config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.config.AWS_SECRET_ACCESS_KEY,
            region_name=self.config.AWS_REGION
        )
        
        result = kb_service.sync_and_handle_failed_files(folder_name)
        
        if result.get('status') == 'COMPLETE':
            logger.info(f"KB sync completed successfully for folder {folder_name}")
        elif result.get('status') == 'Completed with Failed Files':
            logger.warning(f"KB sync completed with token limit failures for folder {folder_name}. Files moved to unprocessed: {result.get('files_moved_to_unprocessed', [])}")
        else:
            logger.warning(f"KB sync completed with issues for folder {folder_name}: {result}")
        
        return result
    
    def reset_count(self, folder_name: str):
        """Reset file count for folder after sync"""
        if folder_name in self.file_counts:
            self.file_counts[folder_name] = 0
            self._save_state()
    
    def get_count(self, folder_name: str) -> int:
        """Get current file count for folder"""
        return self.file_counts.get(folder_name, 0)
    
    def should_final_sync(self, folder_name: str) -> bool:
        """Check if final sync should trigger (remaining files < threshold)"""
        count = self.get_count(folder_name)
        return count > 0 and count < self.SYNC_THRESHOLD
    
    def get_all_pending_folders(self) -> Dict[str, int]:
        """Get all folders with pending files to sync"""
        return {
            folder: count 
            for folder, count in self.file_counts.items() 
            if count > 0
        }
