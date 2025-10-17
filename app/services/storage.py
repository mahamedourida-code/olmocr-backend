import uuid
import time
import os
import json
import shutil
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from pathlib import Path
import logging

from app.core.config import settings
from app.utils.exceptions import SessionNotFoundError, FileNotFoundError

if TYPE_CHECKING:
    from app.models.jobs import SessionMetadata

logger = logging.getLogger(__name__)


class FileStorageManager:
    """Manages file storage operations for the application."""
    
    def __init__(self):
        self.storage_path = Path(settings.temp_storage_path)
        self.sessions_path = self.storage_path / "sessions"
        self.downloads_path = self.storage_path / "downloads"
        
        # Create directories if they don't exist
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Ensure storage directories exist."""
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.sessions_path.mkdir(parents=True, exist_ok=True)
        self.downloads_path.mkdir(parents=True, exist_ok=True)
    
    def create_session(self) -> str:
        """
        Create a new session for file storage.
        
        Returns:
            Session ID string
        """
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        session_path = self.sessions_path / session_id
        session_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (session_path / "uploads").mkdir(exist_ok=True)
        (session_path / "processing").mkdir(exist_ok=True)
        (session_path / "results").mkdir(exist_ok=True)
        
        logger.info(f"Created session: {session_id}")
        return session_id
    
    def get_session_path(self, session_id: str) -> Path:
        """
        Get the file path for a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Path to session directory
            
        Raises:
            SessionNotFoundError: If session doesn't exist
        """
        session_path = self.sessions_path / session_id
        if not session_path.exists():
            raise SessionNotFoundError(f"Session {session_id} not found")
        return session_path
    
    def save_uploaded_image(self, session_id: str, filename: str, image_data: bytes) -> str:
        """
        Save uploaded image data to session storage.
        
        Args:
            session_id: Session identifier
            filename: Original filename
            image_data: Image binary data
            
        Returns:
            Saved file path relative to session
        """
        session_path = self.get_session_path(session_id)
        uploads_path = session_path / "uploads"
        
        # Generate unique filename
        file_id = uuid.uuid4().hex[:8]
        safe_filename = f"{file_id}_{filename}"
        file_path = uploads_path / safe_filename
        
        # Save file
        with open(file_path, "wb") as f:
            f.write(image_data)
        
        logger.info(f"Saved uploaded image: {file_path}")
        return str(file_path.relative_to(session_path))
    
    async def save_result_file(self, session_id: str, filename: str, file_data: bytes, file_id: str = None) -> str:
        """
        Save result file (XLSX) to session storage.
        
        Args:
            session_id: Session identifier
            filename: Result filename
            file_data: File binary data
            file_id: Optional custom file ID (if not provided, generates one)
            
        Returns:
            File ID for download access
        """
        session_path = self.get_session_path(session_id)
        results_path = session_path / "results"
        
        # Use provided file_id or generate unique one
        if file_id is None:
            file_id = uuid.uuid4().hex
        file_path = results_path / f"{file_id}_{filename}"
        
        # Save file asynchronously
        import aiofiles
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(file_data)
        
        # Also create a symlink in downloads directory for direct access
        download_path = self.downloads_path / f"{file_id}.xlsx"
        try:
            download_path.symlink_to(file_path)
        except OSError:
            # If symlink fails, copy the file
            import asyncio
            await asyncio.to_thread(shutil.copy2, file_path, download_path)
        
        # Update session metadata to include this file
        try:
            session = await self.get_session_metadata(session_id)
            if session and file_id not in session.result_files:
                session.result_files.append(file_id)
                await self.update_session_metadata(session)
                logger.info(f"Added file {file_id} to session {session_id} result_files")
        except Exception as e:
            logger.error(f"Failed to update session metadata after saving file {file_id}: {e}")
        
        logger.info(f"Saved result file: {file_path}")
        return file_id

    def save_result_file_sync(self, session_id: str, filename: str, file_data: bytes, file_id: str = None) -> str:
        """
        Save result file (XLSX) to session storage - synchronous version.
        
        Args:
            session_id: Session identifier
            filename: Result filename  
            file_data: File binary data
            file_id: Optional custom file ID (if not provided, generates one)
            
        Returns:
            File ID for download access
        """
        # Ensure session directory exists (create if necessary)
        session_path = self.sessions_path / session_id
        if not session_path.exists():
            logger.info(f"Creating session directory for {session_id}")
            session_path.mkdir(parents=True, exist_ok=True)
            (session_path / "uploads").mkdir(exist_ok=True)
            (session_path / "processing").mkdir(exist_ok=True)
            (session_path / "results").mkdir(exist_ok=True)
        
        results_path = session_path / "results"
        
        # Use provided file_id or generate unique one
        if file_id is None:
            file_id = uuid.uuid4().hex
        file_path = results_path / f"{file_id}_{filename}"
        
        # Save file synchronously
        with open(file_path, "wb") as f:
            f.write(file_data)
        
        # Also create a symlink in downloads directory for direct access
        download_path = self.downloads_path / f"{file_id}.xlsx"
        try:
            download_path.symlink_to(file_path)
        except OSError:
            # If symlink fails, copy the file
            shutil.copy2(file_path, download_path)
        
        # Update session metadata to include this file
        try:
            import asyncio
            session = asyncio.run(self.get_session_metadata(session_id))
            if session and file_id not in session.result_files:
                session.result_files.append(file_id)
                asyncio.run(self.update_session_metadata(session))
                logger.info(f"Added file {file_id} to session {session_id} result_files")
        except Exception as e:
            logger.error(f"Failed to update session metadata after saving file {file_id}: {e}")
        
        logger.info(f"Saved result file: {file_path}")
        return file_id
    
    async def cleanup_file(self, file_id: str) -> None:
        """
        Clean up a file by its ID.
        
        Args:
            file_id: File identifier to clean up
        """
        try:
            # Remove from downloads directory
            download_path = self.downloads_path / f"{file_id}.xlsx"
            if download_path.exists():
                download_path.unlink()
                logger.info(f"Cleaned up download file: {download_path}")
        except Exception as e:
            logger.error(f"Failed to cleanup file {file_id}: {e}")
    
    def get_download_file_path(self, file_id: str) -> Path:
        """
        Get the file path for a download file.
        
        Args:
            file_id: File identifier
            
        Returns:
            Path to download file
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        file_path = self.downloads_path / f"{file_id}.xlsx"
        if not file_path.exists():
            raise FileNotFoundError(f"File {file_id} not found or expired")
        return file_path
    
    def cleanup_expired_files(self) -> int:
        """
        Clean up expired files and sessions.
        
        Returns:
            Number of cleaned up items
        """
        cleanup_count = 0
        cutoff_time = time.time() - settings.file_retention_seconds
        
        # Clean up sessions
        for session_path in self.sessions_path.iterdir():
            if session_path.is_dir():
                # Check if session is expired based on modification time
                if session_path.stat().st_mtime < cutoff_time:
                    try:
                        shutil.rmtree(session_path)
                        cleanup_count += 1
                        logger.info(f"Cleaned up expired session: {session_path.name}")
                    except Exception as e:
                        logger.error(f"Failed to cleanup session {session_path.name}: {e}")
        
        # Clean up download files
        for file_path in self.downloads_path.iterdir():
            if file_path.is_file():
                if file_path.stat().st_mtime < cutoff_time:
                    try:
                        file_path.unlink()
                        cleanup_count += 1
                        logger.info(f"Cleaned up expired download file: {file_path.name}")
                    except Exception as e:
                        logger.error(f"Failed to cleanup file {file_path.name}: {e}")
        
        logger.info(f"Cleanup completed. Removed {cleanup_count} items.")
        return cleanup_count
    
    def get_session_info(self, session_id: str) -> Dict[str, Any]:
        """
        Get information about a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Dictionary with session information
        """
        session_path = self.get_session_path(session_id)
        
        # Count files in each directory
        uploads_count = len(list((session_path / "uploads").iterdir()))
        processing_count = len(list((session_path / "processing").iterdir()))
        results_count = len(list((session_path / "results").iterdir()))
        
        # Get creation time
        creation_time = session_path.stat().st_ctime
        expires_at = creation_time + settings.file_retention_seconds
        
        return {
            "session_id": session_id,
            "created_at": datetime.fromtimestamp(creation_time).isoformat(),
            "expires_at": datetime.fromtimestamp(expires_at).isoformat(),
            "uploads_count": uploads_count,
            "processing_count": processing_count,
            "results_count": results_count,
            "is_expired": time.time() > expires_at
        }
    
    async def health_check(self) -> bool:
        """
        Check if storage service is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            # Check if directories exist and are writable
            self._ensure_directories()
            
            # Try to create a test file
            test_file = self.storage_path / "health_test.tmp"
            test_file.write_text("health_check")
            test_file.unlink()  # Remove test file
            
            return True
        except Exception as e:
            logger.error(f"Storage health check failed: {e}")
            return False
    
    async def get_statistics(self) -> Dict[str, Any]:
        """
        Get storage statistics.
        
        Returns:
            Dictionary with storage statistics
        """
        try:
            # Count sessions
            sessions_count = len(list(self.sessions_path.iterdir())) if self.sessions_path.exists() else 0
            downloads_count = len(list(self.downloads_path.iterdir())) if self.downloads_path.exists() else 0
            
            # Calculate disk usage
            total_size = 0
            if self.storage_path.exists():
                for file_path in self.storage_path.rglob("*"):
                    if file_path.is_file():
                        total_size += file_path.stat().st_size
            
            return {
                "sessions_count": sessions_count,
                "downloads_count": downloads_count,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2)
            }
        except Exception as e:
            logger.error(f"Failed to get storage statistics: {e}")
            return {
                "error": str(e)
            }
    
    async def get_session_metadata(self, session_id: str) -> Optional["SessionMetadata"]:
        """
        Get metadata for a session.
        
        Args:
            session_id: Session ID to get metadata for
            
        Returns:
            SessionMetadata object or None if session doesn't exist
        """
        from app.models.jobs import SessionMetadata
        
        try:
            session_path = self.sessions_path / session_id
            if not session_path.exists():
                return None
            
            # Try to load from metadata file first
            metadata_file = session_path / "metadata.json"
            if metadata_file.exists():
                try:
                    with open(metadata_file, 'r') as f:
                        data = json.load(f)
                    
                    # Convert datetime strings back to datetime objects
                    return SessionMetadata(
                        session_id=data['session_id'],
                        created_at=datetime.fromisoformat(data['created_at']),
                        last_activity=datetime.fromisoformat(data['last_activity']),
                        expires_at=datetime.fromisoformat(data['expires_at']),
                        uploads_count=data['uploads_count'],
                        jobs_count=data['jobs_count'],
                        downloads_count=data['downloads_count'],
                        uploaded_files=data['uploaded_files'],
                        result_files=data['result_files'],
                        active_jobs=data['active_jobs']
                    )
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Failed to load session metadata from file: {e}")
            
            # Fallback to directory-based metadata (for backwards compatibility)
            uploads_count = len(list((session_path / "uploads").iterdir())) if (session_path / "uploads").exists() else 0
            
            # Get creation time
            creation_time = session_path.stat().st_ctime
            expires_at = creation_time + settings.file_retention_seconds
            
            return SessionMetadata(
                session_id=session_id,
                created_at=datetime.fromtimestamp(creation_time),
                last_activity=datetime.fromtimestamp(creation_time),
                expires_at=datetime.fromtimestamp(expires_at),
                uploads_count=uploads_count,
                jobs_count=0,
                downloads_count=0,
                uploaded_files=[],
                result_files=[],
                active_jobs=[]
            )
        except Exception as e:
            logger.error(f"Error getting session metadata for {session_id}: {e}")
            return None
    
    async def create_session(self, session_id: str) -> 'SessionMetadata':
        """
        Create a session and return metadata.
        
        Args:
            session_id: Session ID to create
            
        Returns:
            Session metadata object
        """
        session_path = self.sessions_path / session_id
        session_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (session_path / "uploads").mkdir(exist_ok=True)
        (session_path / "processing").mkdir(exist_ok=True)
        (session_path / "results").mkdir(exist_ok=True)
        
        logger.info(f"Created session: {session_id}")
        
        # Return metadata for the new session
        creation_time = time.time()
        expires_at = creation_time + settings.file_retention_seconds
        
        # Import here to avoid circular import
        from app.models.jobs import SessionMetadata
        
        return SessionMetadata(
            session_id=session_id,
            created_at=datetime.fromtimestamp(creation_time),
            last_activity=datetime.fromtimestamp(creation_time),
            expires_at=datetime.fromtimestamp(expires_at),
            uploads_count=0,
            jobs_count=0,
            downloads_count=0,
            uploaded_files=[],
            result_files=[],
            active_jobs=[]
        )

    async def update_session_metadata(self, session: 'SessionMetadata') -> None:
        """
        Update session metadata by saving to JSON file.
        
        Args:
            session: Session metadata to update
        """
        try:
            session_path = self.sessions_path / session.session_id
            if not session_path.exists():
                logger.warning(f"Session directory {session.session_id} does not exist")
                return
            
            # Update last activity
            session.last_activity = datetime.utcnow()
            
            # Convert session metadata to JSON-serializable format
            metadata_data = {
                'session_id': session.session_id,
                'created_at': session.created_at.isoformat(),
                'last_activity': session.last_activity.isoformat(),
                'expires_at': session.expires_at.isoformat(),
                'uploads_count': session.uploads_count,
                'jobs_count': session.jobs_count,
                'downloads_count': session.downloads_count,
                'uploaded_files': session.uploaded_files,
                'result_files': session.result_files,
                'active_jobs': session.active_jobs
            }
            
            # Save to metadata file
            metadata_file = session_path / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata_data, f, indent=2)
            
            logger.debug(f"Updated session metadata for {session.session_id}")
            
        except Exception as e:
            logger.error(f"Failed to update session metadata for {session.session_id}: {e}")

    async def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a file.
        
        Args:
            file_id: File identifier
            
        Returns:
            Dictionary with file information or None if not found
        """
        try:
            # Try to find the file in the storage directory
            # First check if it's a direct file
            file_path = self.storage_path / f"{file_id}.xlsx"
            if file_path.exists():
                stat = file_path.stat()
                return {
                    'file_id': file_id,
                    'filename': f"{file_id}.xlsx",
                    'size_bytes': stat.st_size,
                    'created_at': datetime.fromtimestamp(stat.st_ctime),
                    'modified_at': datetime.fromtimestamp(stat.st_mtime)
                }
            
            # Check in session directories
            for session_dir in self.sessions_path.iterdir():
                if session_dir.is_dir():
                    file_path = session_dir / f"{file_id}.xlsx"
                    if file_path.exists():
                        stat = file_path.stat()
                        return {
                            'file_id': file_id,
                            'filename': f"{file_id}.xlsx",
                            'size_bytes': stat.st_size,
                            'created_at': datetime.fromtimestamp(stat.st_ctime),
                            'modified_at': datetime.fromtimestamp(stat.st_mtime),
                            'session_id': session_dir.name
                        }
            
            logger.debug(f"File {file_id} not found in storage")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get file info for {file_id}: {e}")
            return None

    async def download_file(self, file_id: str) -> Optional[bytes]:
        """
        Download a file from storage.
        
        Args:
            file_id: File identifier
            
        Returns:
            File content as bytes or None if not found
        """
        try:
            # Try to find the file
            file_path = self.storage_path / f"{file_id}.xlsx"
            if file_path.exists():
                with open(file_path, 'rb') as f:
                    return f.read()
            
            # Check in session directories
            for session_dir in self.sessions_path.iterdir():
                if session_dir.is_dir():
                    file_path = session_dir / f"{file_id}.xlsx"
                    if file_path.exists():
                        with open(file_path, 'rb') as f:
                            return f.read()
            
            logger.debug(f"File {file_id} not found for download")
            return None
            
        except Exception as e:
            logger.error(f"Failed to download file {file_id}: {e}")
            return None


# Global storage manager instance
storage_manager = FileStorageManager()