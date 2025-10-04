"""
Celery tasks package for image-to-XLSX conversion service.

This package contains:
- celery_app: Main Celery application configuration
- batch_tasks: Distributed tasks for batch image processing
"""

from .celery_app import celery_app

__all__ = ["celery_app"]