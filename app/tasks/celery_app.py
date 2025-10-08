"""
Celery application configuration for distributed task processing.

This module configures Celery with Redis broker for handling
batch image processing tasks with optimal concurrency and scalability.
"""

import os
from celery import Celery
from kombu import Exchange, Queue

from app.core.config import settings

# Get Redis URL with Windows compatibility
def get_redis_url_for_celery():
    """Get Redis URL with fallback for Windows networking issues."""
    # Try different Redis connection formats for Windows
    redis_hosts = [
        "redis://localhost:6379/0",
        "redis://127.0.0.1:6379/0",
        "redis://0.0.0.0:6379/0"
    ]
    
    # If environment specifies a Redis URL, use that first
    env_redis = os.getenv("REDIS_URL") or os.getenv("REDISURL")
    if env_redis:
        return env_redis
    
    # Return the first host (localhost) by default for Celery
    # Celery will handle connection retries
    return redis_hosts[0]

# Create Celery application instance with Windows-friendly Redis URL
celery_app = Celery(
    "image_to_xlsx_tasks",
    broker=get_redis_url_for_celery(),
    backend=get_redis_url_for_celery(),
    include=[
        "app.tasks.batch_tasks",  # Import task modules
    ]
)

# Celery Configuration
celery_app.conf.update(
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    
    # Task execution settings
    task_always_eager=False,  # Don't execute tasks immediately in testing
    task_eager_propagates=True,  # Propagate exceptions in eager mode
    task_ignore_result=False,  # Store task results
    task_store_eager_result=True,  # Store results even in eager mode
    
    # Result backend settings
    result_expires=3600,  # Results expire after 1 hour
    result_persistent=True,  # Persist results to disk
    result_compression='gzip',  # Compress results
    
    # Worker settings
    worker_prefetch_multiplier=1,  # Disable prefetching for better load balancing
    worker_max_tasks_per_child=1000,  # Restart workers after 1000 tasks
    worker_disable_rate_limits=False,  # Enable rate limiting
    worker_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    worker_task_log_format="[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s",
    
    # Broker settings with enhanced Windows compatibility
    broker_connection_retry_on_startup=True,  # Retry connection on startup
    broker_connection_retry=True,  # Retry failed connections
    broker_connection_max_retries=10,  # Maximum 10 retries instead of unlimited
    broker_pool_limit=10,  # Connection pool size
    
    # Redis-specific broker settings with Windows compatibility
    broker_transport_options={
        'visibility_timeout': 3600,  # Task visibility timeout (1 hour)
        'fanout_prefix': True,  # Use fanout queue prefix
        'fanout_patterns': True,  # Enable fanout patterns
        'priority_steps': list(range(10)),  # Priority levels 0-9
        'queue_order_strategy': 'priority',  # Use priority-based ordering
        'sep': ':',  # Queue name separator
        'retry_policy': {
            'timeout': 10.0,  # Increased timeout for Windows
            'interval_start': 0.5,  # Start retry interval
            'interval_step': 0.5,  # Step for increasing intervals
            'interval_max': 5.0,  # Maximum interval between retries
            'max_retries': 5,  # Maximum retries per operation
        },
        # Windows-specific connection settings
        'socket_connect_timeout': 10,  # Socket connection timeout
        'socket_keepalive': True,  # Enable TCP keepalive
    },
    
    # Result backend transport options with Windows compatibility
    result_backend_transport_options={
        'retry_policy': {
            'timeout': 10.0,  # Increased timeout for Windows
            'interval_start': 0.5,
            'interval_step': 0.5,
            'interval_max': 5.0,
            'max_retries': 5,
        },
        'global_keyprefix': 'celery_result_',  # Prefix for result keys
        'result_chord_ordered': True,  # Maintain order in group results
        # Windows-specific settings
        'socket_connect_timeout': 10,
        'socket_keepalive': True,
    },
    
    # Task routing and queues
    task_default_queue='default',
    task_default_exchange='default',
    task_default_exchange_type='direct',
    task_default_routing_key='default',
    
    # Define task queues
    task_routes={
        'app.tasks.batch_tasks.process_single_image': {'queue': 'image_processing'},
        'app.tasks.batch_tasks.process_batch_images': {'queue': 'batch_processing'},
        'app.tasks.batch_tasks.cleanup_job_files': {'queue': 'cleanup'},
    },
    
    # Queue definitions
    task_queues=(
        Queue('default', Exchange('default'), routing_key='default'),
        Queue('image_processing', Exchange('image_processing'), routing_key='image_processing', 
              queue_arguments={'x-max-priority': 10}),
        Queue('batch_processing', Exchange('batch_processing'), routing_key='batch_processing',
              queue_arguments={'x-max-priority': 5}),
        Queue('cleanup', Exchange('cleanup'), routing_key='cleanup',
              queue_arguments={'x-max-priority': 1}),
    ),
    
    # Task time limits
    task_soft_time_limit=300,  # 5 minutes soft limit
    task_time_limit=600,  # 10 minutes hard limit
    
    # Task retry settings
    task_acks_late=True,  # Acknowledge tasks after completion
    task_reject_on_worker_lost=True,  # Reject tasks if worker is lost
    
    # Task rate limiting
    task_annotations={
        'app.tasks.batch_tasks.process_single_image': {
            'rate_limit': '10/m'  # 10 tasks per minute per worker
        },
        'app.tasks.batch_tasks.process_batch_images': {
            'rate_limit': '5/m'  # 5 batch tasks per minute per worker
        },
    },
    
    # Beat schedule (for periodic tasks)
    beat_schedule={
        'cleanup-expired-jobs': {
            'task': 'app.tasks.batch_tasks.cleanup_expired_jobs',
            'schedule': 3600.0,  # Run every hour
            'options': {'queue': 'cleanup', 'priority': 1}
        },
    },
    
    # Monitoring
    worker_send_task_events=True,  # Send task events for monitoring
    task_send_sent_event=True,  # Send task-sent events
    
    # Security
    worker_hijack_root_logger=False,  # Don't hijack root logger
    worker_log_color=False,  # Disable colored logs for production
    
    # Performance optimizations
    task_compression='gzip',  # Compress task messages
    
    # Soft shutdown timeout for graceful worker shutdown
    worker_soft_shutdown_timeout=60.0,  # 60 seconds
)

# Additional configuration for Railway deployment
if os.getenv('RAILWAY_ENVIRONMENT'):
    # Railway-specific configurations
    celery_app.conf.update(
        # Use Railway's environment for logging
        worker_log_format="[%(asctime)s: %(levelname)s] %(message)s",
        worker_task_log_format="[%(asctime)s: %(levelname)s][%(task_name)s] %(message)s",
        
        # Optimize for Railway's container environment
        worker_max_memory_per_child=512000,  # 512MB per worker process
        worker_processes=2,  # Number of worker processes
        
        # Use Railway's Redis configuration
        broker_connection_retry_on_startup=True,
        broker_connection_retry=True,
    )

# Task discovery for auto-registration
celery_app.autodiscover_tasks([
    'app.tasks',
])


def create_celery_app() -> Celery:
    """
    Factory function to create Celery app instance.
    
    Returns:
        Configured Celery application
    """
    return celery_app


# Export the configured app
__all__ = ['celery_app', 'create_celery_app']