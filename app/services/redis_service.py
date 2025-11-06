"""
Redis Service for job queue management and caching.

This service provides Redis integration for the FastAPI application,
including connection pooling, job queue management, and health checks.
Uses async Redis for optimal performance with FastAPI.
"""

import json
import logging
import asyncio
from typing import Optional, Dict, Any, List, Callable, Union
from datetime import datetime, timedelta

import redis.asyncio as redis
from redis.asyncio import ConnectionPool
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError
from redis.retry import Retry
from redis.backoff import ExponentialBackoff

from app.core.config import settings
from app.models.websocket import WebSocketMessageType, WebSocketTopics

logger = logging.getLogger(__name__)


class RedisService:
    """Redis service for job queue and caching operations."""
    
    def __init__(self):
        self.pool: Optional[ConnectionPool] = None
        self.client: Optional[redis.Redis] = None
        self._is_connected = False
        self._connection_attempts = 0
        self._max_connection_attempts = 3
    
    async def _try_connection_with_host(self, host: str, port: int = 6379) -> redis.Redis:
        """Try to connect to Redis with a specific host."""
        logger.info(f"Attempting Redis connection to {host}:{port}")
        
        pool = ConnectionPool(
            host=host,
            port=port,
            db=0,
            max_connections=settings.redis_max_connections,
            retry_on_timeout=True,
            socket_connect_timeout=settings.redis_connect_timeout,
            socket_timeout=settings.redis_socket_timeout,
            health_check_interval=30,
            decode_responses=True
        )
        
        client = redis.Redis(
            connection_pool=pool,
            decode_responses=True,
            retry_on_error=[RedisConnectionError],
            retry=Retry(ExponentialBackoff(cap=5, base=1), 3)
        )
        
        # Test the connection
        await client.ping()
        logger.info(f"Successfully connected to Redis at {host}:{port}")
        return client, pool
    
    async def initialize(self) -> None:
        """Initialize Redis connection pool and client with fallback hosts."""
        from urllib.parse import urlparse

        # Parse Redis URL from settings
        redis_url = settings.redis_url
        logger.info(f"Parsing Redis URL: {redis_url[:20]}...")  # Log partial URL for security

        parsed = urlparse(redis_url)
        host = parsed.hostname or "localhost"
        port = parsed.port or 6379
        password = parsed.password

        # List of connection configurations to try
        connections_to_try = [
            (host, port, password),  # Try configured host first
            ("localhost", 6379, None),  # Fallback to localhost
            ("127.0.0.1", 6379, None),  # Fallback to 127.0.0.1
        ]

        last_error = None

        for conn_host, conn_port, conn_password in connections_to_try:
            try:
                self._connection_attempts += 1
                logger.info(f"Redis connection attempt {self._connection_attempts} using host: {conn_host}:{conn_port}")

                # Create connection pool with password if provided
                pool = ConnectionPool(
                    host=conn_host,
                    port=conn_port,
                    db=0,
                    password=conn_password,
                    max_connections=settings.redis_max_connections,
                    retry_on_timeout=True,
                    socket_connect_timeout=settings.redis_connect_timeout,
                    socket_timeout=settings.redis_socket_timeout,
                    health_check_interval=30,
                    decode_responses=True
                )

                client = redis.Redis(
                    connection_pool=pool,
                    decode_responses=True,
                    retry_on_error=[RedisConnectionError],
                    retry=Retry(ExponentialBackoff(cap=5, base=1), 3)
                )

                # Test the connection
                await client.ping()

                self.client = client
                self.pool = pool
                self._is_connected = True
                logger.info(f"Redis connection established successfully with {conn_host}:{conn_port}")
                return

            except Exception as e:
                logger.warning(f"Failed to connect to Redis at {conn_host}:{conn_port}: {e}")
                last_error = e

                # Clean up failed connection attempts
                try:
                    if client:
                        await client.aclose()
                    if pool:
                        await pool.aclose()
                except:
                    pass

                # Continue to next connection
                continue

        # If we get here, all connection attempts failed
        logger.error(f"All Redis connection attempts failed. Last error: {last_error}")
        logger.warning("⚠️  Redis unavailable - service will run in DEGRADED MODE (no real-time updates)")
        self._is_connected = False
        # DON'T raise - allow service to continue without Redis
        # raise ConnectionError(f"Failed to connect to Redis after trying {len(connections_to_try)} connections. Last error: {last_error}")
    
    async def close(self) -> None:
        """Close Redis connections."""
        if self.client:
            await self.client.aclose()
        if self.pool:
            await self.pool.aclose()
        self._is_connected = False
        logger.info("Redis connections closed")
    
    async def health_check(self) -> bool:
        """Check Redis connection health."""
        try:
            if not self.client:
                return False
            await self.client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False
    
    # Job Management Methods
    
    async def create_job(self, job_id: str, job_data: Dict[str, Any]) -> bool:
        """
        Create a new job in Redis with verification.

        Args:
            job_id: Unique job identifier
            job_data: Job metadata and configuration

        Returns:
            bool: True if job was created successfully
        """
        if not self._is_connected or not self.client:
            logger.warning(f"Redis unavailable - cannot create job {job_id} (degraded mode)")
            return False

        try:
            # Make a copy to avoid modifying the original
            job_data_copy = job_data.copy()
            
            # Add timestamp
            job_data_copy['created_at'] = datetime.utcnow().isoformat()
            job_data_copy['updated_at'] = datetime.utcnow().isoformat()
            
            # Convert complex objects to JSON (same as update_job method)
            for field in ['images', 'results', 'errors', 'download_urls', 'generated_files']:
                if field in job_data_copy and isinstance(job_data_copy[field], (list, dict)):
                    job_data_copy[field] = json.dumps(job_data_copy[field])
            
            # Store job data
            job_key = f"job:{job_id}"
            result = await self.client.hset(job_key, mapping=job_data_copy)
            
            # Verify the data was stored correctly
            verification = await self.client.hgetall(job_key)
            if not verification:
                logger.error(f"Job {job_id} creation verification failed - no data found")
                return False
            
            # Check that key fields are present
            required_fields = ['job_id', 'session_id', 'status']
            for field in required_fields:
                if field in job_data_copy:  # Only check if it was in the original data
                    field_key = field.encode('utf-8') if isinstance(list(verification.keys())[0], bytes) else field
                    if field_key not in verification:
                        logger.error(f"Job {job_id} creation verification failed - missing field {field}")
                        return False
            
            # Set expiration (24 hours)
            await self.client.expire(job_key, settings.job_expiry_seconds)
            
            # Add to jobs index
            await self.client.sadd("jobs:active", job_id)
            
            logger.info(f"Job {job_id} created successfully with {len(verification)} fields")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create job {job_id}: {e}")
            return False
    
    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job data from Redis.

        Args:
            job_id: Job identifier

        Returns:
            Dict containing job data or None if not found
        """
        if not self._is_connected or not self.client:
            logger.warning(f"Redis unavailable - cannot get job {job_id} (degraded mode)")
            return None

        try:
            job_key = f"job:{job_id}"
            job_data = await self.client.hgetall(job_key)
            
            if not job_data:
                return None
            
            # Ensure all keys and values are strings (handle decode_responses issues)
            job_data_str = {}
            for key, value in job_data.items():
                # Convert bytes to string if needed
                str_key = key.decode('utf-8') if isinstance(key, bytes) else key
                str_value = value.decode('utf-8') if isinstance(value, bytes) else value
                job_data_str[str_key] = str_value
            
            # Parse JSON fields
            for field in ['images', 'results', 'errors', 'download_urls', 'generated_files']:
                if field in job_data_str and job_data_str[field]:
                    try:
                        job_data_str[field] = json.loads(job_data_str[field])
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse JSON field {field} for job {job_id}")
            
            return job_data_str
            
        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            return None
    
    async def update_job(self, job_id: str, updates: Dict[str, Any]) -> bool:
        """
        Update job data in Redis, preserving existing fields.

        Args:
            job_id: Job identifier
            updates: Fields to update

        Returns:
            bool: True if update was successful
        """
        if not self._is_connected or not self.client:
            logger.debug(f"Redis unavailable - cannot update job {job_id} (degraded mode)")
            return False

        try:
            # Make a copy to avoid modifying the original
            updates_copy = updates.copy()
            
            # Add update timestamp
            updates_copy['updated_at'] = datetime.utcnow().isoformat()
            
            # Convert complex objects to JSON
            for field in ['images', 'results', 'errors', 'download_urls', 'generated_files']:
                if field in updates_copy and isinstance(updates_copy[field], (list, dict)):
                    updates_copy[field] = json.dumps(updates_copy[field])
            
            job_key = f"job:{job_id}"
            
            # Verify job exists first
            exists = await self.client.exists(job_key)
            if not exists:
                logger.warning(f"Attempted to update non-existent job {job_id}")
                return False
            
            # Update the specific fields
            await self.client.hset(job_key, mapping=updates_copy)
            
            # Verify the update worked
            verification = await self.client.hget(job_key, 'updated_at')
            if not verification:
                logger.error(f"Job {job_id} update verification failed")
                return False
            
            logger.info(f"Job {job_id} updated successfully with {len(updates_copy)} fields")
            
            # Automatically publish WebSocket update if status changed
            if 'status' in updates_copy or 'progress' in updates_copy:
                try:
                    # Get updated job data for WebSocket publishing
                    updated_job_data = await self.get_job(job_id)
                    if updated_job_data:
                        await self.publish_job_update(job_id, updated_job_data)
                except Exception as e:
                    logger.warning(f"Failed to publish WebSocket update for job {job_id}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update job {job_id}: {e}")
            return False
    
    async def delete_job(self, job_id: str) -> bool:
        """
        Delete job from Redis.
        
        Args:
            job_id: Job identifier
        
        Returns:
            bool: True if deletion was successful
        """
        try:
            job_key = f"job:{job_id}"
            
            # Remove job data
            await self.client.delete(job_key)
            
            # Remove from active jobs index
            await self.client.srem("jobs:active", job_id)
            
            logger.info(f"Job {job_id} deleted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete job {job_id}: {e}")
            return False
    
    async def get_active_jobs(self) -> List[str]:
        """
        Get list of active job IDs.
        
        Returns:
            List of active job IDs
        """
        try:
            job_ids = await self.client.smembers("jobs:active")
            return list(job_ids) if job_ids else []
        except Exception as e:
            logger.error(f"Failed to get active jobs: {e}")
            return []
    
    # Queue Management Methods
    
    async def enqueue_task(self, queue_name: str, task_data: Dict[str, Any]) -> bool:
        """
        Add task to Redis queue.
        
        Args:
            queue_name: Name of the queue
            task_data: Task data to enqueue
        
        Returns:
            bool: True if task was enqueued successfully
        """
        try:
            task_json = json.dumps(task_data)
            await self.client.lpush(f"queue:{queue_name}", task_json)
            logger.debug(f"Task enqueued to {queue_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to enqueue task to {queue_name}: {e}")
            return False
    
    async def dequeue_task(self, queue_name: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
        """
        Remove and get task from Redis queue.
        
        Args:
            queue_name: Name of the queue
            timeout: Timeout in seconds for blocking operation
        
        Returns:
            Task data or None if timeout
        """
        try:
            result = await self.client.brpop(f"queue:{queue_name}", timeout=timeout)
            if result:
                _, task_json = result
                return json.loads(task_json)
            return None
        except Exception as e:
            logger.error(f"Failed to dequeue task from {queue_name}: {e}")
            return None
    
    async def get_queue_length(self, queue_name: str) -> int:
        """
        Get number of tasks in queue.
        
        Args:
            queue_name: Name of the queue
        
        Returns:
            Number of tasks in queue
        """
        try:
            return await self.client.llen(f"queue:{queue_name}")
        except Exception as e:
            logger.error(f"Failed to get queue length for {queue_name}: {e}")
            return 0
    
    # Caching Methods
    
    async def set_cache(self, key: str, value: Any, expire_seconds: int = 3600) -> bool:
        """
        Set value in Redis cache.
        
        Args:
            key: Cache key
            value: Value to cache
            expire_seconds: Expiration time in seconds
        
        Returns:
            bool: True if cache was set successfully
        """
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            
            await self.client.setex(f"cache:{key}", expire_seconds, value)
            return True
        except Exception as e:
            logger.error(f"Failed to set cache for key {key}: {e}")
            return False
    
    async def get_cache(self, key: str) -> Optional[Any]:
        """
        Get value from Redis cache.
        
        Args:
            key: Cache key
        
        Returns:
            Cached value or None if not found
        """
        try:
            value = await self.client.get(f"cache:{key}")
            if value:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return None
        except Exception as e:
            logger.error(f"Failed to get cache for key {key}: {e}")
            return None
    
    async def delete_cache(self, key: str) -> bool:
        """
        Delete value from Redis cache.
        
        Args:
            key: Cache key
        
        Returns:
            bool: True if deletion was successful
        """
        try:
            result = await self.client.delete(f"cache:{key}")
            return result > 0
        except Exception as e:
            logger.error(f"Failed to delete cache for key {key}: {e}")
            return False
    
    # Pub/Sub Methods for WebSocket Integration
    
    async def publish_message(self, topic: str, message: Union[Dict[str, Any], WebSocketMessageType]) -> bool:
        """
        Publish a message to a Redis pub/sub topic.
        If no subscribers, buffer the message for later retrieval.

        Args:
            topic: Topic name to publish to
            message: Message data (dict or WebSocket message model)

        Returns:
            bool: True if message was published successfully
        """
        if not self._is_connected or not self.client:
            logger.debug(f"Redis unavailable - cannot publish to topic '{topic}' (degraded mode)")
            return False

        try:
            # Convert Pydantic models to dict for JSON serialization
            if hasattr(message, 'model_dump'):
                message_data = message.model_dump(mode='json')
            else:
                message_data = message

            # Ensure timestamp is included
            if 'timestamp' not in message_data:
                message_data['timestamp'] = datetime.utcnow().isoformat()

            message_json = json.dumps(message_data)
            subscribers = await self.client.publish(topic, message_json)

            # CRITICAL FIX: Buffer message if no subscribers
            # This prevents message loss when WebSocket connects after processing starts
            if subscribers == 0:
                logger.warning(f"⚠️ No subscribers for topic '{topic}', buffering message type: {message_data.get('type', 'unknown')}")
                await self.buffer_message(topic, message_data)
            else:
                logger.info(f"📢 Published message to topic '{topic}': {subscribers} subscribers, message type: {message_data.get('type', 'unknown')}")

            return True

        except Exception as e:
            logger.error(f"Failed to publish message to topic '{topic}': {e}")
            return False
    
    async def publish_job_update(self, job_id: str, job_data: Dict[str, Any]) -> bool:
        """
        Publish job status update to relevant WebSocket topics.
        
        Args:
            job_id: Job identifier
            job_data: Job data dictionary
        
        Returns:
            bool: True if all publications were successful
        """
        try:
            from app.models.websocket import JobProgressUpdate, JobCompletedMessage, JobErrorMessage
            
            status = job_data.get('status', 'unknown')
            session_id = job_data.get('session_id')
            
            # Create appropriate WebSocket message based on job status
            if status in ['completed', 'failed', 'partially_completed']:
                # Job completed message
                message = JobCompletedMessage(
                    job_id=job_id,
                    status=status,
                    successful_images=job_data.get('processed_images', 0),
                    failed_images=max(0, job_data.get('total_images', 0) - job_data.get('processed_images', 0)),
                    download_urls=job_data.get('download_urls', []),
                    primary_download_url=job_data.get('download_url'),
                    processing_time=job_data.get('processing_time', 0.0),
                    expires_at=datetime.utcnow() + timedelta(hours=24),  # Default expiration
                    errors=job_data.get('errors', []),
                    session_id=session_id
                )
            elif status == 'failed' and job_data.get('error'):
                # Job error message
                message = JobErrorMessage(
                    job_id=job_id,
                    error=job_data.get('error', 'Unknown error'),
                    session_id=session_id
                )
            else:
                # Job progress message
                message = JobProgressUpdate(
                    job_id=job_id,
                    status=status,
                    progress=int(job_data.get('progress', 0)),
                    total_images=job_data.get('total_images', 0),
                    processed_images=job_data.get('processed_images', 0),
                    current_image=job_data.get('current_image'),
                    processing_time=job_data.get('processing_time'),
                    session_id=session_id
                )
            
            # Publish to multiple topics for different subscription patterns
            topics_to_publish = [
                WebSocketTopics.job_topic(job_id),  # job:{job_id}
                WebSocketTopics.job_status_topic(job_id),  # job_status:{job_id}
            ]
            
            # Add session-specific topic if session_id is available
            if session_id:
                topics_to_publish.append(WebSocketTopics.session_topic(session_id))
            
            # Add general progress topic
            if status == 'processing':
                topics_to_publish.append(WebSocketTopics.JOB_PROGRESS)
            elif status in ['completed', 'failed', 'partially_completed']:
                topics_to_publish.append(WebSocketTopics.JOB_COMPLETED)
            
            # Publish to all relevant topics
            success_count = 0
            for topic in topics_to_publish:
                if await self.publish_message(topic, message):
                    success_count += 1
            
            logger.info(f"Published job update for {job_id} to {success_count}/{len(topics_to_publish)} topics")
            return success_count == len(topics_to_publish)
            
        except Exception as e:
            logger.error(f"Failed to publish job update for {job_id}: {e}")
            return False
    
    async def subscribe_to_topics(self, topics: List[str], callback: Callable[[str, Dict[str, Any]], None]) -> Optional[redis.client.PubSub]:
        """
        Subscribe to Redis pub/sub topics with a callback function.
        
        Args:
            topics: List of topic names to subscribe to
            callback: Async callback function that receives (topic, message_data)
        
        Returns:
            PubSub instance or None if subscription failed
        """
        try:
            pubsub = self.client.pubsub()
            
            # Subscribe to all topics
            for topic in topics:
                await pubsub.subscribe(topic)
            
            logger.info(f"Subscribed to topics: {topics}")
            
            # Start message handling task
            asyncio.create_task(self._handle_pubsub_messages(pubsub, callback))
            
            return pubsub
            
        except Exception as e:
            logger.error(f"Failed to subscribe to topics {topics}: {e}")
            return None
    
    async def _handle_pubsub_messages(self, pubsub: redis.client.PubSub, 
                                    callback: Callable[[str, Dict[str, Any]], None]) -> None:
        """
        Handle incoming pub/sub messages and call the callback function.
        
        Args:
            pubsub: Redis PubSub instance
            callback: Callback function to handle messages
        """
        try:
            async for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        # Parse JSON message
                        topic = message['channel']
                        data = json.loads(message['data'])
                        
                        # Call the callback function
                        await callback(topic, data)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse pub/sub message: {e}")
                    except Exception as e:
                        logger.error(f"Error in pub/sub callback: {e}")
                        
        except Exception as e:
            logger.error(f"Error in pub/sub message handling: {e}")
        finally:
            try:
                await pubsub.unsubscribe()
                await pubsub.aclose()
            except:
                pass
    
    async def unsubscribe_from_topics(self, pubsub: redis.client.PubSub, topics: List[str]) -> bool:
        """
        Unsubscribe from Redis pub/sub topics.
        
        Args:
            pubsub: Redis PubSub instance
            topics: List of topic names to unsubscribe from
        
        Returns:
            bool: True if unsubscription was successful
        """
        try:
            for topic in topics:
                await pubsub.unsubscribe(topic)
            
            logger.info(f"Unsubscribed from topics: {topics}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unsubscribe from topics {topics}: {e}")
            return False
    
    async def get_topic_subscribers(self, topic: str) -> int:
        """
        Get the number of subscribers for a topic.
        
        Args:
            topic: Topic name
        
        Returns:
            Number of subscribers
        """
        try:
            # Use PUBSUB NUMSUB command to get subscriber count
            result = await self.client.execute_command('PUBSUB', 'NUMSUB', topic)
            if result and len(result) >= 2:
                return int(result[1])
            return 0
        except Exception as e:
            logger.error(f"Failed to get subscriber count for topic '{topic}': {e}")
            return 0
    
    async def list_active_topics(self) -> List[str]:
        """
        Get list of active pub/sub topics.

        Returns:
            List of active topic names
        """
        try:
            # Use PUBSUB CHANNELS command to get active channels
            result = await self.client.execute_command('PUBSUB', 'CHANNELS')
            return list(result) if result else []
        except Exception as e:
            logger.error(f"Failed to list active topics: {e}")
            return []

    # Message Buffering Methods (for handling pub/sub message loss)

    async def buffer_message(self, topic: str, message_data: Dict[str, Any]) -> bool:
        """
        Buffer a message in Redis for later retrieval.
        Used when no subscribers are available to receive the message.

        Args:
            topic: Topic name the message belongs to
            message_data: Message data to buffer

        Returns:
            bool: True if message was buffered successfully
        """
        try:
            buffer_key = f"buffer:{topic}"
            message_json = json.dumps(message_data)

            # Add message to buffer list
            await self.client.lpush(buffer_key, message_json)

            # Set expiration on buffer (1 hour) to prevent accumulation
            await self.client.expire(buffer_key, 3600)

            logger.info(f"💾 Buffered message for topic '{topic}', type: {message_data.get('type', 'unknown')}")
            return True

        except Exception as e:
            logger.error(f"Failed to buffer message for topic '{topic}': {e}")
            return False

    async def get_buffered_messages(self, topic: str) -> List[Dict[str, Any]]:
        """
        Retrieve all buffered messages for a topic.
        Messages are returned in FIFO order (oldest first).

        Args:
            topic: Topic name to retrieve messages for

        Returns:
            List of buffered messages
        """
        try:
            buffer_key = f"buffer:{topic}"

            # Get all messages from buffer
            messages_json = await self.client.lrange(buffer_key, 0, -1)

            if not messages_json:
                return []

            # Parse JSON messages
            messages = []
            for msg_json in reversed(messages_json):  # Reverse to get FIFO order
                try:
                    messages.append(json.loads(msg_json))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse buffered message: {e}")

            logger.info(f"📤 Retrieved {len(messages)} buffered messages for topic '{topic}'")
            return messages

        except Exception as e:
            logger.error(f"Failed to get buffered messages for topic '{topic}': {e}")
            return []

    async def clear_buffered_messages(self, topic: str) -> bool:
        """
        Clear all buffered messages for a topic.
        Should be called after messages have been successfully delivered.

        Args:
            topic: Topic name to clear messages for

        Returns:
            bool: True if clearing was successful
        """
        try:
            buffer_key = f"buffer:{topic}"
            result = await self.client.delete(buffer_key)

            if result > 0:
                logger.info(f"🗑️ Cleared buffered messages for topic '{topic}'")

            return True

        except Exception as e:
            logger.error(f"Failed to clear buffered messages for topic '{topic}': {e}")
            return False

    # Cleanup Methods
    
    async def cleanup_expired_jobs(self) -> int:
        """
        Clean up expired jobs from Redis.
        
        Returns:
            Number of jobs cleaned up
        """
        try:
            active_jobs = await self.get_active_jobs()
            cleaned = 0
            
            for job_id in active_jobs:
                job_key = f"job:{job_id}"
                exists = await self.client.exists(job_key)
                
                if not exists:
                    # Job expired, remove from active index
                    await self.client.srem("jobs:active", job_id)
                    cleaned += 1
            
            if cleaned > 0:
                logger.info(f"Cleaned up {cleaned} expired jobs")
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Failed to cleanup expired jobs: {e}")
            return 0


# Global Redis service instance
redis_service = RedisService()


async def get_redis_service() -> RedisService:
    """
    Dependency to get Redis service instance.
    Handles connection failures gracefully.

    Returns:
        RedisService instance (may be in degraded mode if Redis unavailable)
    """
    if not redis_service._is_connected:
        try:
            await redis_service.initialize()
        except Exception as e:
            logger.warning(f"Redis initialization failed, continuing in degraded mode: {e}")
            # Return service anyway - it will operate in degraded mode
    return redis_service