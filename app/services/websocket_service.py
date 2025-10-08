"""
WebSocket service for real-time communication and connection management.

This service provides WebSocket connection management following FastAPI best practices
for handling WebSocket connections, disconnections, and message broadcasting.
"""

import json
import logging
import asyncio
from typing import Dict, Set, Optional, List, Any
from datetime import datetime
import uuid

from fastapi import WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState

from app.services.redis_service import RedisService, get_redis_service
from app.models.websocket import (
    WebSocketClientInfo, WebSocketMessageType, WebSocketTopics,
    ConnectionMessage, SubscriptionMessage, SystemMessage,
    WebSocketSubscriptionRequest, WebSocketAuthRequest
)
from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class WebSocketConnectionManager:
    """
    Manages WebSocket connections and message broadcasting following FastAPI patterns.
    
    Based on FastAPI's official WebSocket documentation:
    https://fastapi.tiangolo.com/advanced/websockets/
    """
    
    def __init__(self):
        # Active WebSocket connections list (simplified approach)
        self.active_connections: List[WebSocket] = []
        
        # Client metadata: websocket -> client info
        self.client_info: Dict[WebSocket, WebSocketClientInfo] = {}
        
        # Topic subscriptions: topic -> Set[WebSocket]
        self.topic_subscribers: Dict[str, Set[WebSocket]] = {}
        
        # Client subscriptions: websocket -> Set[topic]
        self.client_subscriptions: Dict[WebSocket, Set[str]] = {}
        
        # Redis service for pub/sub (optional)
        self.redis_service: Optional[RedisService] = None
        self.pubsub_task: Optional[asyncio.Task] = None
    
    async def initialize(self) -> None:
        """Initialize the WebSocket manager with Redis pub/sub."""
        try:
            self.redis_service = await get_redis_service()
            if self.redis_service:
                # Start Redis pub/sub listener
                await self.start_redis_pubsub_listener()
            logger.info("WebSocket manager initialized")
        except Exception as e:
            logger.error(f"Failed to initialize WebSocket manager: {e}")
    
    async def connect(self, websocket: WebSocket, client_id: Optional[str] = None, 
                     session_id: Optional[str] = None) -> str:
        """
        Accept a new WebSocket connection following FastAPI patterns.
        
        Args:
            websocket: WebSocket instance
            client_id: Optional client identifier  
            session_id: Optional session identifier
        
        Returns:
            Generated client ID
        """
        # Accept the WebSocket connection
        await websocket.accept()
        
        # Generate client ID if not provided
        if not client_id:
            client_id = str(uuid.uuid4())
        
        # Add to active connections
        self.active_connections.append(websocket)
        
        # Create client info
        client_info = WebSocketClientInfo(
            client_id=client_id,
            session_id=session_id,
            ip_address=websocket.client.host if websocket.client else None,
            user_agent=websocket.headers.get("user-agent")
        )
        self.client_info[websocket] = client_info
        
        # Initialize client subscriptions
        self.client_subscriptions[websocket] = set()
        
        # Send connection confirmation
        await self.send_personal_message(f"Connected successfully as {client_id}", websocket)
        
        logger.info(f"WebSocket client {client_id} connected (session: {session_id})")
        return client_id
    
    def disconnect(self, websocket: WebSocket) -> None:
        """
        Disconnect a WebSocket client following FastAPI patterns.
        
        Args:
            websocket: WebSocket instance to disconnect
        """
        # Remove from active connections
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        
        # Remove client subscriptions
        if websocket in self.client_subscriptions:
            topics = self.client_subscriptions[websocket].copy()
            for topic in topics:
                self._unsubscribe_websocket_from_topic(websocket, topic)
            del self.client_subscriptions[websocket]
        
        # Remove client info
        client_id = "unknown"
        if websocket in self.client_info:
            client_id = self.client_info[websocket].client_id
            del self.client_info[websocket]
        
        logger.info(f"WebSocket client {client_id} disconnected")
    
    async def send_personal_message(self, message: str, websocket: WebSocket) -> bool:
        """
        Send a personal message to a specific WebSocket connection.
        
        Args:
            message: Message to send
            websocket: WebSocket connection
            
        Returns:
            bool: True if message was sent successfully
        """
        try:
            await websocket.send_text(message)
            return True
        except WebSocketDisconnect:
            self.disconnect(websocket)
            return False
        except Exception as e:
            logger.error(f"Failed to send personal message: {e}")
            return False
    
    async def send_json_message(self, message: dict, websocket: WebSocket) -> bool:
        """
        Send a JSON message to a specific WebSocket connection.
        
        Args:
            message: JSON message to send
            websocket: WebSocket connection
            
        Returns:
            bool: True if message was sent successfully
        """
        try:
            await websocket.send_json(message)
            
            # Update client activity
            if websocket in self.client_info:
                self.client_info[websocket].update_activity()
            
            return True
        except WebSocketDisconnect:
            self.disconnect(websocket)
            return False
        except Exception as e:
            logger.error(f"Failed to send JSON message: {e}")
            return False
    
    async def broadcast(self, message: str) -> int:
        """
        Broadcast a message to all connected clients.
        
        Args:
            message: Message to broadcast
            
        Returns:
            Number of clients that received the message
        """
        sent_count = 0
        disconnected_connections = []
        
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
                sent_count += 1
            except WebSocketDisconnect:
                disconnected_connections.append(connection)
            except Exception as e:
                logger.error(f"Error broadcasting to connection: {e}")
                disconnected_connections.append(connection)
        
        # Clean up disconnected connections
        for connection in disconnected_connections:
            self.disconnect(connection)
        
        logger.debug(f"Broadcasted message to {sent_count} clients")
        return sent_count
    
    async def broadcast_json(self, message: dict) -> int:
        """
        Broadcast a JSON message to all connected clients.
        
        Args:
            message: JSON message to broadcast
            
        Returns:
            Number of clients that received the message
        """
        sent_count = 0
        disconnected_connections = []
        
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
                sent_count += 1
            except WebSocketDisconnect:
                disconnected_connections.append(connection)
            except Exception as e:
                logger.error(f"Error broadcasting JSON to connection: {e}")
                disconnected_connections.append(connection)
        
        # Clean up disconnected connections
        for connection in disconnected_connections:
            self.disconnect(connection)
        
        logger.debug(f"Broadcasted JSON message to {sent_count} clients")
        return sent_count
    
    async def broadcast_to_topic(self, topic: str, message: dict) -> int:
        """
        Broadcast a message to all clients subscribed to a topic.
        
        Args:
            topic: Topic name
            message: Message to broadcast
        
        Returns:
            Number of clients that received the message
        """
        if topic not in self.topic_subscribers:
            return 0
        
        subscribers = self.topic_subscribers[topic].copy()
        sent_count = 0
        disconnected_connections = []
        
        for websocket in subscribers:
            try:
                await websocket.send_json(message)
                sent_count += 1
            except WebSocketDisconnect:
                disconnected_connections.append(websocket)
            except Exception as e:
                logger.error(f"Error broadcasting to topic subscriber: {e}")
                disconnected_connections.append(websocket)
        
        # Clean up disconnected connections
        for connection in disconnected_connections:
            self.disconnect(connection)
        
        logger.debug(f"Broadcasted message to topic '{topic}': {sent_count}/{len(subscribers)} clients")
        return sent_count
    
    def subscribe_to_topic(self, websocket: WebSocket, topic: str) -> bool:
        """
        Subscribe a WebSocket to a topic.
        
        Args:
            websocket: WebSocket connection
            topic: Topic name
            
        Returns:
            bool: True if subscription was successful
        """
        if websocket not in self.active_connections:
            return False
        
        # Add to topic subscribers
        if topic not in self.topic_subscribers:
            self.topic_subscribers[topic] = set()
        self.topic_subscribers[topic].add(websocket)
        
        # Add to client subscriptions
        if websocket not in self.client_subscriptions:
            self.client_subscriptions[websocket] = set()
        self.client_subscriptions[websocket].add(topic)
        
        # Update client info
        if websocket in self.client_info:
            self.client_info[websocket].add_subscription(topic)
        
        logger.debug(f"WebSocket subscribed to topic: {topic}")
        return True
    
    def unsubscribe_from_topic(self, websocket: WebSocket, topic: str) -> bool:
        """
        Unsubscribe a WebSocket from a topic.
        
        Args:
            websocket: WebSocket connection
            topic: Topic name
            
        Returns:
            bool: True if unsubscription was successful
        """
        if websocket not in self.active_connections:
            return False
        
        self._unsubscribe_websocket_from_topic(websocket, topic)
        
        logger.debug(f"WebSocket unsubscribed from topic: {topic}")
        return True
    
    def _unsubscribe_websocket_from_topic(self, websocket: WebSocket, topic: str) -> None:
        """Unsubscribe a WebSocket from a single topic (internal method)."""
        # Remove from topic subscribers
        if topic in self.topic_subscribers:
            self.topic_subscribers[topic].discard(websocket)
            if not self.topic_subscribers[topic]:
                del self.topic_subscribers[topic]
        
        # Remove from client subscriptions
        if websocket in self.client_subscriptions:
            self.client_subscriptions[websocket].discard(topic)
        
        # Update client info
        if websocket in self.client_info:
            self.client_info[websocket].remove_subscription(topic)
    
    def authenticate_client(self, websocket: WebSocket, session_id: Optional[str] = None) -> bool:
        """
        Authenticate a WebSocket client.
        
        Args:
            websocket: WebSocket connection
            session_id: Session identifier for authentication
        
        Returns:
            bool: True if authentication was successful
        """
        if websocket not in self.client_info:
            return False
        
        # Update client info with session
        self.client_info[websocket].session_id = session_id
        
        # Auto-subscribe to session-specific topics
        if session_id:
            self.subscribe_to_topic(websocket, WebSocketTopics.session_topic(session_id))
        
        client_id = self.client_info[websocket].client_id
        logger.info(f"Client {client_id} authenticated with session {session_id}")
        return True
    
    async def handle_client_message(self, websocket: WebSocket, message: Dict[str, Any]) -> None:
        """
        Handle incoming message from a WebSocket client.
        
        Args:
            websocket: WebSocket connection
            message: Parsed message data
        """
        try:
            message_type = message.get("action", "unknown")
            
            if message_type == "subscribe":
                # Handle subscription request
                topics = message.get("topics", [])
                job_ids = message.get("job_ids", [])
                
                # Add job-specific topics
                for job_id in job_ids:
                    topics.extend([
                        WebSocketTopics.job_topic(job_id),
                        WebSocketTopics.job_status_topic(job_id)
                    ])
                
                # Subscribe to all topics
                for topic in topics:
                    self.subscribe_to_topic(websocket, topic)
                
                # Send confirmation
                await self.send_json_message({
                    "type": "subscription",
                    "event": "subscribed",
                    "topics": topics,
                    "message": f"Subscribed to {len(topics)} topics"
                }, websocket)
                
            elif message_type == "unsubscribe":
                # Handle unsubscription request
                topics = message.get("topics", [])
                job_ids = message.get("job_ids", [])
                
                # Add job-specific topics
                for job_id in job_ids:
                    topics.extend([
                        WebSocketTopics.job_topic(job_id),
                        WebSocketTopics.job_status_topic(job_id)
                    ])
                
                # Unsubscribe from all topics
                for topic in topics:
                    self.unsubscribe_from_topic(websocket, topic)
                
                # Send confirmation
                await self.send_json_message({
                    "type": "subscription",
                    "event": "unsubscribed", 
                    "topics": topics,
                    "message": f"Unsubscribed from {len(topics)} topics"
                }, websocket)
                
            elif message_type == "authenticate":
                # Handle authentication request
                session_id = message.get("session_id")
                if self.authenticate_client(websocket, session_id):
                    await self.send_json_message({
                        "type": "connection",
                        "event": "authenticated",
                        "message": "Authentication successful"
                    }, websocket)
                
            else:
                # Unknown message type
                await self.send_json_message({
                    "type": "system",
                    "level": "warning",
                    "message": f"Unknown message type: {message_type}"
                }, websocket)
                
        except Exception as e:
            logger.error(f"Error handling client message: {e}")
            await self.send_json_message({
                "type": "system",
                "level": "error",
                "message": f"Error processing message: {str(e)}"
            }, websocket)
    
    async def start_redis_pubsub_listener(self) -> None:
        """
        Start Redis pub/sub listener following Redis best practices.
        
        Based on Redis pub/sub documentation:
        https://redis.io/docs/latest/develop/pubsub/
        
        Redis pub/sub exhibits at-most-once message delivery semantics.
        Pattern matching subscriptions are supported with glob-style patterns.
        
        Note: WebSocket functionality works without Redis. Redis enhances 
        broadcasting across multiple server instances.
        """
        if not self.redis_service:
            logger.info("Redis service not available, WebSocket will work in standalone mode")
            return
        
        try:
            # Test Redis connection before starting pub/sub
            await self.redis_service.client.ping()
            
            # Create a pubsub client following Redis patterns
            pubsub = self.redis_service.client.pubsub()
            
            # Direct channel subscriptions (exact matches)
            direct_channels = [
                WebSocketTopics.JOB_PROGRESS,
                WebSocketTopics.JOB_COMPLETED, 
                WebSocketTopics.JOB_ERROR,
                WebSocketTopics.SYSTEM_NOTIFICATIONS
            ]
            
            # Pattern subscriptions for dynamic topics (Redis glob patterns)
            pattern_channels = [
                "job:*",        # All job-specific topics
                "session:*",    # All session-specific topics  
                "job_status:*"  # All job status topics
            ]
            
            # Subscribe to direct channels
            if direct_channels:
                await pubsub.subscribe(*direct_channels)
                logger.debug(f"Subscribed to {len(direct_channels)} direct channels")
            
            # Subscribe to pattern channels using PSUBSCRIBE
            for pattern in pattern_channels:
                await pubsub.psubscribe(pattern)
                logger.debug(f"Pattern subscribed to: {pattern}")
            
            # Start listener task
            self.pubsub_task = asyncio.create_task(self._pubsub_listener_loop(pubsub))
            logger.info(f"Started Redis pub/sub listener: {len(direct_channels)} channels, {len(pattern_channels)} patterns")
                
        except Exception as e:
            logger.error(f"Error starting Redis pub/sub listener: {e}")
    
    async def _pubsub_listener_loop(self, pubsub) -> None:
        """
        Redis pub/sub listener loop following Redis message format patterns.
        
        Message format from Redis documentation:
        - 'subscribe'/'unsubscribe': subscription confirmations
        - 'message': direct channel message
        - 'pmessage': pattern-matched message
        """
        try:
            logger.info("Redis pub/sub listener started")
            async for message in pubsub.listen():
                try:
                    # Skip None messages (can happen during connection issues)
                    if message is None:
                        continue
                        
                    msg_type = message.get('type')
                    
                    # Handle actual message types only (ignore subscription confirmations in debug)
                    if msg_type == 'message':
                        # Direct channel message
                        channel = self._decode_redis_data(message.get('channel', ''))
                        data_raw = self._decode_redis_data(message.get('data', ''))
                        
                        # Skip empty messages
                        if not data_raw or not channel:
                            continue
                            
                        try:
                            data = json.loads(data_raw)
                            await self.broadcast_to_topic(channel, data)
                            logger.debug(f"Broadcasted message to '{channel}': {data.get('type', 'unknown')}")
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in Redis message on '{channel}': {data_raw[:100]}")
                        
                    elif msg_type == 'pmessage':
                        # Pattern-matched message
                        pattern = self._decode_redis_data(message.get('pattern', ''))
                        channel = self._decode_redis_data(message.get('channel', ''))
                        data_raw = self._decode_redis_data(message.get('data', ''))
                        
                        # Skip empty messages
                        if not data_raw or not channel:
                            continue
                            
                        try:
                            data = json.loads(data_raw)
                            await self.broadcast_to_topic(channel, data)
                            logger.debug(f"Broadcasted pattern message to '{channel}' (pattern: {pattern}): {data.get('type', 'unknown')}")
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in Redis pattern message on '{channel}': {data_raw[:100]}")
                        
                    elif msg_type in ['subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe']:
                        # Subscription confirmation messages - log at debug level
                        channel = self._decode_redis_data(message.get('channel', 'unknown'))
                        count = message.get('data', 0)
                        logger.debug(f"Redis subscription {msg_type}: {channel} (active subscriptions: {count})")
                        
                    else:
                        # Unknown message type - log for debugging
                        logger.debug(f"Unknown Redis message type: {msg_type}, data: {message}")
                        
                except Exception as e:
                    logger.error(f"Error processing Redis message: {e} - Message: {message}")
                    
        except asyncio.CancelledError:
            logger.info("Redis pub/sub listener cancelled")
        except Exception as e:
            logger.error(f"Redis pub/sub listener error: {e}")
        finally:
            try:
                # Clean shutdown following Redis patterns
                await pubsub.unsubscribe()
                await pubsub.punsubscribe() 
                await pubsub.aclose()
                logger.info("Redis pub/sub connection closed cleanly")
            except Exception as e:
                logger.error(f"Error closing Redis pub/sub: {e}")
    
    def _decode_redis_data(self, data) -> str:
        """
        Safely decode Redis data that can be either bytes or string.
        
        Based on redis-py documentation, Redis data can be returned as:
        - bytes (default behavior)  
        - str (when decode_responses=True)
        """
        if data is None:
            return ""
        if isinstance(data, bytes):
            return data.decode('utf-8')
        if isinstance(data, str):
            return data
        return str(data)
    
    async def stop_redis_pubsub_listener(self) -> None:
        """Stop the Redis pub/sub listener."""
        if self.pubsub_task and not self.pubsub_task.done():
            self.pubsub_task.cancel()
            try:
                await self.pubsub_task
            except asyncio.CancelledError:
                pass
            self.pubsub_task = None
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get WebSocket connection statistics."""
        return {
            "active_connections": len(self.active_connections),
            "total_subscriptions": sum(len(subs) for subs in self.client_subscriptions.values()),
            "active_topics": len(self.topic_subscribers),
            "clients_by_topic": {
                topic: len(subscribers) 
                for topic, subscribers in self.topic_subscribers.items()
            }
        }
    
    def cleanup_inactive_connections(self) -> int:
        """Clean up inactive WebSocket connections."""
        inactive_connections = []
        
        for websocket in self.active_connections:
            if websocket.client_state != WebSocketState.CONNECTED:
                inactive_connections.append(websocket)
        
        for websocket in inactive_connections:
            self.disconnect(websocket)
        
        if inactive_connections:
            logger.info(f"Cleaned up {len(inactive_connections)} inactive WebSocket connections")
        
        return len(inactive_connections)


# Global WebSocket manager instance
websocket_manager = WebSocketConnectionManager()


async def get_websocket_manager() -> WebSocketConnectionManager:
    """
    Dependency to get WebSocket manager instance.
    
    Returns:
        WebSocketConnectionManager instance
    """
    if not websocket_manager.redis_service:
        await websocket_manager.initialize()
    return websocket_manager
