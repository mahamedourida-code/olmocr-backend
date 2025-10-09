"""
WebSocket endpoints for real-time communication.

MVP Version - Simplified to session-based WebSocket only.
This provides real-time job status updates for batch processing.
"""

import json
import logging
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends

from app.services.websocket_service import get_websocket_manager, WebSocketConnectionManager
from app.models.websocket import WebSocketTopics
from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/ws", tags=["WebSocket"])


# MVP: Removed /connect and /jobs/{job_id} endpoints - only session-based needed


@router.websocket("/session/{session_id}")
async def websocket_session_endpoint(
    websocket: WebSocket,
    session_id: str,
    websocket_manager: WebSocketConnectionManager = Depends(get_websocket_manager)
):
    """
    WebSocket endpoint for monitoring all jobs in a session following FastAPI patterns.
    
    This endpoint automatically subscribes the client to session-specific topics
    and provides real-time updates for all jobs in the specified session.
    
    Path Parameters:
    - session_id: The session identifier to monitor
    """
    # Connect and authenticate the client
    actual_client_id = await websocket_manager.connect(
        websocket=websocket,
        session_id=session_id
    )
    
    websocket_manager.authenticate_client(websocket, session_id)
    
    # Auto-subscribe to session-specific topic only
    # Session topic receives all job updates (progress, completion, errors) for this session
    # No need to subscribe to general topics as they cause duplicate messages
    from app.models.websocket import WebSocketTopics
    session_topics = [
        WebSocketTopics.session_topic(session_id)
    ]
    
    for topic in session_topics:
        websocket_manager.subscribe_to_topic(websocket, topic)
    
    # Send welcome message
    await websocket_manager.send_json_message({
        "type": "system",
        "level": "info",
        "message": f"Monitoring session {session_id}. You will receive updates for all jobs in this session.",
        "details": {"session_id": session_id, "subscribed_topics": session_topics}
    }, websocket)

    # CRITICAL FIX: Retrieve and send any buffered messages
    # This ensures file_ready messages published before WebSocket connection aren't lost
    try:
        if websocket_manager.redis_service:
            session_topic = WebSocketTopics.session_topic(session_id)
            buffered_messages = await websocket_manager.redis_service.get_buffered_messages(session_topic)

            if buffered_messages:
                logger.info(f"📬 Sending {len(buffered_messages)} buffered messages to newly connected client")

                # Send each buffered message to the client
                for msg in buffered_messages:
                    await websocket_manager.send_json_message(msg, websocket)

                # Clear the buffer after successful delivery
                await websocket_manager.redis_service.clear_buffered_messages(session_topic)
                logger.info(f"✅ Successfully delivered and cleared {len(buffered_messages)} buffered messages")
            else:
                logger.debug("No buffered messages found for this session")

    except Exception as e:
        logger.error(f"Error retrieving buffered messages: {e}")
        # Continue anyway - buffered messages are a nice-to-have feature

    try:
        # Message handling loop
        while True:
            data = await websocket.receive_text()
            
            try:
                message = json.loads(data)
                await websocket_manager.handle_client_message(websocket, message)
            except json.JSONDecodeError as e:
                await websocket_manager.send_json_message({
                    "type": "system",
                    "level": "error",
                    "message": f"Invalid JSON format: {str(e)}"
                }, websocket)
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket client {actual_client_id} disconnected from session {session_id}")
    except Exception as e:
        logger.error(f"WebSocket session error for {session_id}: {e}")
    finally:
        websocket_manager.disconnect(websocket)


# MVP: Stats and broadcast endpoints removed for simplicity
