"""P6 — Connected sources API (Google Drive / Dropbox watch folders)."""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import RedirectResponse

from app.core.config import settings
from app.core.dependencies import get_current_user
from app.models.requests import (
    ConnectedSourceConnectRequest,
    ConnectedSourceFolderUpdate,
    ConnectedSourceProvider,
)
from app.services.connected_sources_service import (
    ConnectedSourcesConfigurationError,
    build_authorization_url,
    disconnect_source,
    handle_oauth_callback,
    list_connected_sources,
    manual_sync_stub,
    update_watched_folder,
)


router = APIRouter(prefix="/connected-sources", tags=["Connected Sources"])


@router.get("", response_model=Dict[str, Any])
async def list_sources(
    workspace_id: str = Query(...),
    user: dict = Depends(get_current_user),
):
    sources = await list_connected_sources(workspace_id, user["user_id"])
    return {
        "sources": sources,
        "total": len(sources),
        "providers_configured": {
            "google_drive": bool(settings.google_drive_client_id and settings.google_drive_client_secret),
            "dropbox": bool(settings.dropbox_app_key and settings.dropbox_app_secret),
        },
    }


@router.post("/connect", response_model=Dict[str, Any])
async def start_connect(
    request: ConnectedSourceConnectRequest,
    user: dict = Depends(get_current_user),
):
    try:
        authorization_url = build_authorization_url(
            provider=request.provider,
            workspace_id=request.workspace_id,
            user_id=user["user_id"],
            redirect_after=request.redirect_after,
        )
    except ConnectedSourcesConfigurationError as exc:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return {"authorization_url": authorization_url}


@router.get("/{provider}/callback")
async def oauth_callback(
    provider: ConnectedSourceProvider,
    request: Request,
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
):
    """OAuth landing endpoint — exchanges code, then redirects to the frontend."""
    if error:
        return _failure_redirect(error, state)
    if not code or not state:
        return _failure_redirect("missing_code_or_state", state)
    try:
        result = await handle_oauth_callback(provider=provider, code=code, state=state)
    except ConnectedSourcesConfigurationError as exc:
        return _failure_redirect(str(exc), state)
    except ValueError as exc:
        return _failure_redirect(str(exc), state)
    except Exception as exc:  # pragma: no cover
        return _failure_redirect(f"unexpected_error: {exc}", state)
    redirect_after = result.get("redirect_after") or _default_inbox_url()
    sep = "&" if "?" in redirect_after else "?"
    return RedirectResponse(
        url=f"{redirect_after}{sep}connected={provider}&status=connected",
        status_code=302,
    )


@router.patch("/{source_id}", response_model=Dict[str, Any])
async def update_source(
    source_id: str,
    payload: ConnectedSourceFolderUpdate,
    user: dict = Depends(get_current_user),
):
    try:
        updated = await update_watched_folder(
            source_id=source_id,
            user_id=user["user_id"],
            watched_folder=payload.watched_folder,
            watched_folder_id=payload.watched_folder_id,
            display_label=payload.display_label,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    # Strip encrypted token columns before returning.
    updated.pop("encrypted_access_token", None)
    updated.pop("encrypted_refresh_token", None)
    return {"source": updated}


@router.delete("/{source_id}", response_model=Dict[str, Any])
async def disconnect(
    source_id: str,
    user: dict = Depends(get_current_user),
):
    try:
        await disconnect_source(source_id, user["user_id"])
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    return {"success": True, "source_id": source_id}


@router.post("/{source_id}/sync", response_model=Dict[str, Any])
async def trigger_sync(
    source_id: str,
    user: dict = Depends(get_current_user),
):
    try:
        result = await manual_sync_stub(source_id, user["user_id"])
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    result.pop("encrypted_access_token", None)
    result.pop("encrypted_refresh_token", None)
    return {"source": result}


def _default_inbox_url() -> str:
    return f"{settings.frontend_url.rstrip('/')}/dashboard/inbox"


def _failure_redirect(reason: str, state: Optional[str]) -> RedirectResponse:
    target = _default_inbox_url()
    try:
        if state:
            from app.services.connected_sources_service import _decode_state
            decoded = _decode_state(state)
            after = decoded.get("redirect_after") or ""
            if after:
                target = after
    except Exception:
        pass
    sep = "&" if "?" in target else "?"
    safe_reason = reason.replace(" ", "_")[:160]
    return RedirectResponse(
        url=f"{target}{sep}connected=error&reason={safe_reason}",
        status_code=302,
    )
