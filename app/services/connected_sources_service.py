"""P6 — Connected document sources (Google Drive / Dropbox watch folders).

This service owns the OAuth handshake for the two supported providers and
persists encrypted tokens against the workspace.  File polling and download
are deliberately split out from this module (see ``run_connected_source_sync``
stub below) so the initial cut can be deployed without a periodic worker.

Encryption: tokens are wrapped with Fernet using
``CONNECTED_SOURCES_TOKEN_ENCRYPTION_KEY``.  If the key is unset the service
refuses to persist tokens — failing closed protects the integration if it is
deployed before secrets land.
"""

from __future__ import annotations

import base64
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import httpx
from cryptography.fernet import Fernet, InvalidToken

from app.core.config import settings
from app.services.supabase_service import get_supabase_service

logger = logging.getLogger(__name__)


GOOGLE_DRIVE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_DRIVE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_DRIVE_SCOPES = "https://www.googleapis.com/auth/drive.readonly openid email"

DROPBOX_AUTH_URL = "https://www.dropbox.com/oauth2/authorize"
DROPBOX_TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"


class ConnectedSourcesConfigurationError(RuntimeError):
    """Raised when a provider is not configured (missing secrets)."""


def _cipher() -> Fernet:
    key = settings.connected_sources_token_encryption_key.strip()
    if not key:
        raise ConnectedSourcesConfigurationError(
            "CONNECTED_SOURCES_TOKEN_ENCRYPTION_KEY is not set on this deployment"
        )
    try:
        return Fernet(key.encode("utf-8"))
    except (ValueError, TypeError) as exc:
        raise ConnectedSourcesConfigurationError(
            "CONNECTED_SOURCES_TOKEN_ENCRYPTION_KEY is not a valid Fernet key"
        ) from exc


def _encrypt(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return _cipher().encrypt(value.encode("utf-8")).decode("utf-8")


def _decrypt(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return _cipher().decrypt(value.encode("utf-8")).decode("utf-8")
    except InvalidToken:
        logger.warning("Connected source token decrypt failed — re-auth required")
        return None


def _provider_config(provider: str) -> Dict[str, str]:
    if provider == "google_drive":
        if not settings.google_drive_client_id or not settings.google_drive_client_secret:
            raise ConnectedSourcesConfigurationError(
                "Google Drive OAuth is not configured. Register the Google Cloud "
                "OAuth client and set GOOGLE_DRIVE_CLIENT_ID / "
                "GOOGLE_DRIVE_CLIENT_SECRET / GOOGLE_DRIVE_REDIRECT_URI."
            )
        return {
            "client_id": settings.google_drive_client_id,
            "client_secret": settings.google_drive_client_secret,
            "redirect_uri": settings.google_drive_redirect_uri,
        }
    if provider == "dropbox":
        if not settings.dropbox_app_key or not settings.dropbox_app_secret:
            raise ConnectedSourcesConfigurationError(
                "Dropbox OAuth is not configured. Register the Dropbox app and "
                "set DROPBOX_APP_KEY / DROPBOX_APP_SECRET / DROPBOX_REDIRECT_URI."
            )
        return {
            "client_id": settings.dropbox_app_key,
            "client_secret": settings.dropbox_app_secret,
            "redirect_uri": settings.dropbox_redirect_uri,
        }
    raise ValueError(f"Unknown provider {provider!r}")


def _encode_state(workspace_id: str, user_id: str, redirect_after: Optional[str]) -> str:
    """Pack (workspace_id, user_id, nonce, redirect) into an opaque state."""
    nonce = secrets.token_urlsafe(18)
    raw = "|".join([
        workspace_id,
        user_id,
        nonce,
        redirect_after or "",
    ])
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("utf-8")


def _decode_state(state: str) -> Dict[str, str]:
    try:
        raw = base64.urlsafe_b64decode(state.encode("utf-8")).decode("utf-8")
    except Exception as exc:  # pragma: no cover — defensive
        raise ValueError("Invalid state parameter") from exc
    parts = raw.split("|", 3)
    if len(parts) < 3:
        raise ValueError("Invalid state parameter")
    return {
        "workspace_id": parts[0],
        "user_id": parts[1],
        "nonce": parts[2],
        "redirect_after": parts[3] if len(parts) > 3 else "",
    }


def build_authorization_url(
    provider: str,
    workspace_id: str,
    user_id: str,
    redirect_after: Optional[str] = None,
) -> str:
    cfg = _provider_config(provider)
    state = _encode_state(workspace_id, user_id, redirect_after)

    if provider == "google_drive":
        params = {
            "client_id": cfg["client_id"],
            "redirect_uri": cfg["redirect_uri"],
            "response_type": "code",
            "scope": GOOGLE_DRIVE_SCOPES,
            "access_type": "offline",
            "prompt": "consent",
            "include_granted_scopes": "true",
            "state": state,
        }
        return f"{GOOGLE_DRIVE_AUTH_URL}?{urlencode(params)}"

    if provider == "dropbox":
        params = {
            "client_id": cfg["client_id"],
            "redirect_uri": cfg["redirect_uri"],
            "response_type": "code",
            "token_access_type": "offline",
            "state": state,
        }
        return f"{DROPBOX_AUTH_URL}?{urlencode(params)}"

    raise ValueError(f"Unknown provider {provider!r}")


async def _exchange_code_for_tokens(provider: str, code: str) -> Dict[str, Any]:
    cfg = _provider_config(provider)
    if provider == "google_drive":
        token_url = GOOGLE_DRIVE_TOKEN_URL
        payload = {
            "code": code,
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "redirect_uri": cfg["redirect_uri"],
            "grant_type": "authorization_code",
        }
    else:
        token_url = DROPBOX_TOKEN_URL
        payload = {
            "code": code,
            "grant_type": "authorization_code",
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "redirect_uri": cfg["redirect_uri"],
        }
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(token_url, data=payload)
    if response.status_code >= 400:
        logger.warning(
            "Token exchange failed for %s: %s %s",
            provider, response.status_code, response.text[:300],
        )
        raise ValueError(f"OAuth token exchange failed ({response.status_code})")
    return response.json()


async def _fetch_account_email(provider: str, access_token: str) -> Optional[str]:
    """Look up the connected account's email so the UI can show who is linked."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            if provider == "google_drive":
                resp = await client.get(
                    "https://www.googleapis.com/oauth2/v3/userinfo",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
                if resp.status_code == 200:
                    return resp.json().get("email")
            elif provider == "dropbox":
                resp = await client.post(
                    "https://api.dropboxapi.com/2/users/get_current_account",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("email") or data.get("name", {}).get("display_name")
    except Exception as exc:  # pragma: no cover — best-effort lookup
        logger.debug("Account email lookup failed for %s: %s", provider, exc)
    return None


async def list_connected_sources(workspace_id: str, user_id: str) -> List[Dict[str, Any]]:
    """Return the connected sources for a workspace, tokens stripped."""
    supabase = get_supabase_service()
    resolved = await supabase.resolve_owned_workspace_id(user_id, workspace_id)
    if not resolved:
        return []
    response = supabase.client.table("connected_sources")\
        .select("id,workspace_id,provider,status,display_label,watched_folder,watched_folder_id,account_email,last_synced_at,last_sync_status,last_sync_error,created_at,updated_at")\
        .eq("owner_user_id", user_id)\
        .eq("workspace_id", resolved)\
        .order("created_at")\
        .execute()
    return response.data or []


async def disconnect_source(source_id: str, user_id: str) -> None:
    supabase = get_supabase_service()
    existing = supabase.client.table("connected_sources")\
        .select("id,owner_user_id")\
        .eq("id", source_id)\
        .limit(1)\
        .execute()
    if not existing.data or existing.data[0].get("owner_user_id") != user_id:
        raise ValueError("Connected source not found")
    supabase.client.table("connected_sources")\
        .update({
            "status": "disconnected",
            "encrypted_access_token": None,
            "encrypted_refresh_token": None,
            "updated_at": datetime.utcnow().isoformat(),
        })\
        .eq("id", source_id)\
        .eq("owner_user_id", user_id)\
        .execute()


async def update_watched_folder(
    source_id: str,
    user_id: str,
    *,
    watched_folder: Optional[str] = None,
    watched_folder_id: Optional[str] = None,
    display_label: Optional[str] = None,
) -> Dict[str, Any]:
    supabase = get_supabase_service()
    existing = supabase.client.table("connected_sources")\
        .select("*")\
        .eq("id", source_id)\
        .eq("owner_user_id", user_id)\
        .limit(1)\
        .execute()
    if not existing.data:
        raise ValueError("Connected source not found")
    update: Dict[str, Any] = {"updated_at": datetime.utcnow().isoformat()}
    if watched_folder is not None:
        update["watched_folder"] = watched_folder.strip() or None
    if watched_folder_id is not None:
        update["watched_folder_id"] = watched_folder_id.strip() or None
    if display_label is not None:
        update["display_label"] = display_label.strip() or None
    response = supabase.client.table("connected_sources")\
        .update(update)\
        .eq("id", source_id)\
        .eq("owner_user_id", user_id)\
        .execute()
    return (response.data or [existing.data[0]])[0]


async def handle_oauth_callback(
    provider: str,
    code: str,
    state: str,
    expected_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Exchange the OAuth code, persist encrypted tokens, return the source row."""
    decoded = _decode_state(state)
    workspace_id = decoded["workspace_id"]
    user_id = decoded["user_id"]
    if expected_user_id and expected_user_id != user_id:
        raise ValueError("OAuth state was issued for a different user")

    supabase = get_supabase_service()
    resolved = await supabase.resolve_owned_workspace_id(user_id, workspace_id)
    if not resolved:
        raise ValueError("Workspace is no longer accessible to this user")

    tokens = await _exchange_code_for_tokens(provider, code)
    access_token = tokens.get("access_token")
    if not access_token:
        raise ValueError("Provider did not return an access token")
    refresh_token = tokens.get("refresh_token")
    expires_in = tokens.get("expires_in")
    token_expires_at = None
    if isinstance(expires_in, (int, float)):
        token_expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
        ).isoformat()

    account_email = await _fetch_account_email(provider, access_token)

    now = datetime.utcnow().isoformat()
    payload = {
        "workspace_id": resolved,
        "owner_user_id": user_id,
        "provider": provider,
        "status": "connected",
        "encrypted_access_token": _encrypt(access_token),
        "encrypted_refresh_token": _encrypt(refresh_token) if refresh_token else None,
        "token_expires_at": token_expires_at,
        "account_email": account_email,
        "last_sync_status": None,
        "last_sync_error": None,
        "updated_at": now,
    }
    response = supabase.client.table("connected_sources")\
        .upsert(payload, on_conflict="workspace_id,provider")\
        .execute()
    if not response.data:
        raise Exception("Supabase returned no connected source row after callback")
    row = response.data[0]
    return {
        "id": row["id"],
        "workspace_id": row["workspace_id"],
        "provider": row["provider"],
        "status": row["status"],
        "account_email": row.get("account_email"),
        "redirect_after": decoded.get("redirect_after") or "",
    }


async def manual_sync_stub(source_id: str, user_id: str) -> Dict[str, Any]:
    """Manual sync trigger — file-fetch implementation is the next milestone.

    This records that a sync was attempted so the UI can show the timestamp.
    Once the Celery beat poller is wired, this will hand off to the same
    code path the scheduled run uses.
    """
    supabase = get_supabase_service()
    existing = supabase.client.table("connected_sources")\
        .select("id,owner_user_id")\
        .eq("id", source_id)\
        .limit(1)\
        .execute()
    if not existing.data or existing.data[0].get("owner_user_id") != user_id:
        raise ValueError("Connected source not found")
    now = datetime.utcnow().isoformat()
    response = supabase.client.table("connected_sources")\
        .update({
            "last_synced_at": now,
            "last_sync_status": "pending_implementation",
            "last_sync_error": (
                "Folder polling is configured but the periodic poller is not yet "
                "wired. Files will start flowing once the Celery beat task lands."
            ),
            "updated_at": now,
        })\
        .eq("id", source_id)\
        .eq("owner_user_id", user_id)\
        .execute()
    return (response.data or [existing.data[0]])[0]
