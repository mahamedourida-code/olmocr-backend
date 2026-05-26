"""Backend-only QuickBooks Online OAuth and read-only reference-data access."""

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import httpx
from cryptography.fernet import Fernet, InvalidToken

from app.core.config import settings
from app.services.supabase_service import get_supabase_service


class QuickBooksService:
    """Connect a workspace to QBO without exposing credentials to the browser."""

    AUTHORIZATION_URL = "https://appcenter.intuit.com/connect/oauth2"
    TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    REVOKE_URL = "https://developer.api.intuit.com/v2/oauth2/tokens/revoke"
    ACCOUNTING_SCOPE = "com.intuit.quickbooks.accounting"

    def __init__(self) -> None:
        self.supabase = get_supabase_service()
        self.client = self.supabase.client

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @classmethod
    def _timestamp(cls, seconds: int) -> str:
        return (cls._now() + timedelta(seconds=max(int(seconds or 0), 0))).isoformat()

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    @staticmethod
    def _state_hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _require_configuration(self) -> None:
        required = {
            "QUICKBOOKS_CLIENT_ID": settings.quickbooks_client_id,
            "QUICKBOOKS_CLIENT_SECRET": settings.quickbooks_client_secret,
            "QUICKBOOKS_REDIRECT_URI": settings.quickbooks_redirect_uri,
            "QUICKBOOKS_TOKEN_ENCRYPTION_KEY": settings.quickbooks_token_encryption_key,
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(f"QuickBooks is not configured: {', '.join(missing)}")

    def _cipher(self) -> Fernet:
        self._require_configuration()
        try:
            return Fernet(settings.quickbooks_token_encryption_key.encode("utf-8"))
        except (TypeError, ValueError) as exc:
            raise ValueError("QUICKBOOKS_TOKEN_ENCRYPTION_KEY is not a valid Fernet key") from exc

    def _encrypt(self, token: str) -> str:
        return self._cipher().encrypt(token.encode("utf-8")).decode("utf-8")

    def _decrypt(self, ciphertext: Optional[str]) -> str:
        if not ciphertext:
            raise ValueError("QuickBooks connection has no stored token")
        try:
            return self._cipher().decrypt(ciphertext.encode("utf-8")).decode("utf-8")
        except InvalidToken as exc:
            raise ValueError("QuickBooks token storage cannot be decrypted") from exc

    @property
    def _company_base_url(self) -> str:
        if settings.quickbooks_environment.lower() == "production":
            return "https://quickbooks.api.intuit.com"
        return "https://sandbox-quickbooks.api.intuit.com"

    async def _owned_workspace(self, user_id: str, workspace_id: Optional[str]) -> str:
        resolved = await self.supabase.resolve_owned_workspace_id(user_id, workspace_id)
        if not resolved:
            raise ValueError("Select a workspace before connecting QuickBooks")
        return resolved

    async def _owned_connection(
        self,
        user_id: str,
        workspace_id: Optional[str],
        require_connected: bool = False,
    ) -> tuple[str, Optional[Dict[str, Any]]]:
        resolved = await self._owned_workspace(user_id, workspace_id)
        response = self.client.table("quickbooks_connections")\
            .select("*")\
            .eq("owner_user_id", user_id)\
            .eq("workspace_id", resolved)\
            .limit(1)\
            .execute()
        connection = response.data[0] if response.data else None
        if require_connected and (not connection or connection.get("status") != "connected"):
            raise ValueError("QuickBooks is not connected for this workspace")
        return resolved, connection

    async def begin_connection(self, user_id: str, workspace_id: Optional[str] = None) -> str:
        """Create a single-use OAuth state and return Intuit's authorization URL."""
        self._require_configuration()
        resolved = await self._owned_workspace(user_id, workspace_id)
        raw_state = secrets.token_urlsafe(48)
        self.client.table("quickbooks_oauth_states").insert({
            "state_hash": self._state_hash(raw_state),
            "owner_user_id": user_id,
            "workspace_id": resolved,
            "expires_at": self._timestamp(10 * 60),
        }).execute()
        params = {
            "client_id": settings.quickbooks_client_id,
            "response_type": "code",
            "scope": self.ACCOUNTING_SCOPE,
            "redirect_uri": settings.quickbooks_redirect_uri,
            "state": raw_state,
        }
        return f"{self.AUTHORIZATION_URL}?{urlencode(params)}"

    def _consume_state(self, raw_state: str) -> Dict[str, Any]:
        response = self.client.table("quickbooks_oauth_states")\
            .select("*")\
            .eq("state_hash", self._state_hash(raw_state))\
            .limit(1)\
            .execute()
        if not response.data:
            raise ValueError("QuickBooks authorization state is invalid")
        state_record = response.data[0]
        if state_record.get("used_at") or self._parse_timestamp(state_record.get("expires_at")) <= self._now():
            raise ValueError("QuickBooks authorization state has expired")
        used_at = self._now().isoformat()
        claimed = self.client.table("quickbooks_oauth_states")\
            .update({"used_at": used_at})\
            .eq("id", state_record["id"])\
            .is_("used_at", "null")\
            .execute()
        if not claimed.data:
            raise ValueError("QuickBooks authorization state has already been used")
        return state_record

    async def _token_request(self, data: Dict[str, str]) -> Dict[str, Any]:
        self._require_configuration()
        async with httpx.AsyncClient(timeout=25.0) as client:
            response = await client.post(
                self.TOKEN_URL,
                auth=(settings.quickbooks_client_id, settings.quickbooks_client_secret),
                headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )
        if response.status_code >= 400:
            raise ValueError("QuickBooks authorization could not be completed")
        payload = response.json()
        if not payload.get("access_token") or not payload.get("refresh_token"):
            raise ValueError("QuickBooks returned incomplete authorization credentials")
        return payload

    async def complete_connection(self, code: str, realm_id: str, raw_state: str) -> Dict[str, Any]:
        """Exchange an authorized code and persist encrypted tokens for its workspace."""
        state_record = self._consume_state(raw_state)
        token_payload = await self._token_request({
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": settings.quickbooks_redirect_uri,
        })
        now = self._now().isoformat()
        connection_data = {
            "owner_user_id": state_record["owner_user_id"],
            "workspace_id": state_record["workspace_id"],
            "realm_id": realm_id,
            "status": "connected",
            "scopes": [self.ACCOUNTING_SCOPE],
            "access_token_ciphertext": self._encrypt(token_payload["access_token"]),
            "refresh_token_ciphertext": self._encrypt(token_payload["refresh_token"]),
            "access_token_expires_at": self._timestamp(token_payload.get("expires_in", 3600)),
            "refresh_token_expires_at": self._timestamp(token_payload.get("x_refresh_token_expires_in", 100 * 24 * 3600)),
            "connected_at": now,
            "disconnected_at": None,
            "updated_at": now,
        }
        response = self.client.table("quickbooks_connections")\
            .upsert(connection_data, on_conflict="workspace_id")\
            .execute()
        if not response.data:
            raise ValueError("QuickBooks connection could not be stored")
        connection = response.data[0]
        try:
            company = await self._read_company_info(connection)
            name = company.get("CompanyName") or company.get("LegalName")
            if name:
                updated = self.client.table("quickbooks_connections")\
                    .update({"company_name": str(name), "updated_at": self._now().isoformat()})\
                    .eq("id", connection["id"])\
                    .execute()
                connection = updated.data[0] if updated.data else connection
        except ValueError:
            pass
        return await self._safe_status(connection)

    async def _refresh_access_token(self, connection: Dict[str, Any]) -> str:
        payload = await self._token_request({
            "grant_type": "refresh_token",
            "refresh_token": self._decrypt(connection.get("refresh_token_ciphertext")),
        })
        update = {
            "access_token_ciphertext": self._encrypt(payload["access_token"]),
            "refresh_token_ciphertext": self._encrypt(payload["refresh_token"]),
            "access_token_expires_at": self._timestamp(payload.get("expires_in", 3600)),
            "refresh_token_expires_at": self._timestamp(payload.get("x_refresh_token_expires_in", 100 * 24 * 3600)),
            "updated_at": self._now().isoformat(),
        }
        self.client.table("quickbooks_connections").update(update).eq("id", connection["id"]).execute()
        connection.update(update)
        return payload["access_token"]

    async def _access_token(self, connection: Dict[str, Any]) -> str:
        expires_at = self._parse_timestamp(connection.get("access_token_expires_at"))
        if not expires_at or expires_at <= self._now() + timedelta(seconds=60):
            return await self._refresh_access_token(connection)
        return self._decrypt(connection.get("access_token_ciphertext"))

    async def _accounting_get(
        self,
        connection: Dict[str, Any],
        endpoint: str,
        params: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        token = await self._access_token(connection)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(endpoint, headers=headers, params=params)
            if response.status_code == 401:
                headers["Authorization"] = f"Bearer {await self._refresh_access_token(connection)}"
                response = await client.get(endpoint, headers=headers, params=params)
        if response.status_code >= 400:
            raise ValueError("QuickBooks reference data could not be read")
        return response.json()

    async def _read_company_info(self, connection: Dict[str, Any]) -> Dict[str, Any]:
        realm_id = str(connection["realm_id"])
        payload = await self._accounting_get(
            connection,
            f"{self._company_base_url}/v3/company/{realm_id}/companyinfo/{realm_id}",
            {"minorversion": settings.quickbooks_minor_version},
        )
        return payload.get("CompanyInfo") or {}

    @staticmethod
    def _reference_row(resource_type: str, record: Dict[str, Any]) -> Dict[str, Any]:
        if resource_type == "vendor":
            details = {
                "company_name": record.get("CompanyName"),
                "print_on_check_name": record.get("PrintOnCheckName"),
            }
            display_name = record.get("DisplayName") or record.get("CompanyName")
        elif resource_type == "account":
            details = {
                "account_type": record.get("AccountType"),
                "account_sub_type": record.get("AccountSubType"),
                "classification": record.get("Classification"),
            }
            display_name = record.get("Name")
        else:
            details = {
                "description": record.get("Description"),
                "taxable": record.get("Taxable"),
            }
            display_name = record.get("Name")
        return {
            "external_id": str(record.get("Id") or ""),
            "display_name": str(display_name or "Unnamed"),
            "active": bool(record.get("Active", True)),
            "details": {key: value for key, value in details.items() if value is not None},
        }

    async def sync_reference_data(self, user_id: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
        """Read only coding reference lists needed for a later draft-bill workflow."""
        self._require_configuration()
        _, connection = await self._owned_connection(user_id, workspace_id, require_connected=True)
        assert connection is not None
        company = await self._read_company_info(connection)
        now = self._now().isoformat()
        mappings = {"vendor": "Vendor", "account": "Account", "tax_code": "TaxCode"}
        counts: Dict[str, int] = {}
        for resource_type, entity in mappings.items():
            payload = await self._accounting_get(
                connection,
                f"{self._company_base_url}/v3/company/{connection['realm_id']}/query",
                {"query": f"select * from {entity} maxresults 1000", "minorversion": settings.quickbooks_minor_version},
            )
            rows = payload.get("QueryResponse", {}).get(entity, []) or []
            self.client.table("quickbooks_reference_data").delete()\
                .eq("connection_id", connection["id"])\
                .eq("resource_type", resource_type)\
                .execute()
            records = []
            for raw in rows:
                safe = self._reference_row(resource_type, raw)
                if not safe["external_id"]:
                    continue
                records.append({
                    **safe,
                    "connection_id": connection["id"],
                    "workspace_id": connection["workspace_id"],
                    "owner_user_id": user_id,
                    "synced_at": now,
                })
            if records:
                self.client.table("quickbooks_reference_data").insert(records).execute()
            counts[resource_type] = len(records)
        company_name = company.get("CompanyName") or company.get("LegalName")
        update = {"last_synced_at": now, "updated_at": now}
        if company_name:
            update["company_name"] = str(company_name)
        updated = self.client.table("quickbooks_connections")\
            .update(update)\
            .eq("id", connection["id"])\
            .execute()
        status_data = await self._safe_status(updated.data[0] if updated.data else connection)
        status_data["reference_counts"] = counts
        return status_data

    async def list_reference_data(
        self,
        user_id: str,
        workspace_id: Optional[str] = None,
        resource_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        _, connection = await self._owned_connection(user_id, workspace_id, require_connected=True)
        assert connection is not None
        query = self.client.table("quickbooks_reference_data")\
            .select("resource_type,external_id,display_name,active,details,synced_at")\
            .eq("connection_id", connection["id"])\
            .eq("owner_user_id", user_id)
        if resource_type:
            query = query.eq("resource_type", resource_type)
        response = query.order("display_name").execute()
        return response.data or []

    async def _safe_status(self, connection: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not connection:
            return {"connected": False, "status": "disconnected", "reference_counts": {}}
        rows = self.client.table("quickbooks_reference_data")\
            .select("resource_type")\
            .eq("connection_id", connection["id"])\
            .execute()
        counts: Dict[str, int] = {}
        for row in rows.data or []:
            key = str(row.get("resource_type"))
            counts[key] = counts.get(key, 0) + 1
        return {
            "connected": connection.get("status") == "connected",
            "status": connection.get("status") or "disconnected",
            "workspace_id": connection.get("workspace_id"),
            "realm_id": connection.get("realm_id"),
            "company_name": connection.get("company_name"),
            "connected_at": connection.get("connected_at"),
            "last_synced_at": connection.get("last_synced_at"),
            "reference_counts": counts,
        }

    async def status(self, user_id: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
        _, connection = await self._owned_connection(user_id, workspace_id)
        return await self._safe_status(connection)

    async def disconnect(self, user_id: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
        self._require_configuration()
        _, connection = await self._owned_connection(user_id, workspace_id, require_connected=True)
        assert connection is not None
        async with httpx.AsyncClient(timeout=25.0) as client:
            response = await client.post(
                self.REVOKE_URL,
                auth=(settings.quickbooks_client_id, settings.quickbooks_client_secret),
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                json={"token": self._decrypt(connection.get("refresh_token_ciphertext"))},
            )
        if response.status_code not in {200, 204}:
            raise ValueError("QuickBooks could not be disconnected; retry shortly")
        update = {
            "status": "disconnected",
            "access_token_ciphertext": None,
            "refresh_token_ciphertext": None,
            "access_token_expires_at": None,
            "refresh_token_expires_at": None,
            "disconnected_at": self._now().isoformat(),
            "updated_at": self._now().isoformat(),
        }
        updated = self.client.table("quickbooks_connections")\
            .update(update)\
            .eq("id", connection["id"])\
            .eq("owner_user_id", user_id)\
            .execute()
        return await self._safe_status(updated.data[0] if updated.data else {**connection, **update})


_quickbooks_service: Optional[QuickBooksService] = None


def get_quickbooks_service() -> QuickBooksService:
    global _quickbooks_service
    if _quickbooks_service is None:
        _quickbooks_service = QuickBooksService()
    return _quickbooks_service
