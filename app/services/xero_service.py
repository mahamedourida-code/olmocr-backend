"""Backend-only Xero connection, reference data, and reviewed Bill publishing.

Mirrors ``QuickBooksService`` so the AP coding form and Integrations page can
treat the two connectors interchangeably. Xero specifics:

- A "Bill" is an ACCPAY ``Invoice`` (POST /Invoices, Status=DRAFT).
- Line items reference an account by its **Code** (not the AccountID) and a
  tax rate by its **TaxType** string, so we persist both on the reference row.
- The tenant (organisation) id is fetched from /connections after token
  exchange and sent on every call as the ``Xero-tenant-id`` header.
"""

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

import httpx
from cryptography.fernet import Fernet, InvalidToken

from app.core.config import settings
from app.services.supabase_service import get_supabase_service


class XeroPublishError(ValueError):
    """A publication error whose uncertainty determines whether a Bill may be retried."""

    def __init__(self, message: str, indeterminate: bool = False) -> None:
        super().__init__(message)
        self.indeterminate = indeterminate


class XeroService:
    """Connect a workspace to Xero without exposing credentials to the browser."""

    AUTHORIZATION_URL = "https://login.xero.com/identity/connect/authorize"
    TOKEN_URL = "https://identity.xero.com/connect/token"
    CONNECTIONS_URL = "https://api.xero.com/connections"
    API_BASE = "https://api.xero.com/api.xro/2.0"
    SCOPES = "offline_access accounting.transactions accounting.contacts accounting.settings"

    def __init__(self) -> None:
        self.supabase = get_supabase_service()
        self.client = self.supabase.client

    # ── time + crypto helpers (mirror QuickBooksService) ───────────────────

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
            "XERO_CLIENT_ID": settings.xero_client_id,
            "XERO_CLIENT_SECRET": settings.xero_client_secret,
            "XERO_REDIRECT_URI": settings.xero_redirect_uri,
            "XERO_TOKEN_ENCRYPTION_KEY": settings.xero_token_encryption_key,
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(f"Xero is not configured: {', '.join(missing)}")

    def _cipher(self) -> Fernet:
        self._require_configuration()
        try:
            return Fernet(settings.xero_token_encryption_key.encode("utf-8"))
        except (TypeError, ValueError) as exc:
            raise ValueError("XERO_TOKEN_ENCRYPTION_KEY is not a valid Fernet key") from exc

    def _encrypt(self, token: str) -> str:
        return self._cipher().encrypt(token.encode("utf-8")).decode("utf-8")

    def _decrypt(self, ciphertext: Optional[str]) -> str:
        if not ciphertext:
            raise ValueError("Xero connection has no stored token")
        try:
            return self._cipher().decrypt(ciphertext.encode("utf-8")).decode("utf-8")
        except InvalidToken as exc:
            raise ValueError("Xero token storage cannot be decrypted") from exc

    # ── ownership ──────────────────────────────────────────────────────────

    async def _owned_workspace(self, user_id: str, workspace_id: Optional[str]) -> str:
        resolved = await self.supabase.resolve_owned_workspace_id(user_id, workspace_id)
        if not resolved:
            raise ValueError("Select a workspace before connecting Xero")
        return resolved

    async def _owned_connection(
        self,
        user_id: str,
        workspace_id: Optional[str],
        require_connected: bool = False,
    ) -> tuple[str, Optional[Dict[str, Any]]]:
        resolved = await self._owned_workspace(user_id, workspace_id)
        response = self.client.table("xero_connections")\
            .select("*")\
            .eq("owner_user_id", user_id)\
            .eq("workspace_id", resolved)\
            .limit(1)\
            .execute()
        connection = response.data[0] if response.data else None
        if require_connected and (not connection or connection.get("status") != "connected"):
            raise ValueError("Xero is not connected for this workspace")
        return resolved, connection

    # ── OAuth ───────────────────────────────────────────────────────────────

    async def begin_connection(self, user_id: str, workspace_id: Optional[str] = None) -> str:
        self._require_configuration()
        resolved = await self._owned_workspace(user_id, workspace_id)
        raw_state = secrets.token_urlsafe(48)
        self.client.table("xero_oauth_states").insert({
            "state_hash": self._state_hash(raw_state),
            "owner_user_id": user_id,
            "workspace_id": resolved,
            "expires_at": self._timestamp(10 * 60),
        }).execute()
        params = {
            "response_type": "code",
            "client_id": settings.xero_client_id,
            "redirect_uri": settings.xero_redirect_uri,
            "scope": self.SCOPES,
            "state": raw_state,
        }
        return f"{self.AUTHORIZATION_URL}?{urlencode(params)}"

    def _consume_state(self, raw_state: str) -> Dict[str, Any]:
        response = self.client.table("xero_oauth_states")\
            .select("*")\
            .eq("state_hash", self._state_hash(raw_state))\
            .limit(1)\
            .execute()
        if not response.data:
            raise ValueError("Xero authorization state is invalid")
        state_record = response.data[0]
        if state_record.get("used_at") or self._parse_timestamp(state_record.get("expires_at")) <= self._now():
            raise ValueError("Xero authorization state has expired")
        claimed = self.client.table("xero_oauth_states")\
            .update({"used_at": self._now().isoformat()})\
            .eq("id", state_record["id"])\
            .is_("used_at", "null")\
            .execute()
        if not claimed.data:
            raise ValueError("Xero authorization state has already been used")
        return state_record

    async def _token_request(self, data: Dict[str, str]) -> Dict[str, Any]:
        self._require_configuration()
        async with httpx.AsyncClient(timeout=25.0) as client:
            response = await client.post(
                self.TOKEN_URL,
                auth=(settings.xero_client_id, settings.xero_client_secret),
                headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )
        if response.status_code >= 400:
            raise ValueError("Xero authorization could not be completed")
        payload = response.json()
        if not payload.get("access_token") or not payload.get("refresh_token"):
            raise ValueError("Xero returned incomplete authorization credentials")
        return payload

    async def _fetch_tenant(self, access_token: str) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(
                self.CONNECTIONS_URL,
                headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            )
        if response.status_code >= 400:
            raise ValueError("Could not read the connected Xero organisation")
        tenants = response.json() or []
        # Prefer an ORGANISATION tenant; fall back to the first.
        org = next((t for t in tenants if t.get("tenantType") == "ORGANISATION"), None)
        return org or (tenants[0] if tenants else {})

    async def complete_connection(self, code: str, raw_state: str) -> Dict[str, Any]:
        state_record = self._consume_state(raw_state)
        token_payload = await self._token_request({
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": settings.xero_redirect_uri,
        })
        tenant = await self._fetch_tenant(token_payload["access_token"])
        now = self._now().isoformat()
        connection_data = {
            "owner_user_id": state_record["owner_user_id"],
            "workspace_id": state_record["workspace_id"],
            "tenant_id": tenant.get("tenantId"),
            "tenant_name": tenant.get("tenantName"),
            "status": "connected",
            "scopes": self.SCOPES.split(),
            "access_token_ciphertext": self._encrypt(token_payload["access_token"]),
            "refresh_token_ciphertext": self._encrypt(token_payload["refresh_token"]),
            "access_token_expires_at": self._timestamp(token_payload.get("expires_in", 1800)),
            "refresh_token_expires_at": self._timestamp(60 * 24 * 3600),
            "connected_at": now,
            "disconnected_at": None,
            "updated_at": now,
        }
        response = self.client.table("xero_connections")\
            .upsert(connection_data, on_conflict="workspace_id")\
            .execute()
        if not response.data:
            raise ValueError("Xero connection could not be stored")
        return await self._safe_status(response.data[0])

    async def _refresh_access_token(self, connection: Dict[str, Any]) -> str:
        payload = await self._token_request({
            "grant_type": "refresh_token",
            "refresh_token": self._decrypt(connection.get("refresh_token_ciphertext")),
        })
        update = {
            "access_token_ciphertext": self._encrypt(payload["access_token"]),
            "refresh_token_ciphertext": self._encrypt(payload["refresh_token"]),
            "access_token_expires_at": self._timestamp(payload.get("expires_in", 1800)),
            "refresh_token_expires_at": self._timestamp(60 * 24 * 3600),
            "updated_at": self._now().isoformat(),
        }
        self.client.table("xero_connections").update(update).eq("id", connection["id"]).execute()
        connection.update(update)
        return payload["access_token"]

    async def _access_token(self, connection: Dict[str, Any]) -> str:
        expires_at = self._parse_timestamp(connection.get("access_token_expires_at"))
        if not expires_at or expires_at <= self._now() + timedelta(seconds=60):
            return await self._refresh_access_token(connection)
        return self._decrypt(connection.get("access_token_ciphertext"))

    def _headers(self, token: str, connection: Dict[str, Any]) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Xero-tenant-id": str(connection.get("tenant_id") or ""),
            "Accept": "application/json",
        }

    async def _api_get(self, connection: Dict[str, Any], path: str) -> Dict[str, Any]:
        token = await self._access_token(connection)
        url = f"{self.API_BASE}/{path}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers(token, connection))
            if response.status_code == 401:
                token = await self._refresh_access_token(connection)
                response = await client.get(url, headers=self._headers(token, connection))
        if response.status_code >= 400:
            raise ValueError("Xero reference data could not be read")
        return response.json()

    async def _api_post(self, connection: Dict[str, Any], path: str, json_payload: Dict[str, Any]) -> Dict[str, Any]:
        token = await self._access_token(connection)
        url = f"{self.API_BASE}/{path}"
        headers = {**self._headers(token, connection), "Content-Type": "application/json"}
        try:
            async with httpx.AsyncClient(timeout=45.0) as client:
                response = await client.post(url, headers=headers, json=json_payload)
                if response.status_code == 401:
                    headers["Authorization"] = f"Bearer {await self._refresh_access_token(connection)}"
                    response = await client.post(url, headers=headers, json=json_payload)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise XeroPublishError(
                "Xero did not confirm the request. Check the connected organisation before retrying.",
                indeterminate=True,
            ) from exc
        if response.status_code >= 500:
            raise XeroPublishError(
                "Xero did not confirm the request. Check the connected organisation before retrying.",
                indeterminate=True,
            )
        if response.status_code >= 400:
            detail = "Xero rejected this bill. Check contact, account, tax, date, and currency selections."
            try:
                body = response.json()
                elements = body.get("Elements") or []
                if elements and elements[0].get("ValidationErrors"):
                    detail = str(elements[0]["ValidationErrors"][0].get("Message"))[:240]
                elif body.get("Message"):
                    detail = str(body["Message"])[:240]
            except (ValueError, AttributeError, TypeError, IndexError):
                pass
            raise XeroPublishError(detail)
        return response.json()

    async def _api_upload_attachment(
        self,
        connection: Dict[str, Any],
        path: str,
        content: bytes,
        content_type: Optional[str],
    ) -> None:
        token = await self._access_token(connection)
        url = f"{self.API_BASE}/{path}"
        headers = {
            **self._headers(token, connection),
            "Content-Type": str(content_type or "application/octet-stream"),
        }
        try:
            async with httpx.AsyncClient(timeout=45.0) as client:
                response = await client.post(url, headers=headers, content=content)
                if response.status_code == 401:
                    headers["Authorization"] = f"Bearer {await self._refresh_access_token(connection)}"
                    response = await client.post(url, headers=headers, content=content)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise ValueError("Xero did not confirm the source-document attachment") from exc
        if response.status_code >= 400:
            raise ValueError("Xero could not attach the source document to this bill")

    # ── reference sync ───────────────────────────────────────────────────────

    @staticmethod
    def _reference_row(resource_type: str, record: Dict[str, Any]) -> Dict[str, Any]:
        if resource_type == "vendor":
            external_id = record.get("ContactID")
            display_name = record.get("Name")
            details = {"is_supplier": record.get("IsSupplier")}
        elif resource_type == "account":
            external_id = record.get("AccountID")
            display_name = record.get("Name")
            # Xero line items reference the account CODE, not the ID.
            details = {
                "code": record.get("Code"),
                "account_type": record.get("Type"),
                "tax_type": record.get("TaxType"),
                "class": record.get("Class"),
            }
        else:  # tax_code
            external_id = record.get("TaxType") or record.get("Name")
            display_name = record.get("Name")
            details = {
                "tax_type": record.get("TaxType"),
                "effective_rate": record.get("EffectiveRate"),
            }
        return {
            "external_id": str(external_id or ""),
            "display_name": str(display_name or "Unnamed"),
            "active": str(record.get("Status") or "ACTIVE").upper() == "ACTIVE",
            "details": {k: v for k, v in details.items() if v is not None},
        }

    async def sync_reference_data(self, user_id: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
        self._require_configuration()
        _, connection = await self._owned_connection(user_id, workspace_id, require_connected=True)
        assert connection is not None
        now = self._now().isoformat()
        # Suppliers only for contacts; active accounts; all tax rates.
        sources = {
            "vendor": ("Contacts?where=IsSupplier==true", "Contacts"),
            "account": ("Accounts", "Accounts"),
            "tax_code": ("TaxRates", "TaxRates"),
        }
        counts: Dict[str, int] = {}
        for resource_type, (path, key) in sources.items():
            payload = await self._api_get(connection, path)
            rows = payload.get(key, []) or []
            self.client.table("xero_reference_data").delete()\
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
                self.client.table("xero_reference_data").insert(records).execute()
            counts[resource_type] = len(records)
        updated = self.client.table("xero_connections")\
            .update({"last_synced_at": now, "updated_at": now})\
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
        query = self.client.table("xero_reference_data")\
            .select("resource_type,external_id,display_name,active,details,synced_at")\
            .eq("connection_id", connection["id"])\
            .eq("owner_user_id", user_id)
        if resource_type:
            query = query.eq("resource_type", resource_type)
        response = query.order("display_name").execute()
        return response.data or []

    def _selected_reference(
        self,
        connection: Dict[str, Any],
        owner_user_id: str,
        resource_type: str,
        external_id: Optional[str],
        label: str,
        required: bool = True,
    ) -> Optional[Dict[str, Any]]:
        if not external_id:
            if required:
                raise ValueError(f"Select a synced Xero {label} before publishing")
            return None
        response = self.client.table("xero_reference_data")\
            .select("external_id,display_name,active,details")\
            .eq("connection_id", connection["id"])\
            .eq("owner_user_id", owner_user_id)\
            .eq("resource_type", resource_type)\
            .eq("external_id", external_id)\
            .limit(1)\
            .execute()
        if not response.data:
            raise ValueError(f"The selected Xero {label} is no longer in the synced list")
        return response.data[0]

    # ── publishing ───────────────────────────────────────────────────────────

    @staticmethod
    def _amount(value: Any) -> Optional[Decimal]:
        if value is None or value == "":
            return None
        try:
            return Decimal(str(value).replace(",", "").strip()).quantize(Decimal("0.01"))
        except (InvalidOperation, ValueError):
            return None

    def _bill_payload(self, item: Dict[str, Any], connection: Dict[str, Any]) -> Dict[str, Any]:
        draft = dict(item.get("draft_data") or {})
        vendor = self._selected_reference(connection, item["owner_user_id"], "vendor", draft.get("vendor_ref_id"), "contact")
        account = self._selected_reference(connection, item["owner_user_id"], "account", draft.get("account_ref_id"), "account")
        tax_code = self._selected_reference(
            connection, item["owner_user_id"], "tax_code", draft.get("tax_code_ref_id"), "tax rate", required=False,
        )
        assert vendor is not None and account is not None
        account_code = (account.get("details") or {}).get("code")
        if not account_code:
            raise ValueError("The selected Xero account has no code; re-sync the chart of accounts")
        tax_type = (tax_code.get("details") or {}).get("tax_type") if tax_code else None

        def _line(amount: Decimal, description: str) -> Dict[str, Any]:
            line: Dict[str, Any] = {
                "Description": description[:4000],
                "LineAmount": float(amount),
                "AccountCode": str(account_code),
            }
            if tax_type:
                line["TaxType"] = str(tax_type)
            return line

        lines: List[Dict[str, Any]] = []
        for raw in draft.get("line_items") or []:
            if not isinstance(raw, dict):
                continue
            amount = (
                self._amount(raw.get("line_total"))
                or self._amount(raw.get("total"))
                or self._amount(raw.get("amount"))
            )
            if amount is None:
                quantity = self._amount(raw.get("quantity"))
                unit_price = self._amount(raw.get("unit_price"))
                amount = quantity * unit_price if quantity is not None and unit_price is not None else None
            if amount is None:
                continue
            description = raw.get("description") or raw.get("item") or raw.get("name") or "Line item"
            lines.append(_line(amount, str(description)))
        if not lines:
            total = self._amount(draft.get("total"))
            if total is None:
                raise ValueError("Add a total or valid line amounts before publishing")
            lines = [_line(total, str(draft.get("reference") or item.get("source_filename") or "Reviewed invoice"))]

        payload: Dict[str, Any] = {
            "Type": "ACCPAY",
            "Status": "DRAFT",
            "Contact": {"ContactID": vendor["external_id"]},
            "LineItems": lines,
            "LineAmountTypes": "Exclusive",
        }
        if draft.get("invoice_number"):
            payload["InvoiceNumber"] = str(draft["invoice_number"])[:255]
        if draft.get("invoice_date"):
            payload["Date"] = str(draft["invoice_date"])
        if draft.get("due_date"):
            payload["DueDate"] = str(draft["due_date"])
        if draft.get("currency"):
            payload["CurrencyCode"] = str(draft["currency"]).upper()
        return payload

    async def _attach_source_to_bill(
        self,
        connection: Dict[str, Any],
        item: Dict[str, Any],
        invoice_id: str,
    ) -> None:
        source_storage_path = str(item.get("source_storage_path") or "").strip()
        if not source_storage_path:
            raise ValueError("The source document is no longer available for attachment")
        content = await self.supabase.download_file_from_storage(source_storage_path)
        filename = quote(str(item.get("source_filename") or "source-document"), safe="")
        await self._api_upload_attachment(
            connection,
            f"Invoices/{invoice_id}/Attachments/{filename}",
            content,
            item.get("source_content_type"),
        )

    async def publish_accounts_payable_bill(self, item_id: str, user_id: str) -> Dict[str, Any]:
        """Publish one Ready AP invoice as one DRAFT ACCPAY invoice in Xero."""
        item = await self.supabase.get_accounts_payable_item(item_id, user_id)
        if item.get("status") not in {"ready_to_publish", "published"}:
            raise ValueError("Only Ready to publish invoice items can be sent to Xero")
        if item.get("status") == "ready_to_publish" and not str((item.get("draft_data") or {}).get("due_date") or "").strip():
            raise ValueError("Complete the due date before publishing")
        _, connection = await self._owned_connection(user_id, item.get("workspace_id"), require_connected=True)
        assert connection is not None

        existing = self.client.table("xero_bill_publications")\
            .select("*")\
            .eq("accounts_payable_item_id", item_id)\
            .eq("owner_user_id", user_id)\
            .limit(1)\
            .execute()
        publication = existing.data[0] if existing.data else None
        if publication and publication.get("xero_invoice_id"):
            if item.get("attachment_visible") and publication.get("attachment_status") != "attached":
                try:
                    await self._attach_source_to_bill(connection, item, str(publication["xero_invoice_id"]))
                    publication = self._update_publication(publication["id"], {"attachment_status": "attached"})
                except Exception as exc:
                    publication = self._update_publication(publication["id"], {
                        "attachment_status": "failed",
                        "failure_details": list(publication.get("failure_details") or []) + [{
                            "stage": "attachment",
                            "message": str(exc)[:240],
                            "at": self._now().isoformat(),
                        }],
                    })
            item = await self.supabase.mark_accounts_payable_published(item_id, user_id, str(publication["id"]), provider="xero")
            item["xero_publication"] = self._safe_publication(publication)
            return item
        if publication and publication.get("status") in {"publishing", "indeterminate"}:
            raise ValueError("This Bill request may already be in Xero. Check the connected organisation before retrying.")

        payload = self._bill_payload(item, connection)
        now = self._now().isoformat()
        base_record = {
            "accounts_payable_item_id": item_id,
            "owner_user_id": user_id,
            "workspace_id": item.get("workspace_id"),
            "connection_id": connection["id"],
            "status": "publishing",
            "attachment_status": "pending" if item.get("attachment_visible") else "not_requested",
            "attempted_at": now,
            "updated_at": now,
        }
        if publication:
            self.client.table("xero_bill_publications").update(base_record).eq("id", publication["id"]).execute()
            publication_id = publication["id"]
        else:
            inserted = self.client.table("xero_bill_publications").insert(base_record).execute()
            publication_id = inserted.data[0]["id"]

        try:
            result = await self._api_post(connection, "Invoices", {"Invoices": [payload]})
        except XeroPublishError as exc:
            self._update_publication(publication_id, {
                "status": "indeterminate" if exc.indeterminate else "failed",
                "failure_details": [{"message": str(exc)[:240], "at": self._now().isoformat()}],
            })
            raise
        invoices = result.get("Invoices") or []
        invoice_id = invoices[0].get("InvoiceID") if invoices else None
        if not invoice_id:
            self._update_publication(publication_id, {"status": "failed"})
            raise XeroPublishError("Xero accepted the request but returned no invoice id")
        completed = self._update_publication(publication_id, {
            "status": "published",
            "xero_invoice_id": str(invoice_id),
            "published_at": self._now().isoformat(),
        })
        if item.get("attachment_visible"):
            try:
                await self._attach_source_to_bill(connection, item, str(invoice_id))
                completed = self._update_publication(publication_id, {"attachment_status": "attached"})
            except Exception as exc:
                completed = self._update_publication(publication_id, {
                    "attachment_status": "failed",
                    "failure_details": list(completed.get("failure_details") or []) + [{
                        "stage": "attachment",
                        "message": str(exc)[:240],
                        "at": self._now().isoformat(),
                    }],
                })
        item = await self.supabase.mark_accounts_payable_published(item_id, user_id, str(publication_id), provider="xero")
        item["xero_publication"] = self._safe_publication(completed)
        return item

    async def publish_accounts_payable_bills(self, item_ids: List[str], user_id: str) -> Dict[str, Any]:
        items: List[Dict[str, Any]] = []
        failures: List[Dict[str, str]] = []
        for item_id in item_ids:
            try:
                items.append(await self.publish_accounts_payable_bill(item_id, user_id))
            except ValueError as exc:
                failures.append({"item_id": item_id, "detail": str(exc)})
        return {"items": items, "failures": failures, "total": len(items)}

    @staticmethod
    def _safe_publication(publication: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": publication.get("id"),
            "status": publication.get("status"),
            "xero_invoice_id": publication.get("xero_invoice_id"),
            "attachment_status": publication.get("attachment_status"),
            "failure_details": publication.get("failure_details") or [],
            "attempted_at": publication.get("attempted_at"),
            "published_at": publication.get("published_at"),
            "updated_at": publication.get("updated_at"),
        }

    def _update_publication(self, publication_id: str, update: Dict[str, Any]) -> Dict[str, Any]:
        update = {**update, "updated_at": self._now().isoformat()}
        response = self.client.table("xero_bill_publications").update(update).eq("id", publication_id).execute()
        return response.data[0] if response.data else {"id": publication_id, **update}

    # ── status + disconnect ────────────────────────────────────────────────

    async def _safe_status(self, connection: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not connection:
            return {"connected": False, "status": "disconnected", "reference_counts": {}}
        rows = self.client.table("xero_reference_data")\
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
            "tenant_id": connection.get("tenant_id"),
            "company_name": connection.get("tenant_name"),
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
        # Xero token revocation is best-effort; clear local tokens regardless.
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                await client.post(
                    "https://identity.xero.com/connect/revocation",
                    auth=(settings.xero_client_id, settings.xero_client_secret),
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data={"token": self._decrypt(connection.get("refresh_token_ciphertext"))},
                )
        except Exception:
            pass
        now = self._now().isoformat()
        updated = self.client.table("xero_connections").update({
            "status": "disconnected",
            "access_token_ciphertext": None,
            "refresh_token_ciphertext": None,
            "access_token_expires_at": None,
            "refresh_token_expires_at": None,
            "disconnected_at": now,
            "updated_at": now,
        }).eq("id", connection["id"]).execute()
        return await self._safe_status(updated.data[0] if updated.data else None)


_xero_service: Optional[XeroService] = None


def get_xero_service() -> XeroService:
    global _xero_service
    if _xero_service is None:
        _xero_service = XeroService()
    return _xero_service
