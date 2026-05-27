"""Backend-only QuickBooks Online connection, reference data, and reviewed Bill publishing."""

import hashlib
import json
import secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import httpx
from cryptography.fernet import Fernet, InvalidToken

from app.core.config import settings
from app.services.supabase_service import get_supabase_service


class QuickBooksPublishError(ValueError):
    """A publication error whose uncertainty determines whether a Bill may be retried."""

    def __init__(self, message: str, indeterminate: bool = False) -> None:
        super().__init__(message)
        self.indeterminate = indeterminate


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

    async def _accounting_post(
        self,
        connection: Dict[str, Any],
        endpoint: str,
        *,
        json_payload: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        token = await self._access_token(connection)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
            async with httpx.AsyncClient(timeout=45.0) as client:
                response = await client.post(endpoint, headers=headers, json=json_payload, files=files)
                if response.status_code == 401:
                    headers["Authorization"] = f"Bearer {await self._refresh_access_token(connection)}"
                    response = await client.post(endpoint, headers=headers, json=json_payload, files=files)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise QuickBooksPublishError(
                "QuickBooks did not confirm the request. Check the connected company before retrying.",
                indeterminate=True,
            ) from exc
        if response.status_code >= 500:
            raise QuickBooksPublishError(
                "QuickBooks did not confirm the request. Check the connected company before retrying.",
                indeterminate=True,
            )
        if response.status_code >= 400:
            detail = "QuickBooks rejected this bill. Check vendor, account, tax, date, and currency selections."
            try:
                fault = response.json().get("Fault", {}).get("Error", [])
                if fault and fault[0].get("Detail"):
                    detail = str(fault[0]["Detail"])[:240]
            except (ValueError, AttributeError, TypeError):
                pass
            raise QuickBooksPublishError(detail)
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
                raise ValueError(f"Select a synced QuickBooks {label} before publishing")
            return None
        response = self.client.table("quickbooks_reference_data")\
            .select("external_id,display_name,active,details")\
            .eq("connection_id", connection["id"])\
            .eq("owner_user_id", owner_user_id)\
            .eq("resource_type", resource_type)\
            .eq("external_id", external_id)\
            .limit(1)\
            .execute()
        if not response.data or not response.data[0].get("active", True):
            raise ValueError(f"Refresh QuickBooks lists and select an active {label}")
        return response.data[0]

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
        vendor = self._selected_reference(
            connection, item["owner_user_id"], "vendor", draft.get("vendor_ref_id"), "vendor"
        )
        account = self._selected_reference(
            connection, item["owner_user_id"], "account", draft.get("account_ref_id"), "account"
        )
        tax_code = self._selected_reference(
            connection,
            item["owner_user_id"],
            "tax_code",
            draft.get("tax_code_ref_id"),
            "tax code",
            required=False,
        )
        assert vendor is not None and account is not None
        detail_base: Dict[str, Any] = {
            "AccountRef": {"value": account["external_id"], "name": account["display_name"]},
            "BillableStatus": "NotBillable",
        }
        if tax_code:
            detail_base["TaxCodeRef"] = {"value": tax_code["external_id"], "name": tax_code["display_name"]}
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
            line: Dict[str, Any] = {
                "Amount": float(amount),
                "DetailType": "AccountBasedExpenseLineDetail",
                "AccountBasedExpenseLineDetail": dict(detail_base),
            }
            description = raw.get("description") or raw.get("item") or raw.get("name")
            if description:
                line["Description"] = str(description)[:4000]
            lines.append(line)
        if not lines:
            total = self._amount(draft.get("total"))
            if total is None:
                raise ValueError("Add a total or valid line amounts before publishing")
            lines = [{
                "Amount": float(total),
                "Description": str(draft.get("reference") or item.get("source_filename") or "Reviewed invoice")[:4000],
                "DetailType": "AccountBasedExpenseLineDetail",
                "AccountBasedExpenseLineDetail": dict(detail_base),
            }]
        payload: Dict[str, Any] = {
            "VendorRef": {"value": vendor["external_id"], "name": vendor["display_name"]},
            "Line": lines,
        }
        if draft.get("invoice_number"):
            payload["DocNumber"] = str(draft["invoice_number"])[:21]
        if draft.get("invoice_date"):
            payload["TxnDate"] = str(draft["invoice_date"])
        if draft.get("due_date"):
            payload["DueDate"] = str(draft["due_date"])
        if draft.get("currency"):
            payload["CurrencyRef"] = {"value": str(draft["currency"]).upper()}
        return payload

    def _reviewed_receipt_payload(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Merge durable reviewed receipt fields and source-linked line items."""
        payload = self.supabase._duplicate_review_payload(document)
        line_items: List[Dict[str, Any]] = []
        for extraction in sorted(
            document.get("extractions") or [],
            key=lambda row: (
                self.supabase._document_metadata(row).get("source_page") is None,
                self.supabase._document_metadata(row).get("source_page") or 0,
            ),
        ):
            extracted_lines = self.supabase._initial_review_data(extraction).get("line_items")
            if isinstance(extracted_lines, list):
                line_items.extend(item for item in extracted_lines if isinstance(item, dict))
        payload["line_items"] = line_items
        return payload

    def _receipt_lines(
        self,
        payload: Dict[str, Any],
        connection: Dict[str, Any],
        user_id: str,
        account_ref_id: str,
        tax_code_ref_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        account = self._selected_reference(connection, user_id, "account", account_ref_id, "expense account")
        tax_code = self._selected_reference(
            connection, user_id, "tax_code", tax_code_ref_id, "tax code", required=False
        )
        assert account is not None
        detail: Dict[str, Any] = {
            "AccountRef": {"value": account["external_id"], "name": account["display_name"]},
            "BillableStatus": "NotBillable",
        }
        if tax_code:
            detail["TaxCodeRef"] = {"value": tax_code["external_id"], "name": tax_code["display_name"]}
        lines: List[Dict[str, Any]] = []
        for raw in payload.get("line_items") or []:
            amount = (
                self._amount(raw.get("line_total"))
                or self._amount(raw.get("total"))
                or self._amount(raw.get("amount"))
            )
            if amount is None:
                continue
            line: Dict[str, Any] = {
                "Amount": float(amount),
                "DetailType": "AccountBasedExpenseLineDetail",
                "AccountBasedExpenseLineDetail": dict(detail),
            }
            description = raw.get("description") or raw.get("item") or raw.get("name")
            if description:
                line["Description"] = str(description)[:4000]
            lines.append(line)
        if not lines:
            total = self._amount(payload.get("total"))
            if total is None:
                raise ValueError("Confirm a receipt total or line amounts before publishing")
            lines.append({
                "Amount": float(total),
                "Description": str(payload.get("merchant") or "Reviewed receipt")[:4000],
                "DetailType": "AccountBasedExpenseLineDetail",
                "AccountBasedExpenseLineDetail": dict(detail),
            })
        return lines

    def _receipt_transaction_payload(
        self,
        document: Dict[str, Any],
        connection: Dict[str, Any],
        user_id: str,
        request: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        reviewed = self._reviewed_receipt_payload(document)
        lines = self._receipt_lines(
            reviewed,
            connection,
            user_id,
            str(request["account_ref_id"]),
            request.get("tax_code_ref_id"),
        )
        destination = str(request["destination"])
        vendor = self._selected_reference(
            connection,
            user_id,
            "vendor",
            request.get("vendor_ref_id"),
            "vendor",
            required=destination == "bill",
        )
        if destination == "bill":
            assert vendor is not None
            result: Dict[str, Any] = {
                "VendorRef": {"value": vendor["external_id"], "name": vendor["display_name"]},
                "Line": lines,
            }
            entity_type = "Bill"
        else:
            payment_type = request.get("payment_type")
            if not payment_type or not request.get("payment_account_ref_id"):
                raise ValueError("Select a paid-from account and payment type for an expense")
            payment_account = self._selected_reference(
                connection,
                user_id,
                "account",
                request.get("payment_account_ref_id"),
                "paid-from account",
            )
            assert payment_account is not None
            payment_account_type = str((payment_account.get("details") or {}).get("account_type") or "").replace(" ", "").casefold()
            if payment_type == "CreditCard" and payment_account_type != "creditcard":
                raise ValueError("Credit card expenses must use a QuickBooks credit card account")
            if payment_type in {"Cash", "Check"} and payment_account_type != "bank":
                raise ValueError("Cash or check expenses must use a QuickBooks bank account")
            result = {
                "AccountRef": {
                    "value": payment_account["external_id"],
                    "name": payment_account["display_name"],
                },
                "PaymentType": payment_type,
                "Line": lines,
            }
            if vendor:
                result["EntityRef"] = {
                    "value": vendor["external_id"],
                    "name": vendor["display_name"],
                    "type": "Vendor",
                }
            entity_type = "Purchase"
        if reviewed.get("date"):
            result["TxnDate"] = str(reviewed["date"])
        if reviewed.get("currency"):
            result["CurrencyRef"] = {"value": str(reviewed["currency"]).upper()}
        return entity_type, result

    async def _remember_receipt_coding(
        self,
        document: Dict[str, Any],
        connection: Dict[str, Any],
        user_id: str,
        request: Dict[str, Any],
    ) -> None:
        """Persist mappings chosen during a successful receipt publication for later suggestions."""
        existing = await self.supabase.get_vendor_suggestion(document, user_id)
        fields = dict((existing or {}).get("suggested_fields") or {})
        vendor = self._selected_reference(
            connection, user_id, "vendor", request.get("vendor_ref_id"), "vendor", required=False
        )
        account = self._selected_reference(
            connection, user_id, "account", request.get("account_ref_id"), "expense account"
        )
        tax_code = self._selected_reference(
            connection, user_id, "tax_code", request.get("tax_code_ref_id"), "tax code", required=False
        )
        assert account is not None
        fields.update({
            "account_ref_id": account["external_id"],
            "category_account": account["display_name"],
        })
        fields.pop("destination_treatment", None)
        if vendor:
            fields["vendor_ref_id"] = vendor["external_id"]
        if tax_code:
            fields["tax_code_ref_id"] = tax_code["external_id"]
            fields["tax_code"] = tax_code["display_name"]
        await self.supabase.save_vendor_rule_from_document(
            str(document["job_id"]), str(document["id"]), user_id, fields
        )

    @staticmethod
    def _safe_publication(publication: Dict[str, Any]) -> Dict[str, Any]:
        allowed = {
            "id",
            "status",
            "attempt_count",
            "quickbooks_bill_id",
            "quickbooks_attachment_id",
            "attachment_status",
            "failure_details",
            "attempted_at",
            "published_at",
            "updated_at",
        }
        return {key: value for key, value in publication.items() if key in allowed}

    def _update_publication(self, publication_id: str, update: Dict[str, Any]) -> Dict[str, Any]:
        response = self.client.table("quickbooks_bill_publications")\
            .update({**update, "updated_at": self._now().isoformat()})\
            .eq("id", publication_id)\
            .execute()
        if not response.data:
            raise ValueError("QuickBooks publication record could not be updated")
        return response.data[0]

    @staticmethod
    def _safe_receipt_publication(publication: Dict[str, Any]) -> Dict[str, Any]:
        allowed = {
            "id",
            "destination",
            "remote_entity_type",
            "status",
            "attempt_count",
            "quickbooks_remote_id",
            "quickbooks_attachment_id",
            "attachment_status",
            "failure_details",
            "attempted_at",
            "published_at",
            "updated_at",
        }
        return {key: value for key, value in publication.items() if key in allowed}

    def _update_receipt_publication(self, publication_id: str, update: Dict[str, Any]) -> Dict[str, Any]:
        response = self.client.table("quickbooks_receipt_publications")\
            .update({**update, "updated_at": self._now().isoformat()})\
            .eq("id", publication_id)\
            .execute()
        if not response.data:
            raise ValueError("QuickBooks receipt publication record could not be updated")
        return response.data[0]

    async def _attach_source_to_entity(
        self,
        connection: Dict[str, Any],
        source_storage_path: str,
        source_filename: str,
        source_content_type: Optional[str],
        entity_type: str,
        entity_id: str,
    ) -> str:
        content = await self.supabase.download_file_from_storage(source_storage_path)
        metadata = {
            "AttachableRef": [{"EntityRef": {"type": entity_type, "value": entity_id}}],
            "Note": "Source document reviewed in AxLiner.",
        }
        payload = await self._accounting_post(
            connection,
            f"{self._company_base_url}/v3/company/{connection['realm_id']}/upload?minorversion={settings.quickbooks_minor_version}",
            files={
                "file_metadata_01": ("attachment.json", json.dumps(metadata), "application/json"),
                "file_content_01": (
                    source_filename,
                    content,
                    str(source_content_type or "application/octet-stream"),
                ),
            },
        )
        entries = payload.get("AttachableResponse") or []
        attachable = entries[0].get("Attachable") if entries and isinstance(entries[0], dict) else payload.get("Attachable")
        if not attachable or not attachable.get("Id"):
            raise ValueError("QuickBooks returned no attachment identifier")
        return str(attachable["Id"])

    async def _attach_source_to_bill(
        self,
        connection: Dict[str, Any],
        item: Dict[str, Any],
        bill_id: str,
    ) -> str:
        return await self._attach_source_to_entity(
            connection,
            str(item["source_storage_path"]),
            str(item.get("source_filename") or "source-document"),
            item.get("source_content_type"),
            "Bill",
            bill_id,
        )

    async def publish_accounts_payable_bill(self, item_id: str, user_id: str) -> Dict[str, Any]:
        """Publish one explicitly ready AP invoice as one unpaid QBO Bill."""
        item = await self.supabase.get_accounts_payable_item(item_id, user_id)
        if item.get("status") not in {"ready_to_publish", "published"}:
            raise ValueError("Only Ready to publish invoice items can be sent to QuickBooks")
        if item.get("status") == "ready_to_publish" and not str(
            (item.get("draft_data") or {}).get("due_date") or ""
        ).strip():
            raise ValueError("Complete the due date before publishing")
        _, connection = await self._owned_connection(user_id, item.get("workspace_id"), require_connected=True)
        assert connection is not None
        existing = self.client.table("quickbooks_bill_publications")\
            .select("*")\
            .eq("accounts_payable_item_id", item_id)\
            .eq("owner_user_id", user_id)\
            .limit(1)\
            .execute()
        publication = existing.data[0] if existing.data else None
        if item.get("status") == "published" and (
            not publication or not publication.get("quickbooks_bill_id")
        ):
            raise ValueError("This published item has no recorded QuickBooks Bill and cannot be published again")
        if publication and publication.get("quickbooks_bill_id"):
            if (
                item.get("attachment_visible")
                and publication.get("attachment_status") != "attached"
            ):
                try:
                    attachment_id = await self._attach_source_to_bill(
                        connection, item, str(publication["quickbooks_bill_id"])
                    )
                    publication = self._update_publication(publication["id"], {
                        "attachment_status": "attached",
                        "quickbooks_attachment_id": attachment_id,
                    })
                except Exception as exc:
                    publication = self._update_publication(publication["id"], {
                        "attachment_status": "failed",
                        "failure_details": list(publication.get("failure_details") or []) + [{
                            "stage": "attachment",
                            "message": str(exc)[:240],
                            "at": self._now().isoformat(),
                        }],
                    })
            item = await self.supabase.mark_accounts_payable_quickbooks_published(
                item_id, user_id, str(publication["id"])
            )
            item["quickbooks_publication"] = self._safe_publication(publication)
            return item
        if publication and publication.get("status") in {"publishing", "indeterminate"}:
            raise ValueError(
                "This Bill request may already be in QuickBooks. Check the connected company before retrying."
            )
        payload = self._bill_payload(item, connection)
        now = self._now().isoformat()
        if publication:
            failures = list(publication.get("failure_details") or [])
            updated = self.client.table("quickbooks_bill_publications")\
                .update({
                    "status": "publishing",
                    "attempt_count": int(publication.get("attempt_count") or 0) + 1,
                    "payload_snapshot": payload,
                    "attempted_at": now,
                    "status_history": list(publication.get("status_history") or []) + [{"status": "publishing", "at": now}],
                    "updated_at": now,
                    "failure_details": failures,
                })\
                .eq("id", publication["id"])\
                .eq("status", "failed")\
                .execute()
            if not updated.data:
                raise ValueError("This Bill is already being published")
            publication = updated.data[0]
        else:
            try:
                inserted = self.client.table("quickbooks_bill_publications").insert({
                    "accounts_payable_item_id": item_id,
                    "document_id": item["document_id"],
                    "job_id": item["job_id"],
                    "owner_user_id": user_id,
                    "workspace_id": item["workspace_id"],
                    "connection_id": connection["id"],
                    "idempotency_key": f"quickbooks:bill:ap:{item_id}",
                    "status": "publishing",
                    "payload_snapshot": payload,
                    "attempted_at": now,
                    "status_history": [{"status": "publishing", "at": now}],
                    "attachment_status": "pending" if item.get("attachment_visible") else "not_requested",
                }).execute()
                publication = inserted.data[0]
            except Exception:
                raise ValueError("This Bill is already being published")
        try:
            response = await self._accounting_post(
                connection,
                f"{self._company_base_url}/v3/company/{connection['realm_id']}/bill?minorversion={settings.quickbooks_minor_version}",
                json_payload=payload,
            )
            bill = response.get("Bill") or {}
            if not bill.get("Id"):
                raise QuickBooksPublishError(
                    "QuickBooks did not return a Bill identifier. Check the company before retrying.",
                    indeterminate=True,
                )
        except QuickBooksPublishError as exc:
            state = "indeterminate" if exc.indeterminate else "failed"
            self._update_publication(publication["id"], {
                "status": state,
                "failure_details": list(publication.get("failure_details") or []) + [{
                    "stage": "bill",
                    "message": str(exc)[:240],
                    "at": self._now().isoformat(),
                }],
                "status_history": list(publication.get("status_history") or []) + [{"status": state, "at": self._now().isoformat()}],
            })
            raise ValueError(str(exc))
        publication = self._update_publication(publication["id"], {
            "status": "published",
            "quickbooks_bill_id": str(bill["Id"]),
            "quickbooks_sync_token": str(bill.get("SyncToken") or ""),
            "published_at": self._now().isoformat(),
            "status_history": list(publication.get("status_history") or []) + [{"status": "published", "at": self._now().isoformat()}],
        })
        if item.get("attachment_visible"):
            try:
                attachment_id = await self._attach_source_to_bill(connection, item, str(bill["Id"]))
                publication = self._update_publication(publication["id"], {
                    "quickbooks_attachment_id": attachment_id,
                    "attachment_status": "attached",
                })
            except Exception as exc:
                publication = self._update_publication(publication["id"], {
                    "attachment_status": "failed",
                    "failure_details": list(publication.get("failure_details") or []) + [{
                        "stage": "attachment",
                        "message": str(exc)[:240],
                        "at": self._now().isoformat(),
                    }],
                })
        item = await self.supabase.mark_accounts_payable_quickbooks_published(
            item_id, user_id, str(publication["id"])
        )
        item["quickbooks_publication"] = self._safe_publication(publication)
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

    async def publish_reviewed_receipt(
        self,
        job_id: str,
        document_id: str,
        user_id: str,
        request: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Publish one reviewed receipt only after the user selects its QBO treatment."""
        document = await self.supabase.get_job_document(job_id, document_id)
        if not document or document.get("owner_user_id") != user_id:
            raise ValueError("Receipt not found")
        reviewed = self._reviewed_receipt_payload(document)
        if self.supabase._duplicate_document_mode(document, reviewed) != "receipt":
            raise ValueError("Only reviewed receipts can be published from this action")
        if document.get("review_status") not in {"ready", "published"}:
            raise ValueError("Confirm this receipt as Ready before publishing")
        if self.supabase.active_duplicate_warnings(document):
            raise ValueError("Resolve possible duplicate warnings before publishing this receipt")
        destination = str(request.get("destination") or "")
        if destination not in {"expense", "bill"}:
            raise ValueError("Choose Expense or Bill before publishing this receipt")
        workspace_id = document.get("workspace_id") or await self.supabase.resolve_owned_workspace_id(user_id)
        if not workspace_id:
            raise ValueError("Select a workspace before publishing")
        _, connection = await self._owned_connection(user_id, workspace_id, require_connected=True)
        assert connection is not None
        entity_type, payload = self._receipt_transaction_payload(document, connection, user_id, request)
        existing = self.client.table("quickbooks_receipt_publications")\
            .select("*")\
            .eq("document_id", document_id)\
            .eq("owner_user_id", user_id)\
            .limit(1)\
            .execute()
        publication = existing.data[0] if existing.data else None
        if publication and publication.get("destination") != destination:
            raise ValueError("This receipt already has a QuickBooks destination choice and cannot be republished differently")
        if document.get("review_status") == "published" and (
            not publication or not publication.get("quickbooks_remote_id")
        ):
            raise ValueError("This published receipt has no recorded QuickBooks transaction and cannot be published again")
        if publication and publication.get("quickbooks_remote_id"):
            if publication.get("attachment_status") != "attached":
                try:
                    attachment_id = await self._attach_source_to_entity(
                        connection,
                        str(document["source_storage_path"]),
                        str(document.get("original_filename") or "receipt"),
                        document.get("source_content_type"),
                        str(publication["remote_entity_type"]),
                        str(publication["quickbooks_remote_id"]),
                    )
                    publication = self._update_receipt_publication(publication["id"], {
                        "attachment_status": "attached",
                        "quickbooks_attachment_id": attachment_id,
                    })
                except Exception as exc:
                    publication = self._update_receipt_publication(publication["id"], {
                        "attachment_status": "failed",
                        "failure_details": list(publication.get("failure_details") or []) + [{
                            "stage": "attachment",
                            "message": str(exc)[:240],
                            "at": self._now().isoformat(),
                        }],
                    })
            refreshed = await self.supabase.mark_receipt_quickbooks_published(
                job_id, document_id, user_id, str(publication["id"]), destination
            )
            try:
                await self._remember_receipt_coding(document, connection, user_id, request)
            except Exception:
                pass
            refreshed["quickbooks_receipt_publication"] = self._safe_receipt_publication(publication)
            return refreshed
        if publication and publication.get("status") in {"publishing", "indeterminate"}:
            raise ValueError(
                "This receipt may already exist in QuickBooks. Check the connected company before retrying."
            )
        now = self._now().isoformat()
        coding_snapshot = {
            key: request.get(key)
            for key in (
                "destination",
                "vendor_ref_id",
                "account_ref_id",
                "tax_code_ref_id",
                "payment_account_ref_id",
                "payment_type",
            )
            if request.get(key)
        }
        if publication:
            updated = self.client.table("quickbooks_receipt_publications")\
                .update({
                    "status": "publishing",
                    "attempt_count": int(publication.get("attempt_count") or 0) + 1,
                    "coding_snapshot": coding_snapshot,
                    "payload_snapshot": payload,
                    "attempted_at": now,
                    "status_history": list(publication.get("status_history") or []) + [{"status": "publishing", "at": now}],
                    "updated_at": now,
                })\
                .eq("id", publication["id"])\
                .eq("status", "failed")\
                .execute()
            if not updated.data:
                raise ValueError("This receipt is already being published")
            publication = updated.data[0]
        else:
            try:
                inserted = self.client.table("quickbooks_receipt_publications").insert({
                    "document_id": document_id,
                    "job_id": job_id,
                    "owner_user_id": user_id,
                    "workspace_id": workspace_id,
                    "connection_id": connection["id"],
                    "destination": destination,
                    "remote_entity_type": entity_type,
                    "idempotency_key": f"quickbooks:receipt:{destination}:{document_id}",
                    "status": "publishing",
                    "coding_snapshot": coding_snapshot,
                    "payload_snapshot": payload,
                    "attempted_at": now,
                    "status_history": [{"status": "publishing", "at": now}],
                    "attachment_status": "pending",
                }).execute()
                publication = inserted.data[0]
            except Exception:
                raise ValueError("This receipt is already being published")
        endpoint = "purchase" if destination == "expense" else "bill"
        try:
            response = await self._accounting_post(
                connection,
                f"{self._company_base_url}/v3/company/{connection['realm_id']}/{endpoint}?minorversion={settings.quickbooks_minor_version}",
                json_payload=payload,
            )
            transaction = response.get(entity_type) or {}
            if not transaction.get("Id"):
                raise QuickBooksPublishError(
                    "QuickBooks did not return a transaction identifier. Check the company before retrying.",
                    indeterminate=True,
                )
        except QuickBooksPublishError as exc:
            publish_state = "indeterminate" if exc.indeterminate else "failed"
            self._update_receipt_publication(publication["id"], {
                "status": publish_state,
                "failure_details": list(publication.get("failure_details") or []) + [{
                    "stage": endpoint,
                    "message": str(exc)[:240],
                    "at": self._now().isoformat(),
                }],
                "status_history": list(publication.get("status_history") or []) + [{
                    "status": publish_state,
                    "at": self._now().isoformat(),
                }],
            })
            raise ValueError(str(exc))
        publication = self._update_receipt_publication(publication["id"], {
            "status": "published",
            "quickbooks_remote_id": str(transaction["Id"]),
            "quickbooks_sync_token": str(transaction.get("SyncToken") or ""),
            "published_at": self._now().isoformat(),
            "status_history": list(publication.get("status_history") or []) + [{
                "status": "published",
                "at": self._now().isoformat(),
            }],
        })
        try:
            attachment_id = await self._attach_source_to_entity(
                connection,
                str(document["source_storage_path"]),
                str(document.get("original_filename") or "receipt"),
                document.get("source_content_type"),
                entity_type,
                str(transaction["Id"]),
            )
            publication = self._update_receipt_publication(publication["id"], {
                "attachment_status": "attached",
                "quickbooks_attachment_id": attachment_id,
            })
        except Exception as exc:
            publication = self._update_receipt_publication(publication["id"], {
                "attachment_status": "failed",
                "failure_details": list(publication.get("failure_details") or []) + [{
                    "stage": "attachment",
                    "message": str(exc)[:240],
                    "at": self._now().isoformat(),
                }],
            })
        refreshed = await self.supabase.mark_receipt_quickbooks_published(
            job_id, document_id, user_id, str(publication["id"]), destination
        )
        try:
            await self._remember_receipt_coding(document, connection, user_id, request)
        except Exception:
            pass
        refreshed["quickbooks_receipt_publication"] = self._safe_receipt_publication(publication)
        return refreshed

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
