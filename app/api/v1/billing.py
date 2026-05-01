import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from app.core.config import settings
from app.core.dependencies import get_current_user
from app.core.limits import get_plan_limits
from app.services.lemon_squeezy_service import LemonSqueezyService, get_lemon_squeezy_service
from app.services.supabase_service import SupabaseService, get_supabase_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/billing", tags=["Billing"])


class CheckoutRequest(BaseModel):
    plan_key: str


def _format_usd(cents: int) -> str:
    amount = int(cents or 0) / 100
    return f"${amount:,.0f}" if amount.is_integer() else f"${amount:,.2f}"


def _annual_discount_percent(monthly_cents: int, yearly_cents: int) -> int:
    if not monthly_cents or not yearly_cents:
        return 0
    full_year = monthly_cents * 12
    if yearly_cents >= full_year:
        return 0
    return round((1 - (yearly_cents / full_year)) * 100)


def _public_plan(
    plan_key: str,
    plan_data: Dict[str, Any],
    monthly_reference_cents: Optional[int] = None
) -> Dict[str, Any]:
    limits = get_plan_limits(plan_data["plan"])
    price_cents = int(plan_data.get("price_cents") or 0)
    interval = plan_data.get("interval") or "month"
    credits = int(plan_data.get("credits") or 0)

    return {
        "key": plan_key,
        "checkout_key": plan_key,
        "name": plan_data.get("display_name") or plan_data["plan"].title(),
        "plan": plan_data["plan"],
        "interval": interval,
        "price_cents": price_cents,
        "price_formatted": _format_usd(price_cents),
        "currency": "USD",
        "credits": credits,
        "included_volume": f"{credits:,} pages",
        "max_files_per_batch": limits["max_files_per_batch"],
        "daily_image_limit": limits["daily_image_limit"],
        "max_file_size_mb": limits["max_file_size_mb"],
        "annual_discount_percent": (
            _annual_discount_percent(monthly_reference_cents, price_cents)
            if interval == "year" and monthly_reference_cents
            else 0
        ),
        "checkout_available": bool(
            settings.lemonsqueezy_api_key
            and settings.lemonsqueezy_store_id
            and plan_data.get("variant_id")
        ),
    }


def _billing_plan_catalog() -> Dict[str, Any]:
    variants = settings.lemonsqueezy_plan_variants
    free_limits = get_plan_limits("anonymous")
    pro_monthly = variants["pro_monthly"]["price_cents"]
    business_monthly = variants["business_monthly"]["price_cents"]

    paid_plans = [
        _public_plan("pro_monthly", variants["pro_monthly"]),
        _public_plan("pro_yearly", variants["pro_yearly"], pro_monthly),
        _public_plan("business_monthly", variants["business_monthly"]),
        _public_plan("business_yearly", variants["business_yearly"], business_monthly),
    ]

    return {
        "provider": "lemonsqueezy",
        "currency": "USD",
        "plans": [
            {
                "key": "free",
                "checkout_key": None,
                "name": "Free",
                "plan": "anonymous",
                "interval": "forever",
                "price_cents": 0,
                "price_formatted": "$0",
                "currency": "USD",
                "credits": free_limits["daily_image_limit"],
                "included_volume": f"{free_limits['daily_image_limit']:,} trial images",
                "max_files_per_batch": free_limits["max_files_per_batch"],
                "daily_image_limit": free_limits["daily_image_limit"],
                "max_file_size_mb": free_limits["max_file_size_mb"],
                "annual_discount_percent": 0,
                "checkout_available": False,
            },
            *paid_plans,
        ],
    }


def _verify_lemon_signature(raw_body: bytes, signature: str) -> bool:
    if not settings.lemonsqueezy_webhook_secret or not signature:
        return False

    digest = hmac.new(
        settings.lemonsqueezy_webhook_secret.encode("utf-8"),
        raw_body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(digest, signature)


def _event_hash(raw_body: bytes) -> str:
    return hashlib.sha256(raw_body).hexdigest()


def _payload_event_id(payload: Dict[str, Any]) -> str:
    return _event_hash(json.dumps(payload, sort_keys=True).encode("utf-8"))


def _parse_lemon_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _cancelled_access_is_still_valid(ends_at: Optional[str]) -> bool:
    parsed_ends_at = _parse_lemon_datetime(ends_at)
    return bool(parsed_ends_at and parsed_ends_at > datetime.now(timezone.utc))


def _effective_plan_for_subscription(
    status_value: str,
    cancelled: bool,
    ends_at: Optional[str],
    paid_plan: str
) -> str:
    status_normalized = (status_value or "").lower()

    if status_normalized in {"active", "on_trial", "past_due"}:
        return paid_plan

    if status_normalized == "cancelled" or cancelled:
        return paid_plan if _cancelled_access_is_still_valid(ends_at) else "free"

    if status_normalized in {"expired", "unpaid", "paused"}:
        return "free"

    return paid_plan


def _attrs(payload: Dict[str, Any]) -> Dict[str, Any]:
    return payload.get("data", {}).get("attributes", {}) or {}


def _meta(payload: Dict[str, Any]) -> Dict[str, Any]:
    return payload.get("meta", {}) or {}


def _custom_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    custom = _meta(payload).get("custom_data") or {}
    return custom if isinstance(custom, dict) else {}


def _resource(payload: Dict[str, Any]) -> Dict[str, Optional[str]]:
    data = payload.get("data", {}) or {}
    return {
        "type": data.get("type"),
        "id": str(data.get("id")) if data.get("id") is not None else None,
    }


def _urls(attrs: Dict[str, Any]) -> Dict[str, Any]:
    urls = attrs.get("urls") or {}
    return urls if isinstance(urls, dict) else {}


def _resolve_webhook_user_id(
    payload: Dict[str, Any],
    supabase: SupabaseService,
    provider_subscription_id: Optional[str],
    provider_customer_id: Optional[str]
) -> Optional[str]:
    custom = _custom_data(payload)
    if custom.get("user_id"):
        return str(custom["user_id"])
    if provider_subscription_id:
        user_id = supabase.find_user_for_provider_subscription(provider_subscription_id)
        if user_id:
            return user_id
    if provider_customer_id:
        return supabase.find_user_for_provider_customer(provider_customer_id)
    return None


def _subscription_id_from_event(payload: Dict[str, Any]) -> Optional[str]:
    attrs = _attrs(payload)
    data = payload.get("data", {}) or {}
    return str(attrs.get("subscription_id") or data.get("id") or "") or None


def _customer_id_from_event(payload: Dict[str, Any]) -> Optional[str]:
    attrs = _attrs(payload)
    return str(attrs.get("customer_id") or "") or None


def _plan_for_subscription_event(attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    variant_id = attrs.get("variant_id")
    return settings.lemonsqueezy_plan_for_variant(str(variant_id)) if variant_id else None


def _sync_subscription(payload: Dict[str, Any], supabase: SupabaseService, event_name: str) -> Dict[str, Any]:
    attrs = _attrs(payload)
    provider_subscription_id = _subscription_id_from_event(payload)
    provider_customer_id = _customer_id_from_event(payload)
    plan_data = _plan_for_subscription_event(attrs)
    if not provider_subscription_id or not plan_data:
        raise ValueError("Subscription event is missing subscription or configured variant")

    user_id = _resolve_webhook_user_id(payload, supabase, provider_subscription_id, provider_customer_id)
    if not user_id:
        raise ValueError("Could not resolve user for Lemon Squeezy subscription event")

    urls = _urls(attrs)
    status_value = attrs.get("status") or "inactive"
    cancelled = bool(attrs.get("cancelled"))

    supabase.upsert_billing_customer(
        user_id=user_id,
        provider_customer_id=provider_customer_id or attrs.get("customer_id"),
        email=attrs.get("user_email"),
        name=attrs.get("user_name"),
        portal_url=urls.get("customer_portal"),
        metadata={"last_event": event_name},
    )
    subscription = supabase.upsert_subscription(
        user_id=user_id,
        provider_subscription_id=provider_subscription_id,
        provider_customer_id=provider_customer_id,
        provider_variant_id=str(attrs.get("variant_id")),
        plan=plan_data["plan"],
        status=status_value,
        renews_at=attrs.get("renews_at"),
        ends_at=attrs.get("ends_at"),
        cancelled=cancelled,
        customer_portal_url=urls.get("customer_portal"),
        update_payment_method_url=urls.get("update_payment_method"),
        metadata={
            "plan_key": plan_data["plan_key"],
            "variant_name": attrs.get("variant_name"),
            "product_name": attrs.get("product_name"),
            "last_event": event_name,
        },
    )

    effective_plan = _effective_plan_for_subscription(
        status_value=status_value,
        cancelled=cancelled,
        ends_at=attrs.get("ends_at"),
        paid_plan=plan_data["plan"],
    )

    if event_name == "subscription_expired":
        effective_plan = "free"

    supabase.update_user_plan_type(user_id, effective_plan)

    if event_name == "subscription_created" and status_value in {"active", "on_trial"}:
        supabase.grant_plan_credits(
            user_id=user_id,
            credits=plan_data["credits"],
            movement_type="subscription_created",
            provider_subscription_id=provider_subscription_id,
            provider_event_id=_payload_event_id(payload),
            metadata={"plan_key": plan_data["plan_key"]},
        )

    return {"user_id": user_id, "subscription": subscription}


def _handle_subscription_payment(payload: Dict[str, Any], supabase: SupabaseService, event_name: str) -> Dict[str, Any]:
    attrs = _attrs(payload)
    provider_subscription_id = str(attrs.get("subscription_id") or "")
    provider_customer_id = str(attrs.get("customer_id") or "")
    if not provider_subscription_id:
        raise ValueError("Subscription payment event is missing subscription_id")

    user_id = _resolve_webhook_user_id(payload, supabase, provider_subscription_id, provider_customer_id)
    if not user_id:
        raise ValueError("Could not resolve user for Lemon Squeezy payment event")

    subscription = supabase.get_subscription_by_provider_id(provider_subscription_id)
    if not subscription:
        subscription = supabase.get_latest_subscription_for_user(user_id)
    plan_key = (subscription or {}).get("metadata", {}).get("plan_key")
    plan_data = settings.lemonsqueezy_plan_variants.get(plan_key or "")

    if event_name == "subscription_payment_success":
        if not plan_data:
            raise ValueError("Could not resolve plan credits for subscription payment")
        supabase.update_user_plan_type(user_id, plan_data["plan"])
        credits = supabase.grant_plan_credits(
            user_id=user_id,
            credits=plan_data["credits"],
            movement_type="subscription_payment_success",
            provider_subscription_id=provider_subscription_id,
            provider_order_id=str(attrs.get("order_id") or ""),
            provider_event_id=_payload_event_id(payload),
            metadata={"plan_key": plan_key},
        )
        return {"user_id": user_id, "credits": credits}

    if subscription:
        metadata = {
            **(subscription.get("metadata") or {}),
            "last_event": event_name,
            "access_note": "past_due_dunning_keep_existing_access",
        }
        supabase.upsert_subscription(
            user_id=user_id,
            provider_subscription_id=provider_subscription_id,
            provider_customer_id=provider_customer_id,
            provider_variant_id=subscription.get("provider_variant_id"),
            plan=subscription.get("plan") or "free",
            status="past_due",
            renews_at=subscription.get("renews_at"),
            ends_at=subscription.get("ends_at"),
            cancelled=bool(subscription.get("cancelled")),
            customer_portal_url=subscription.get("customer_portal_url"),
            update_payment_method_url=subscription.get("update_payment_method_url"),
            metadata=metadata,
        )
    return {"user_id": user_id, "status": "past_due"}


def _handle_order_created(payload: Dict[str, Any]) -> Dict[str, Any]:
    attrs = _attrs(payload)
    return {
        "ignored": True,
        "event_name": "order_created",
        "reason": "subscription entitlements are driven by subscription events",
        "order_id": attrs.get("order_id") or payload.get("data", {}).get("id"),
    }


def _process_lemon_webhook(payload: Dict[str, Any], supabase: SupabaseService, event_name: str) -> Dict[str, Any]:
    if event_name in {
        "subscription_created",
        "subscription_updated",
        "subscription_cancelled",
        "subscription_resumed",
        "subscription_expired",
        "subscription_paused",
    }:
        return _sync_subscription(payload, supabase, event_name)

    if event_name in {"subscription_payment_success", "subscription_payment_failed"}:
        return _handle_subscription_payment(payload, supabase, event_name)

    if event_name == "order_created":
        return _handle_order_created(payload)

    return {"ignored": True, "event_name": event_name}


@router.post("/lemon/checkout")
async def create_lemon_checkout(
    checkout_request: CheckoutRequest,
    user: dict = Depends(get_current_user),
    lemon: LemonSqueezyService = Depends(get_lemon_squeezy_service)
):
    try:
        return await lemon.create_checkout(
            plan_key=checkout_request.plan_key,
            user_id=user["user_id"],
            email=user.get("email") or "",
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create Lemon checkout: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Could not create checkout. Please try again shortly."
        )


@router.get("/plans")
async def billing_plans():
    return _billing_plan_catalog()


@router.post("/lemon/webhook")
async def lemon_webhook(request: Request):
    raw_body = await request.body()
    signature = request.headers.get("X-Signature", "")

    if not _verify_lemon_signature(raw_body, signature):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook signature")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid webhook JSON")

    event_name = _meta(payload).get("event_name") or request.headers.get("X-Event-Name") or "unknown"
    resource = _resource(payload)
    event_hash = _event_hash(raw_body)
    supabase = get_supabase_service()

    event_record = supabase.record_webhook_event(
        event_hash=event_hash,
        event_name=event_name,
        payload=payload,
        resource_type=resource["type"],
        resource_id=resource["id"],
    )
    if event_record is None:
        return {"status": "duplicate"}

    try:
        result = _process_lemon_webhook(payload, supabase, event_name)
        supabase.mark_webhook_event_processed(event_hash, True)
        return {"status": "processed", "event_name": event_name, "result": result}
    except Exception as e:
        logger.error(f"Failed to process Lemon webhook {event_name}: {e}", exc_info=True)
        supabase.mark_webhook_event_processed(event_hash, False, str(e))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Webhook processing failed")


@router.get("/status")
async def billing_status(
    user: dict = Depends(get_current_user),
    supabase: SupabaseService = Depends(get_supabase_service)
):
    return supabase.get_billing_status(user["user_id"])


@router.get("/portal")
async def billing_portal(
    user: dict = Depends(get_current_user),
    supabase: SupabaseService = Depends(get_supabase_service)
):
    subscription = supabase.get_latest_subscription_for_user(user["user_id"])
    customer = supabase.get_billing_customer_for_user(user["user_id"])
    portal_url = (
        (subscription or {}).get("customer_portal_url")
        or (customer or {}).get("portal_url")
    )

    if not portal_url:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Billing portal is not available yet for this user"
        )

    return {"url": portal_url}
