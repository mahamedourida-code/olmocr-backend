"""
Lemon Squeezy API adapter.

The rest of the app should work with plans and credits; this service only
translates those actions to Lemon Squeezy API calls.
"""

from typing import Any, Dict

import httpx

from app.core.config import settings


class LemonSqueezyService:
    base_url = "https://api.lemonsqueezy.com/v1"

    def __init__(self) -> None:
        self.api_key = settings.lemonsqueezy_api_key
        self.store_id = settings.lemonsqueezy_store_id

    def ensure_configured(self) -> None:
        if not self.api_key or not self.store_id:
            raise ValueError("Lemon Squeezy API key and store ID must be configured")

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Bearer {self.api_key}",
        }

    def plan_for_key(self, plan_key: str) -> Dict[str, Any]:
        plan = settings.lemonsqueezy_plan_variants.get(plan_key)
        if not plan or not plan.get("variant_id"):
            raise ValueError(f"Unsupported or unconfigured Lemon Squeezy plan: {plan_key}")
        return plan

    async def create_checkout(
        self,
        plan_key: str,
        user_id: str,
        email: str = "",
        name: str = ""
    ) -> Dict[str, Any]:
        self.ensure_configured()
        plan = self.plan_for_key(plan_key)
        frontend_url = settings.frontend_url.rstrip("/")

        payload = {
            "data": {
                "type": "checkouts",
                "attributes": {
                    "product_options": {
                        "enabled_variants": [int(plan["variant_id"])],
                        "redirect_url": f"{frontend_url}/pricing?checkout_status=success&plan={plan_key}",
                        "receipt_button_text": "Open AxLiner",
                        "receipt_link_url": f"{frontend_url}/dashboard/client?billing=pending",
                    },
                    "checkout_options": {
                        "button_color": "#7047EB",
                    },
                    "checkout_data": {
                        "email": email or "",
                        "name": name or "",
                        "custom": {
                            "user_id": user_id,
                            "plan_key": plan_key,
                        },
                    },
                },
                "relationships": {
                    "store": {
                        "data": {
                            "type": "stores",
                            "id": str(self.store_id),
                        }
                    },
                    "variant": {
                        "data": {
                            "type": "variants",
                            "id": str(plan["variant_id"]),
                        }
                    },
                },
            }
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.base_url}/checkouts",
                headers=self.headers,
                json=payload,
            )

        if response.status_code >= 400:
            raise RuntimeError(f"Lemon checkout failed: {response.status_code} {response.text}")

        data = response.json()
        attributes = data.get("data", {}).get("attributes", {})
        return {
            "checkout_id": data.get("data", {}).get("id"),
            "checkout_url": attributes.get("url"),
            "plan_key": plan_key,
            "plan": plan["plan"],
            "credits": plan["credits"],
        }


def get_lemon_squeezy_service() -> LemonSqueezyService:
    return LemonSqueezyService()
