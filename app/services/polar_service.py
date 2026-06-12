"""
Polar API adapter.

The rest of the app works with local plan keys and credits; this service only
translates checkout creation to Polar.
"""

from typing import Any, Dict

import httpx

from app.core.config import settings


class PolarService:
    def __init__(self) -> None:
        self.access_token = settings.polar_access_token
        self.base_url = settings.polar_api_base_url.rstrip("/")

    def ensure_configured(self) -> None:
        if not self.access_token:
            raise ValueError("Polar access token must be configured")

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

    def plan_for_key(self, plan_key: str) -> Dict[str, Any]:
        plan = settings.polar_plan_products.get(plan_key)
        if not plan or not plan.get("product_id"):
            raise ValueError(f"Unsupported or unconfigured Polar plan: {plan_key}")
        return plan

    async def create_checkout(
        self,
        plan_key: str,
        user_id: str,
        email: str = "",
        name: str = "",
    ) -> Dict[str, Any]:
        self.ensure_configured()
        plan = self.plan_for_key(plan_key)
        frontend_url = settings.frontend_url.rstrip("/")

        payload = {
            "products": [str(plan["product_id"])],
            "external_customer_id": user_id,
            "customer_email": email or None,
            "customer_name": name or None,
            "success_url": f"{frontend_url}/pricing?checkout_status=success&provider=polar&plan={plan_key}",
            "return_url": f"{frontend_url}/pricing?checkout_status=cancelled&provider=polar&plan={plan_key}",
            "metadata": {
                "user_id": user_id,
                "plan_key": plan_key,
                "plan": plan["plan"],
            },
            "customer_metadata": {
                "user_id": user_id,
            },
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.base_url}/checkouts/",
                headers=self.headers,
                json={key: value for key, value in payload.items() if value is not None},
            )

        if response.status_code >= 400:
            raise RuntimeError(f"Polar checkout failed: {response.status_code} {response.text}")

        data = response.json()
        return {
            "checkout_id": data.get("id") or data.get("checkout_id"),
            "checkout_url": data.get("url") or data.get("checkout_url") or data.get("session_url"),
            "plan_key": plan_key,
            "plan": plan["plan"],
            "credits": plan["credits"],
        }


def get_polar_service() -> PolarService:
    return PolarService()
