"""SaaS billing and Stripe subscription lifecycle management."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import stripe

from core.database import User, get_db, init_orm_tables

logger = logging.getLogger(__name__)

# Plan catalog aligned with public pricing tiers
PLAN_DEVELOPER = "developer"
PLAN_GROWTH = "growth"
PLAN_ENTERPRISE = "enterprise"

SUBSCRIPTION_STATUS_ACTIVE = "active"
SUBSCRIPTION_STATUS_INACTIVE = "inactive"
SUBSCRIPTION_STATUS_CANCELED = "canceled"

PLAN_ALIASES: dict[str, str] = {
    "developer": PLAN_DEVELOPER,
    "free": PLAN_DEVELOPER,
    "growth": PLAN_GROWTH,
    "growth_scale": PLAN_GROWTH,
    "growth scale": PLAN_GROWTH,
    "enterprise": PLAN_ENTERPRISE,
    "enterprise_core": PLAN_ENTERPRISE,
    "enterprise core": PLAN_ENTERPRISE,
}


@dataclass(frozen=True)
class PlanConfig:
    key: str
    display_name: str
    amount_usd: int
    price_id_env: str | None


PLAN_CATALOG: dict[str, PlanConfig] = {
    PLAN_DEVELOPER: PlanConfig(
        key=PLAN_DEVELOPER,
        display_name="Developer",
        amount_usd=0,
        price_id_env=None,
    ),
    PLAN_GROWTH: PlanConfig(
        key=PLAN_GROWTH,
        display_name="Growth Scale",
        amount_usd=79,
        price_id_env="STRIPE_PRICE_GROWTH",
    ),
    PLAN_ENTERPRISE: PlanConfig(
        key=PLAN_ENTERPRISE,
        display_name="Enterprise Core",
        amount_usd=299,
        price_id_env="STRIPE_PRICE_ENTERPRISE",
    ),
}


class BillingError(Exception):
    """Raised when billing configuration or Stripe operations fail."""


def _stripe_api_key() -> str:
    api_key = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    if not api_key:
        raise BillingError("STRIPE_SECRET_KEY is not configured.")
    return api_key


def _stripe_webhook_secret() -> str:
    secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
    if not secret:
        raise BillingError("STRIPE_WEBHOOK_SECRET is not configured.")
    return secret


def _checkout_success_url() -> str:
    return os.environ.get("STRIPE_CHECKOUT_SUCCESS_URL", "http://localhost:5000/dashboard?billing=success")


def _checkout_cancel_url() -> str:
    return os.environ.get("STRIPE_CHECKOUT_CANCEL_URL", "http://localhost:5000/?billing=canceled")


def normalize_plan_type(plan_type: str) -> str:
    normalized = PLAN_ALIASES.get(str(plan_type or "").strip().lower())
    if normalized is None:
        raise BillingError(f"Unsupported plan type: {plan_type}")
    return normalized


def get_plan_config(plan_type: str) -> PlanConfig:
    return PLAN_CATALOG[normalize_plan_type(plan_type)]


def _resolve_price_id(plan: PlanConfig) -> str:
    if plan.key == PLAN_DEVELOPER:
        raise BillingError("Developer tier is free and does not require Stripe checkout.")
    if not plan.price_id_env:
        raise BillingError(f"Stripe price configuration missing for plan '{plan.display_name}'.")
    price_id = os.environ.get(plan.price_id_env, "").strip()
    if not price_id:
        raise BillingError(
            f"Environment variable {plan.price_id_env} is required for plan '{plan.display_name}'."
        )
    return price_id


def _configure_stripe() -> None:
    stripe.api_key = _stripe_api_key()


def _get_or_create_stripe_customer(user: User) -> str:
    if user.stripe_customer_id:
        return user.stripe_customer_id

    customer = stripe.Customer.create(
        email=user.email,
        name=user.company_name or user.email,
        metadata={
            "omnikube_user_id": str(user.id),
            "company_name": user.company_name,
        },
    )
    return str(customer["id"])


def create_checkout_session(user_id: int, plan_type: str) -> str:
    """
    Create a Stripe Checkout session for the given user and plan.

    Returns the hosted Stripe checkout URL.
    """
    init_orm_tables()
    plan = get_plan_config(plan_type)
    price_id = _resolve_price_id(plan)
    _configure_stripe()

    with get_db() as db:
        user = db.query(User).filter(User.id == user_id).one_or_none()
        if user is None:
            raise BillingError(f"User id={user_id} was not found.")

        customer_id = _get_or_create_stripe_customer(user)
        user.stripe_customer_id = customer_id

        checkout_session = stripe.checkout.Session.create(
            mode="subscription",
            customer=customer_id,
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=_checkout_success_url(),
            cancel_url=_checkout_cancel_url(),
            client_reference_id=str(user.id),
            metadata={
                "user_id": str(user.id),
                "plan_type": plan.key,
                "plan_name": plan.display_name,
            },
            subscription_data={
                "metadata": {
                    "user_id": str(user.id),
                    "plan_type": plan.key,
                }
            },
        )

        checkout_url = str(checkout_session.get("url") or "")
        if not checkout_url:
            raise BillingError("Stripe did not return a checkout URL.")

        logger.info(
            "Created Stripe checkout session for user_id=%s plan=%s",
            user_id,
            plan.key,
        )
        return checkout_url


def _activate_user_subscription(
    *,
    user_id: int,
    plan_type: str,
    stripe_customer_id: str | None,
    stripe_subscription_id: str | None,
) -> User:
    plan = get_plan_config(plan_type)

    with get_db() as db:
        user = db.query(User).filter(User.id == user_id).one_or_none()
        if user is None:
            raise BillingError(f"Unable to activate subscription — user id={user_id} not found.")

        user.subscription_tier = plan.key
        user.subscription_status = SUBSCRIPTION_STATUS_ACTIVE
        if stripe_customer_id:
            user.stripe_customer_id = stripe_customer_id
        if stripe_subscription_id:
            user.stripe_subscription_id = stripe_subscription_id

        db.flush()
        db.refresh(user)
        logger.info(
            "Activated subscription for user_id=%s tier=%s status=%s",
            user_id,
            user.subscription_tier,
            user.subscription_status,
        )
        return user


def verify_stripe_webhook_event(payload: bytes, sig_header: str) -> dict[str, Any]:
    """Verify Stripe webhook signature and return the parsed event."""
    if not payload:
        raise BillingError("Webhook payload is empty.")
    if not sig_header:
        raise BillingError("Missing Stripe-Signature header.")

    _configure_stripe()
    try:
        return stripe.Webhook.construct_event(payload, sig_header, _stripe_webhook_secret())
    except ValueError as exc:
        raise BillingError(f"Invalid webhook payload: {exc}") from exc
    except stripe.error.SignatureVerificationError as exc:
        raise BillingError("Invalid Stripe webhook signature.") from exc


def process_stripe_webhook_event(event: dict[str, Any]) -> dict[str, Any]:
    """Process a cryptographically verified Stripe webhook event."""
    event_type = str(event.get("type", ""))
    logger.info("Stripe webhook received: %s", event_type)

    if event_type != "checkout.session.completed":
        return {
            "status": "ignored",
            "event_type": event_type,
            "message": "Event acknowledged but not handled.",
        }

    session = event["data"]["object"]
    metadata = session.get("metadata") or {}
    user_id_raw = metadata.get("user_id") or session.get("client_reference_id")
    plan_type = metadata.get("plan_type") or PLAN_GROWTH

    if not user_id_raw:
        raise BillingError("checkout.session.completed missing user_id metadata.")

    user_id = int(user_id_raw)
    user = _activate_user_subscription(
        user_id=user_id,
        plan_type=str(plan_type),
        stripe_customer_id=str(session.get("customer") or "") or None,
        stripe_subscription_id=str(session.get("subscription") or "") or None,
    )

    return {
        "status": "processed",
        "event_type": event_type,
        "user_id": user.id,
        "subscription_tier": user.subscription_tier,
        "subscription_status": user.subscription_status,
    }


def handle_stripe_webhook(payload: bytes, sig_header: str) -> dict[str, Any]:
    """
    Verify a Stripe webhook signature and process subscription lifecycle events.

    Handles checkout.session.completed by upgrading the matching ORM user tier.
    """
    event = verify_stripe_webhook_event(payload, sig_header)
    return process_stripe_webhook_event(event)
