"""OAuth2 / OpenID Connect integration for enterprise single sign-on."""

from __future__ import annotations

import json
import logging
import os
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from core.database import DEFAULT_ORGANIZATION_ID

logger = logging.getLogger(__name__)

OAUTH_STATE_COOKIE = "omnikube_oauth_state"
OAUTH_STATE_TTL_SEC = 600
SIMULATED_ISSUER_PATH = "/auth/sso"

_oauth_lock = threading.Lock()
_pending_states: dict[str, dict[str, Any]] = {}
_authorization_codes: dict[str, dict[str, Any]] = {}


@dataclass(frozen=True)
class OAuth2Config:
    """Runtime OIDC client configuration."""

    issuer: str
    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: str
    simulated: bool

    @classmethod
    def from_request_base(cls, base_url: str) -> OAuth2Config:
        issuer = os.environ.get("OMNIKUBE_OIDC_ISSUER", "").strip().rstrip("/")
        client_id = os.environ.get("OMNIKUBE_OIDC_CLIENT_ID", "omnikube-portal").strip()
        client_secret = os.environ.get("OMNIKUBE_OIDC_CLIENT_SECRET", "").strip()
        redirect_uri = os.environ.get(
            "OMNIKUBE_OIDC_REDIRECT_URI",
            f"{base_url.rstrip('/')}/auth/callback",
        ).strip()
        scopes = os.environ.get(
            "OMNIKUBE_OIDC_SCOPES",
            "openid profile email",
        ).strip()

        simulated = not issuer
        if simulated:
            issuer = f"{base_url.rstrip('/')}{SIMULATED_ISSUER_PATH}"

        return cls(
            issuer=issuer,
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scopes=scopes,
            simulated=simulated,
        )


def organization_id_from_email(email: str) -> str:
    """Map a corporate email domain to a tenant organization identifier."""
    normalized = str(email or "").strip().lower()
    if "@" not in normalized:
        return DEFAULT_ORGANIZATION_ID
    domain = normalized.rsplit("@", 1)[1].strip()
    return domain or DEFAULT_ORGANIZATION_ID


def _purge_expired_entries(now: float | None = None) -> None:
    current = now if now is not None else time.time()
    expired_states = [
        key for key, value in _pending_states.items() if float(value["expires_at"]) <= current
    ]
    for key in expired_states:
        _pending_states.pop(key, None)

    expired_codes = [
        key for key, value in _authorization_codes.items() if float(value["expires_at"]) <= current
    ]
    for key in expired_codes:
        _authorization_codes.pop(key, None)


def create_oauth_state(*, return_to: str = "/dashboard") -> tuple[str, str]:
    """Create OAuth state and nonce values for CSRF protection."""
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    expires_at = time.time() + OAUTH_STATE_TTL_SEC
    with _oauth_lock:
        _purge_expired_entries(expires_at)
        _pending_states[state] = {
            "nonce": nonce,
            "return_to": return_to or "/dashboard",
            "expires_at": expires_at,
        }
    return state, nonce


def consume_oauth_state(state: str) -> dict[str, Any] | None:
    if not state:
        return None
    with _oauth_lock:
        _purge_expired_entries()
        entry = _pending_states.pop(state, None)
    if entry is None:
        return None
    if time.time() > float(entry["expires_at"]):
        return None
    return entry


def build_authorization_url(config: OAuth2Config, *, state: str, nonce: str) -> str:
    params = {
        "response_type": "code",
        "client_id": config.client_id,
        "redirect_uri": config.redirect_uri,
        "scope": config.scopes,
        "state": state,
        "nonce": nonce,
    }
    if config.simulated:
        params["prompt"] = "login"
    return f"{config.issuer}/authorize?{urllib.parse.urlencode(params)}"


def _http_post_form(url: str, payload: dict[str, str]) -> dict[str, Any]:
    body = urllib.parse.urlencode(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Token endpoint returned an invalid payload.")
    return parsed


def _http_get_json(url: str, access_token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Userinfo endpoint returned an invalid payload.")
    return parsed


def issue_simulated_authorization_code(
    *,
    email: str,
    name: str,
    redirect_uri: str,
    state: str,
    nonce: str,
) -> str:
    code = secrets.token_urlsafe(32)
    expires_at = time.time() + 120
    with _oauth_lock:
        _authorization_codes[code] = {
            "email": email.strip().lower(),
            "name": name.strip() or email.split("@", 1)[0],
            "redirect_uri": redirect_uri,
            "state": state,
            "nonce": nonce,
            "expires_at": expires_at,
        }
    return code


def exchange_authorization_code(
    config: OAuth2Config,
    *,
    code: str,
    state: str,
) -> dict[str, Any]:
    """Exchange an authorization code for OIDC profile claims."""
    if config.simulated:
        return _exchange_simulated_code(code, state, config.redirect_uri)

    token_url = f"{config.issuer.rstrip('/')}/token"
    token_payload = _http_post_form(
        token_url,
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": config.redirect_uri,
            "client_id": config.client_id,
            "client_secret": config.client_secret,
        },
    )
    access_token = str(token_payload.get("access_token", "")).strip()
    if not access_token:
        raise ValueError("Identity provider did not return an access token.")

    userinfo_url = f"{config.issuer.rstrip('/')}/userinfo"
    profile = _http_get_json(userinfo_url, access_token)
    return normalize_oidc_profile(profile)


def _exchange_simulated_code(code: str, state: str, redirect_uri: str) -> dict[str, Any]:
    with _oauth_lock:
        _purge_expired_entries()
        entry = _authorization_codes.pop(code, None)
    if entry is None:
        raise ValueError("Authorization code is invalid or expired.")
    if time.time() > float(entry["expires_at"]):
        raise ValueError("Authorization code expired.")
    if entry["state"] != state:
        raise ValueError("OAuth state mismatch.")
    if entry["redirect_uri"] != redirect_uri:
        raise ValueError("Redirect URI mismatch.")

    email = str(entry["email"])
    name = str(entry["name"])
    return {
        "email": email,
        "name": name,
        "organization_id": organization_id_from_email(email),
        "sub": email,
        "auth_provider": "simulated_oidc",
    }


def normalize_oidc_profile(profile: dict[str, Any]) -> dict[str, Any]:
    email = str(profile.get("email") or profile.get("preferred_username") or "").strip().lower()
    if not email:
        raise ValueError("Identity provider profile is missing an email address.")
    name = str(profile.get("name") or profile.get("given_name") or email.split("@", 1)[0]).strip()
    return {
        "email": email,
        "name": name,
        "organization_id": organization_id_from_email(email),
        "sub": str(profile.get("sub") or email),
        "auth_provider": "oidc",
    }


def render_simulated_login_page(
    *,
    authorize_query: str,
    error_message: str = "",
) -> str:
    params = urllib.parse.parse_qs(authorize_query.lstrip("?"))
    state = (params.get("state") or [""])[0]
    redirect_uri = (params.get("redirect_uri") or [""])[0]
    nonce = (params.get("nonce") or [""])[0]
    client_id = (params.get("client_id") or [""])[0]
    error_html = (
        f'<p class="error">{error_message}</p>' if error_message else ""
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>OmniKube Enterprise SSO</title>
  <style>
    body {{
      margin: 0; min-height: 100vh; display: grid; place-items: center;
      font-family: "Segoe UI", system-ui, sans-serif;
      background: radial-gradient(circle at top, #1e1b4b, #020617 55%);
      color: #e2e8f0;
    }}
    .card {{
      width: min(420px, 92vw); padding: 2rem; border-radius: 1rem;
      background: rgba(15, 23, 42, 0.88); border: 1px solid rgba(99, 102, 241, 0.35);
      box-shadow: 0 24px 80px rgba(15, 23, 42, 0.55);
    }}
    h1 {{ margin: 0 0 0.35rem; font-size: 1.35rem; }}
    p {{ margin: 0 0 1rem; color: #94a3b8; font-size: 0.95rem; }}
    label {{ display: block; font-size: 0.85rem; margin-bottom: 0.35rem; color: #cbd5e1; }}
    input {{
      width: 100%; box-sizing: border-box; margin-bottom: 1rem; padding: 0.7rem 0.85rem;
      border-radius: 0.65rem; border: 1px solid rgba(148, 163, 184, 0.35);
      background: rgba(2, 6, 23, 0.65); color: #f8fafc;
    }}
    button {{
      width: 100%; border: 0; border-radius: 0.65rem; padding: 0.75rem;
      background: linear-gradient(135deg, #6366f1, #4338ca); color: white; font-weight: 600;
      cursor: pointer;
    }}
    .hint {{ margin-top: 1rem; font-size: 0.78rem; color: #64748b; }}
    .error {{ color: #fecaca; margin-bottom: 0.75rem; }}
  </style>
</head>
<body>
  <form class="card" method="POST" action="/auth/sso/authorize">
    <h1>Enterprise Identity Provider</h1>
    <p>Simulated OIDC sign-in for OmniKube CloudMetrics.</p>
    {error_html}
    <input type="hidden" name="state" value="{state}" />
    <input type="hidden" name="redirect_uri" value="{redirect_uri}" />
    <input type="hidden" name="nonce" value="{nonce}" />
    <input type="hidden" name="client_id" value="{client_id}" />
    <label for="email">Work email</label>
    <input id="email" name="email" type="email" required placeholder="you@company.com" />
    <label for="name">Display name</label>
    <input id="name" name="name" type="text" placeholder="Alex Engineer" />
    <button type="submit">Continue with SSO</button>
    <p class="hint">Your organization is inferred from your email domain (e.g. user@company.com → company.com).</p>
  </form>
</body>
</html>"""
