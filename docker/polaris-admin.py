#!/usr/bin/env python3

# Copyright 2026 The OKDP Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Polaris catalog and RBAC bootstrap CLI.

This program applies a declarative Polaris contract from a YAML file.

Example:
    polaris-admin --catalog-file /etc/polaris/catalogs.yaml

Expected grant format:
    catalogRoles:
      - name: catalog_reader
        grants:
          - onCatalogs: [silver, gold]
            privileges:
              - CATALOG_READ_PROPERTIES

Required environment variables:
    POLARIS_URL
        Base URL of the Polaris service, for example:
        https://polaris.example.internal

Authentication model:
    This program authenticates against an external OIDC provider and then uses
    the resulting bearer token to call Polaris.

Per-realm authentication precedence:
    1. <REALM>_OIDC_TOKEN
    2. OIDC_TOKEN
    3. <REALM>_POLARIS_OIDC_CLIENT_ID + <REALM>_POLARIS_OIDC_CLIENT_SECRET
       with either:
         - <REALM>_OIDC_TOKEN_ENDPOINT
         - or <REALM>_OIDC_ISSUER_URL
    4. POLARIS_OIDC_CLIENT_ID + POLARIS_OIDC_CLIENT_SECRET
       with either:
         - OIDC_TOKEN_ENDPOINT
         - or OIDC_ISSUER_URL

Realm prefix derivation:
    <REALM> is derived from the current realm name in the catalog file by:
        - converting to uppercase
        - replacing every non-alphanumeric character with "_"
        - collapsing repeated "_"

Examples:
    Realm "SANDBOX" -> SANDBOX_POLARIS_OIDC_CLIENT_ID
    Realm "okdp-sandbox" -> OKDP_SANDBOX_POLARIS_OIDC_CLIENT_ID

Optional environment variables:
    OIDC_TOKEN
        Global bearer token from the OIDC provider
    <REALM>_OIDC_TOKEN
        Realm-specific bearer token from the OIDC provider

    OIDC_ISSUER_URL
        Global OIDC issuer URL used for discovery
    <REALM>_OIDC_ISSUER_URL
        Realm-specific OIDC issuer URL used for discovery

    OIDC_TOKEN_ENDPOINT
        Global explicit OIDC token endpoint
    <REALM>_OIDC_TOKEN_ENDPOINT
        Realm-specific explicit OIDC token endpoint

    POLARIS_OIDC_CLIENT_ID
        Global OIDC client ID
    POLARIS_OIDC_CLIENT_SECRET
        Global OIDC client secret
    <REALM>_POLARIS_OIDC_CLIENT_ID
        Realm-specific OIDC client ID
    <REALM>_POLARIS_OIDC_CLIENT_SECRET
        Realm-specific OIDC client secret

    OIDC_SCOPE
        Global OAuth scope for client_credentials
    <REALM>_OIDC_SCOPE
        Realm-specific OAuth scope

    OIDC_AUDIENCE
        Global audience parameter sent to the token endpoint when configured
    <REALM>_OIDC_AUDIENCE
        Realm-specific audience parameter

    OIDC_RESOURCE
        Global resource parameter sent to the token endpoint when configured
    <REALM>_OIDC_RESOURCE
        Realm-specific resource parameter

    OIDC_CLIENT_AUTH_METHOD
        Global client auth method for token requests
        Supported values:
            - client_secret_post
            - client_secret_basic
        Default: client_secret_post
    <REALM>_OIDC_CLIENT_AUTH_METHOD
        Realm-specific client auth method

TLS and transport:
    INSECURE_SKIP_VERIFY
        When set to "true", disables TLS certificate verification
    CA_CERT_PATH
        Path to a CA certificate bundle used for TLS verification

    HTTP_CONNECT_TIMEOUT
        Connect timeout in seconds
        Default: 10
    HTTP_READ_TIMEOUT
        Read timeout in seconds
        Default: 60
    HTTP_RETRIES
        Total retry count for transient HTTP failures
        Default: 5
    HTTP_BACKOFF_FACTOR
        Retry backoff factor
        Default: 1.0

Supported contract scope:
    - catalogs
    - catalogRoles with catalog-level grants using grants[].onCatalogs
    - principalRoles
    - principals

Current limitations:
    - only catalog-level grants are supported
    - namespaces, tables, views, and policies are not created by this version
"""

import argparse
import base64
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import requests
import yaml
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ENV_PATTERN = re.compile(r"\$(\w+)|\$\{([^}]+)\}")
REALM_ENV_PATTERN = re.compile(r"[^A-Za-z0-9]+")


class PolarisAdminError(Exception):
    """Raised when the Polaris contract or runtime configuration is invalid."""


@dataclass(frozen=True)
class AuthConfig:
    """Resolved OIDC authentication configuration for one realm."""

    direct_token: str = ""
    issuer_url: str = ""
    token_endpoint: str = ""
    client_id: str = ""
    client_secret: str = ""
    scope: str = ""
    audience: str = ""
    resource: str = ""
    client_auth_method: str = "client_secret_post"

    def has_direct_token(self) -> bool:
        """Return True when a direct bearer token is available."""
        return bool(self.direct_token)

    def can_mint(self) -> bool:
        """Return True when client credentials are sufficient to mint a token."""
        return bool(
            self.client_id
            and self.client_secret
            and (self.token_endpoint or self.issuer_url)
        )


@dataclass
class TokenCacheEntry:
    """In-memory token cache entry."""

    access_token: str
    expires_at: Optional[float]
    source: str  # "direct" or "minted"


class PolarisAdmin:
    """Apply Polaris catalogs and RBAC objects from a declarative contract."""

    TOKEN_EXPIRY_SKEW_SECONDS = 60.0

    def __init__(self) -> None:
        """Initialize runtime configuration, HTTP sessions, and in-memory caches."""
        self.polaris_url = self._require_env("POLARIS_URL")
        self.base_url = self.polaris_url.rstrip("/")

        self.insecure_skip_verify = self._bool_env("INSECURE_SKIP_VERIFY", False)
        self.ca_cert_path = os.getenv("CA_CERT_PATH", "").strip()

        self.connect_timeout = self._float_env("HTTP_CONNECT_TIMEOUT", 10.0)
        self.read_timeout = self._float_env("HTTP_READ_TIMEOUT", 60.0)
        self.http_retries = self._int_env("HTTP_RETRIES", 5)
        self.http_backoff_factor = self._float_env("HTTP_BACKOFF_FACTOR", 1.0)

        if self.http_retries < 0:
            raise PolarisAdminError(
                f"HTTP_RETRIES must be >= 0, got: {self.http_retries}"
            )
        if self.http_backoff_factor < 0:
            raise PolarisAdminError(
                f"HTTP_BACKOFF_FACTOR must be >= 0, got: {self.http_backoff_factor}"
            )

        self.verify = False if self.insecure_skip_verify else (self.ca_cert_path or True)

        self.session = self._build_session("polaris-admin/2.0")
        self.oidc_session = self._build_session("polaris-admin/2.0 oidc")

        self.polaris_realm = ""
        self.realm_token_cache: Dict[str, TokenCacheEntry] = {}
        self.oidc_discovery_cache: Dict[str, Dict[str, Any]] = {}
        self.auth_config_cache: Dict[str, AuthConfig] = {}
        self.auth_config_logged_realms: Set[str] = set()

        self._clear_state_caches()

        if self.insecure_skip_verify:
            try:
                import urllib3

                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass

        logging.info(
            "[STARTUP] PolarisAdmin initialized | polaris_url=%s tls_verify=%s "
            "ca_cert_path=%s connect_timeout=%.1fs read_timeout=%.1fs "
            "http_retries=%d http_backoff_factor=%.1f",
            self.base_url,
            not self.insecure_skip_verify,
            self.ca_cert_path or "<system-default>",
            self.connect_timeout,
            self.read_timeout,
            self.http_retries,
            self.http_backoff_factor,
            )

    def _clear_state_caches(self) -> None:
        """Clear realm-scoped state caches used for idempotence checks."""
        self.catalog_exists_cache: Dict[str, bool] = {}
        self.principal_exists_cache: Dict[str, bool] = {}
        self.principal_role_exists_cache: Dict[str, bool] = {}
        self.catalog_role_exists_cache: Dict[Tuple[str, str], bool] = {}
        self.principal_role_assignments_cache: Dict[str, Set[str]] = {}
        self.catalog_role_bindings_cache: Dict[Tuple[str, str], Set[str]] = {}
        self.catalog_role_grants_cache: Dict[Tuple[str, str], Set[str]] = {}

    @staticmethod
    def _require_env(name: str) -> str:
        """Return the value of a required environment variable or raise an error."""
        value = os.getenv(name, "").strip()
        if not value:
            raise PolarisAdminError(f"{name} is required")
        return value

    @staticmethod
    def _bool_env(name: str, default: bool) -> bool:
        """Read a boolean environment variable with a default value."""
        raw = os.getenv(name)
        if raw is None or raw == "":
            return default
        value = raw.strip().lower()
        if value in {"1", "true", "yes", "y", "on"}:
            return True
        if value in {"0", "false", "no", "n", "off"}:
            return False
        raise PolarisAdminError(f"{name} must be a boolean value, got: {raw}")

    @staticmethod
    def _int_env(name: str, default: int) -> int:
        """Read an integer environment variable with a default value."""
        raw = os.getenv(name)
        if raw is None or raw == "":
            return default
        try:
            return int(raw)
        except ValueError as exc:
            raise PolarisAdminError(f"{name} must be an integer, got: {raw}") from exc

    @staticmethod
    def _float_env(name: str, default: float) -> float:
        """Read a floating-point environment variable with a default value."""
        raw = os.getenv(name)
        if raw is None or raw == "":
            return default
        try:
            value = float(raw)
        except ValueError as exc:
            raise PolarisAdminError(f"{name} must be a number, got: {raw}") from exc
        if value <= 0:
            raise PolarisAdminError(f"{name} must be > 0, got: {value}")
        return value

    def _build_session(self, user_agent: str) -> Session:
        """Build a requests session with retries and standard headers."""
        retry = Retry(
            total=self.http_retries,
            connect=self.http_retries,
            read=self.http_retries,
            status=self.http_retries,
            backoff_factor=self.http_backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(
                {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
            ),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }
        )
        return session

    @staticmethod
    def normalize_realm_env_prefix(realm_name: str) -> str:
        """Convert a realm name into the normalized environment-variable prefix."""
        normalized = REALM_ENV_PATTERN.sub("_", realm_name.strip()).strip("_").upper()
        normalized = re.sub(r"_+", "_", normalized)
        if not normalized:
            raise PolarisAdminError(
                f"cannot derive environment-variable prefix from realm '{realm_name}'"
            )
        return normalized

    @staticmethod
    def _first_non_empty(*names: str) -> str:
        """Return the first non-empty environment variable value from the given names."""
        for name in names:
            value = os.getenv(name, "").strip()
            if value:
                return value
        return ""

    def _realm_env_candidates(self, realm_name: str, suffix: str) -> List[str]:
        """Return the environment-variable names for a realm-specific setting."""
        prefix = self.normalize_realm_env_prefix(realm_name)
        return [f"{prefix}_{suffix}"]

    @staticmethod
    def _truncate(value: str, max_len: int = 2000) -> str:
        """Return a single-line truncated representation of a string."""
        cleaned = value.strip().replace("\n", " ")
        if len(cleaned) <= max_len:
            return cleaned
        return cleaned[: max_len - 3] + "..."

    @staticmethod
    def _mask_secret(value: str, keep_start: int = 4, keep_end: int = 2) -> str:
        """Mask a sensitive string while keeping a short prefix and suffix visible."""
        if not value:
            return ""
        if len(value) <= keep_start + keep_end:
            return "*" * len(value)
        return (
            value[:keep_start]
            + "*" * (len(value) - keep_start - keep_end)
            + value[-keep_end:]
        )

    def _sanitize_for_log(self, value: Any) -> Any:
        """Recursively sanitize secrets and credentials before logging."""
        if isinstance(value, dict):
            sanitized: Dict[str, Any] = {}
            for key, item in value.items():
                key_lower = str(key).lower()
                if any(
                    token in key_lower
                    for token in (
                        "secret",
                        "token",
                        "authorization",
                        "password",
                        "credential",
                    )
                ):
                    if isinstance(item, str):
                        sanitized[key] = self._mask_secret(item)
                    else:
                        sanitized[key] = "***"
                else:
                    sanitized[key] = self._sanitize_for_log(item)
            return sanitized
        if isinstance(value, list):
            return [self._sanitize_for_log(item) for item in value]
        if isinstance(value, str):
            return self._truncate(value, max_len=1000)
        return value

    def _json_for_log(self, value: Any) -> str:
        """Serialize a value to sanitized JSON for logging."""
        return json.dumps(
            self._sanitize_for_log(value),
            ensure_ascii=False,
            sort_keys=True,
        )

    def _response_body_for_log(self, response: Response) -> str:
        """Return a sanitized response body representation suitable for logs."""
        if not response.text:
            return "<empty>"

        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            try:
                body = response.json()
            except ValueError:
                return self._truncate(response.text)
            return self._json_for_log(body)

        return self._truncate(response.text)

    def _format_response_error(self, response: Response) -> str:
        """Extract the most useful error message from an HTTP response."""
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            try:
                body = response.json()
            except ValueError:
                text = self._truncate(response.text)
                return text or response.reason or f"HTTP {response.status_code}"

            if isinstance(body, dict):
                error_obj = body.get("error")
                if isinstance(error_obj, dict):
                    for key in ("message", "detail", "error_description", "title", "type"):
                        value = error_obj.get(key)
                        if isinstance(value, str) and value.strip():
                            return self._truncate(value)

                for key in ("error_description", "message", "error", "detail", "title"):
                    value = body.get(key)
                    if isinstance(value, str) and value.strip():
                        return self._truncate(value)

                text = self._truncate(json.dumps(body, sort_keys=True))
                return text or response.reason or f"HTTP {response.status_code}"

            text = self._truncate(str(body))
            return text or response.reason or f"HTTP {response.status_code}"

        text = self._truncate(response.text)
        return text or response.reason or f"HTTP {response.status_code}"

    @staticmethod
    def _quote_path_segment(value: str) -> str:
        """URL-encode a path segment for safe inclusion in management API paths."""
        return quote(value, safe="")

    def set_realm(self, realm_name: str) -> None:
        """Select the active Polaris realm for subsequent management requests."""
        self.polaris_realm = realm_name
        self.session.headers["Polaris-Realm"] = realm_name
        self.session.headers.pop("Authorization", None)
        self._clear_state_caches()
        logging.info("[REALM] selected realm=%s", realm_name)

    def load_catalog_file(self, path: str) -> Dict[str, Any]:
        """Load, parse, and environment-expand the declarative contract file."""
        if not os.path.isfile(path):
            raise PolarisAdminError(f"catalog file does not exist: {path}")
        if not os.access(path, os.R_OK):
            raise PolarisAdminError(f"catalog file is not readable: {path}")

        logging.info("[INPUT] loading catalog file path=%s", path)
        with open(path, "r", encoding="utf-8") as file_handle:
            data = yaml.safe_load(file_handle) or {}

        expanded = self._expand_env_values(data)
        logging.info("[INPUT] catalog contract loaded successfully")
        logging.debug("[INPUT] expanded contract=%s", self._json_for_log(expanded))
        return expanded

    def _expand_env_values(self, value: Any) -> Any:
        """Recursively expand ${VAR} and $VAR references in the contract."""
        if isinstance(value, dict):
            return {key: self._expand_env_values(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._expand_env_values(item) for item in value]
        if isinstance(value, str):
            return self._expand_env_string(value)
        return value

    def _expand_env_string(self, value: str) -> str:
        """Expand environment-variable placeholders in a single string."""
        if "$" not in value:
            return value

        def replace(match: re.Match[str]) -> str:
            var_name = match.group(1) or match.group(2)
            if var_name not in os.environ:
                raise PolarisAdminError(
                    f"environment variable '{var_name}' is referenced in catalogs.yaml but not set"
                )
            return os.environ[var_name]

        return ENV_PATTERN.sub(replace, value)

    def select_realms(
        self,
        contract: Dict[str, Any],
        realm_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Select the realms to apply from the contract, preserving file order."""
        realms = contract.get("realms", [])
        if not isinstance(realms, list) or not realms:
            raise PolarisAdminError(
                "catalog file must contain a non-empty top-level 'realms' list"
            )

        names_seen: Set[str] = set()
        selected: List[Dict[str, Any]] = []

        for index, realm in enumerate(realms):
            if not isinstance(realm, dict):
                raise PolarisAdminError(f"realm entry at index {index} must be a mapping")
            name = realm.get("name", "")
            if not name:
                raise PolarisAdminError(
                    f"realm entry at index {index} is missing realm.name"
                )
            if name in names_seen:
                raise PolarisAdminError(
                    f"duplicate realm name found in catalog file: {name}"
                )
            names_seen.add(name)

            if realm_filter is None or name == realm_filter:
                selected.append(realm)

        if realm_filter and not selected:
            raise PolarisAdminError(f"realm '{realm_filter}' not found in catalog file")

        logging.info(
            "[INPUT] realm selection completed | requested_realm=%s selected_realms=%s",
            realm_filter or "<all>",
            [realm.get("name") for realm in selected],
            )
        return selected

    def _resolve_auth_for_realm(self, realm_name: str) -> AuthConfig:
        """Resolve the effective authentication configuration for a realm."""
        cached = self.auth_config_cache.get(realm_name)
        if cached is not None:
            return cached

        token = self._first_non_empty(
            *self._realm_env_candidates(realm_name, "OIDC_TOKEN"),
            "OIDC_TOKEN",
        )
        issuer_url = self._first_non_empty(
            *self._realm_env_candidates(realm_name, "OIDC_ISSUER_URL"),
            "OIDC_ISSUER_URL",
        )
        token_endpoint = self._first_non_empty(
            *self._realm_env_candidates(realm_name, "OIDC_TOKEN_ENDPOINT"),
            "OIDC_TOKEN_ENDPOINT",
        )
        client_id = self._first_non_empty(
            *self._realm_env_candidates(realm_name, "POLARIS_OIDC_CLIENT_ID"),
            "POLARIS_OIDC_CLIENT_ID",
        )
        client_secret = self._first_non_empty(
            *self._realm_env_candidates(realm_name, "POLARIS_OIDC_CLIENT_SECRET"),
            "POLARIS_OIDC_CLIENT_SECRET",
        )
        scope = self._first_non_empty(
            *self._realm_env_candidates(realm_name, "OIDC_SCOPE"),
            "OIDC_SCOPE",
        )
        audience = self._first_non_empty(
            *self._realm_env_candidates(realm_name, "OIDC_AUDIENCE"),
            "OIDC_AUDIENCE",
        )
        resource = self._first_non_empty(
            *self._realm_env_candidates(realm_name, "OIDC_RESOURCE"),
            "OIDC_RESOURCE",
        )
        client_auth_method = self._first_non_empty(
            *self._realm_env_candidates(realm_name, "OIDC_CLIENT_AUTH_METHOD"),
            "OIDC_CLIENT_AUTH_METHOD",
        ) or "client_secret_post"

        if (client_id and not client_secret) or (client_secret and not client_id):
            raise PolarisAdminError(
                f"incomplete OIDC client credentials for realm '{realm_name}': "
                f"both client_id and client_secret are required"
            )

        if client_id and client_secret and not (issuer_url or token_endpoint):
            prefix = self.normalize_realm_env_prefix(realm_name)
            raise PolarisAdminError(
                f"missing OIDC endpoint configuration for realm '{realm_name}'. "
                f"Expected either {prefix}_OIDC_ISSUER_URL or {prefix}_OIDC_TOKEN_ENDPOINT "
                f"(with global OIDC_ISSUER_URL / OIDC_TOKEN_ENDPOINT as fallback)."
            )

        if client_auth_method not in {"client_secret_post", "client_secret_basic"}:
            raise PolarisAdminError(
                f"unsupported OIDC_CLIENT_AUTH_METHOD for realm '{realm_name}': "
                f"{client_auth_method}"
            )

        auth_cfg = AuthConfig(
            direct_token=token,
            issuer_url=issuer_url,
            token_endpoint=token_endpoint,
            client_id=client_id,
            client_secret=client_secret,
            scope=scope,
            audience=audience,
            resource=resource,
            client_auth_method=client_auth_method,
        )

        if not auth_cfg.has_direct_token() and not auth_cfg.can_mint():
            prefix = self.normalize_realm_env_prefix(realm_name)
            raise PolarisAdminError(
                f"missing authentication for realm '{realm_name}'. "
                f"Expected either {prefix}_OIDC_TOKEN / OIDC_TOKEN, "
                f"or OIDC client credentials via "
                f"{prefix}_POLARIS_OIDC_CLIENT_ID/{prefix}_POLARIS_OIDC_CLIENT_SECRET "
                f"(or global POLARIS_OIDC_CLIENT_ID/POLARIS_OIDC_CLIENT_SECRET) plus issuer or token endpoint."
            )

        self.auth_config_cache[realm_name] = auth_cfg

        if realm_name not in self.auth_config_logged_realms:
            logging.info(
                "[AUTH] resolved auth inputs for realm=%s | direct_token_present=%s "
                "issuer_url=%s token_endpoint=%s client_id=%s client_secret_present=%s "
                "scope=%s audience=%s resource=%s auth_method=%s",
                realm_name,
                auth_cfg.has_direct_token(),
                auth_cfg.issuer_url or "<empty>",
                auth_cfg.token_endpoint or "<empty>",
                auth_cfg.client_id or "<empty>",
                bool(auth_cfg.client_secret),
                auth_cfg.scope or "<empty>",
                auth_cfg.audience or "<empty>",
                auth_cfg.resource or "<empty>",
                auth_cfg.client_auth_method,
                )
            self.auth_config_logged_realms.add(realm_name)

        return auth_cfg

    @staticmethod
    def _jwt_expiry(token: str) -> Optional[float]:
        """Extract the exp claim from a JWT if available."""
        parts = token.split(".")
        if len(parts) < 2:
            return None

        payload_b64 = parts[1]
        padding = "=" * (-len(payload_b64) % 4)
        try:
            payload_raw = base64.urlsafe_b64decode(payload_b64 + padding)
            payload = json.loads(payload_raw.decode("utf-8"))
        except Exception:
            return None

        exp = payload.get("exp")
        if isinstance(exp, (int, float)):
            return float(exp)
        return None

    def _is_token_valid(self, entry: TokenCacheEntry) -> bool:
        """Return True when a cached token is still valid beyond the skew window."""
        if entry.expires_at is None:
            return True
        return (time.time() + self.TOKEN_EXPIRY_SKEW_SECONDS) < entry.expires_at

    def _discover_oidc_metadata(self, issuer_url: str) -> Dict[str, Any]:
        """Fetch and cache the OIDC discovery document for an issuer."""
        cache_key = issuer_url.rstrip("/")
        if cache_key in self.oidc_discovery_cache:
            logging.debug(
                "[OIDC] using cached discovery metadata for issuer=%s",
                issuer_url,
            )
            return self.oidc_discovery_cache[cache_key]

        if cache_key.endswith("/.well-known/openid-configuration"):
            discovery_url = cache_key
        else:
            discovery_url = f"{cache_key}/.well-known/openid-configuration"

        logging.info(
            "[OIDC] discovering metadata | issuer_url=%s discovery_url=%s",
            issuer_url,
            discovery_url,
        )
        response = self.oidc_session.get(
            discovery_url,
            verify=self.verify,
            timeout=(self.connect_timeout, self.read_timeout),
        )

        if response.status_code != 200:
            raise PolarisAdminError(
                f"[OIDC] discovery failed for issuer {issuer_url} "
                f"http={response.status_code} detail={self._format_response_error(response)}"
            )

        try:
            metadata = response.json()
        except ValueError as exc:
            raise PolarisAdminError(
                f"[OIDC] discovery document is not valid JSON for issuer {issuer_url}"
            ) from exc

        if not isinstance(metadata, dict):
            raise PolarisAdminError(
                f"[OIDC] discovery document is not a JSON object for issuer {issuer_url}"
            )

        token_endpoint = metadata.get("token_endpoint", "")
        if not token_endpoint:
            raise PolarisAdminError(
                f"[OIDC] token_endpoint not found in discovery document for issuer {issuer_url}"
            )

        self.oidc_discovery_cache[cache_key] = metadata
        logging.info(
            "[OIDC] discovery successful | issuer_url=%s token_endpoint=%s",
            issuer_url,
            token_endpoint,
        )
        return metadata

    def _mint_oidc_token(
        self,
        realm_name: str,
        auth_cfg: AuthConfig,
    ) -> TokenCacheEntry:
        """Request a new access token from the configured OIDC provider."""
        token_endpoint = auth_cfg.token_endpoint
        if not token_endpoint:
            metadata = self._discover_oidc_metadata(auth_cfg.issuer_url)
            token_endpoint = str(metadata["token_endpoint"]).rstrip("/")

        form_data: Dict[str, str] = {"grant_type": "client_credentials"}
        if auth_cfg.scope:
            form_data["scope"] = auth_cfg.scope
        if auth_cfg.audience:
            form_data["audience"] = auth_cfg.audience
        if auth_cfg.resource:
            form_data["resource"] = auth_cfg.resource

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        auth: Optional[Tuple[str, str]] = None
        if auth_cfg.client_auth_method == "client_secret_basic":
            auth = (auth_cfg.client_id, auth_cfg.client_secret)
        else:
            form_data["client_id"] = auth_cfg.client_id
            form_data["client_secret"] = auth_cfg.client_secret

        logging.info(
            "[OIDC] requesting token | realm=%s token_endpoint=%s auth_method=%s form=%s",
            realm_name,
            token_endpoint,
            auth_cfg.client_auth_method,
            self._json_for_log(form_data),
        )

        response = self.oidc_session.post(
            token_endpoint,
            headers=headers,
            data=form_data,
            auth=auth,
            verify=self.verify,
            timeout=(self.connect_timeout, self.read_timeout),
        )

        if response.status_code != 200:
            raise PolarisAdminError(
                f"[OIDC] token request failed for realm {realm_name} "
                f"http={response.status_code} detail={self._format_response_error(response)}"
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise PolarisAdminError(
                f"[OIDC] token endpoint returned invalid JSON for realm {realm_name}"
            ) from exc

        if not isinstance(body, dict):
            raise PolarisAdminError(
                f"[OIDC] token response is not a JSON object for realm {realm_name}"
            )

        access_token = body.get("access_token", "")
        if not access_token:
            raise PolarisAdminError(
                f"[OIDC] access_token not found in token response for realm {realm_name}"
            )

        expires_at: Optional[float] = None
        expires_in = body.get("expires_in")
        if isinstance(expires_in, (int, float)) and float(expires_in) > 0:
            expires_at = time.time() + float(expires_in)
        else:
            expires_at = self._jwt_expiry(access_token)

        logging.info(
            "[OIDC] token acquired | realm=%s expires_at=%s token_source=minted",
            realm_name,
            time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(expires_at))
            if expires_at
            else "<unknown>",
        )

        return TokenCacheEntry(
            access_token=access_token,
            expires_at=expires_at,
            source="minted",
        )

    def ensure_token(self, force_refresh: bool = False) -> None:
        """Ensure the outbound Polaris session has a valid Authorization header."""
        if not self.polaris_realm:
            raise PolarisAdminError("realm is not selected")

        auth_cfg = self._resolve_auth_for_realm(self.polaris_realm)

        if not force_refresh:
            cached = self.realm_token_cache.get(self.polaris_realm)
            if cached and self._is_token_valid(cached):
                self.session.headers["Authorization"] = f"Bearer {cached.access_token}"
                logging.info(
                    "[AUTH] using cached token for realm=%s source=%s",
                    self.polaris_realm,
                    cached.source,
                )
                return

        if auth_cfg.has_direct_token() and not force_refresh:
            direct_exp = self._jwt_expiry(auth_cfg.direct_token)
            if direct_exp is None or (
                time.time() + self.TOKEN_EXPIRY_SKEW_SECONDS
            ) < direct_exp:
                self.session.headers["Authorization"] = (
                    f"Bearer {auth_cfg.direct_token}"
                )
                logging.info(
                    "[AUTH] using direct token for realm=%s",
                    self.polaris_realm,
                )
                return

            if not auth_cfg.can_mint():
                raise PolarisAdminError(
                    f"[OIDC] provided token for realm {self.polaris_realm} appears expired "
                    f"and no client credentials are available to refresh it"
                )

        if auth_cfg.can_mint():
            entry = self._mint_oidc_token(self.polaris_realm, auth_cfg)
            self.realm_token_cache[self.polaris_realm] = entry
            self.session.headers["Authorization"] = f"Bearer {entry.access_token}"
            return

        if auth_cfg.has_direct_token():
            self.session.headers["Authorization"] = f"Bearer {auth_cfg.direct_token}"
            logging.info(
                "[AUTH] using direct token for realm=%s after fallback",
                self.polaris_realm,
            )
            return

        raise PolarisAdminError(
            f"[OIDC] unable to resolve usable authentication for realm {self.polaris_realm}"
        )

    def request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
    ) -> Response:
        """Send an authenticated management API request and retry once on 401."""
        if not self.polaris_realm:
            raise PolarisAdminError("realm is not selected")

        self.ensure_token()

        url = f"{self.base_url}{path}"
        headers: Dict[str, str] = {}
        if body is not None:
            headers["Content-Type"] = "application/json"

        request_id = uuid.uuid4().hex[:12]
        log_headers = {
            "Accept": "application/json",
            "Content-Type": headers.get("Content-Type", "<none>"),
            "Polaris-Realm": self.polaris_realm,
            "Authorization": "<redacted>",
        }

        logging.info(
            "[HTTP:%s] request | method=%s url=%s headers=%s body=%s",
            request_id,
            method,
            url,
            self._json_for_log(log_headers),
            self._json_for_log(body) if body is not None else "<empty>",
        )

        started = time.time()
        response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            json=body,
            verify=self.verify,
            timeout=(self.connect_timeout, self.read_timeout),
        )
        elapsed_ms = int((time.time() - started) * 1000)

        logging.info(
            "[HTTP:%s] response | status=%s elapsed_ms=%d reason=%s body=%s",
            request_id,
            response.status_code,
            elapsed_ms,
            response.reason,
            self._response_body_for_log(response),
        )

        if response.status_code != 401:
            return response

        auth_cfg = self._resolve_auth_for_realm(self.polaris_realm)
        if not auth_cfg.can_mint():
            return response

        logging.warning(
            "[AUTH] received 401 from Polaris for realm=%s request_id=%s; refreshing token once",
            self.polaris_realm,
            request_id,
        )
        self.realm_token_cache.pop(self.polaris_realm, None)
        self.ensure_token(force_refresh=True)

        started = time.time()
        retry_response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            json=body,
            verify=self.verify,
            timeout=(self.connect_timeout, self.read_timeout),
        )
        elapsed_ms = int((time.time() - started) * 1000)

        logging.info(
            "[HTTP:%s] retry-response | status=%s elapsed_ms=%d reason=%s body=%s",
            request_id,
            retry_response.status_code,
            elapsed_ms,
            retry_response.reason,
            self._response_body_for_log(retry_response),
        )
        return retry_response

    def _get_json(self, path: str) -> Dict[str, Any]:
        """Issue a GET request and return the decoded JSON body."""
        response = self.request("GET", path)
        if response.status_code != 200:
            raise PolarisAdminError(
                f"[GET] failed path={path} http={response.status_code} "
                f"detail={self._format_response_error(response)}"
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise PolarisAdminError(
                f"[GET] invalid JSON response for path={path}"
            ) from exc
        if not isinstance(body, dict):
            raise PolarisAdminError(
                f"[GET] unexpected non-object JSON response for path={path}"
            )
        return body

    def _get_if_exists(self, path: str) -> Optional[Dict[str, Any]]:
        """Return the decoded JSON body, or None when the resource does not exist."""
        response = self.request("GET", path)
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise PolarisAdminError(
                f"[GET] failed path={path} http={response.status_code} "
                f"detail={self._format_response_error(response)}"
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise PolarisAdminError(
                f"[GET] invalid JSON response for path={path}"
            ) from exc
        if not isinstance(body, dict):
            raise PolarisAdminError(
                f"[GET] unexpected non-object JSON response for path={path}"
            )
        return body

    def _is_duplicate_assignment_error(self, response: Response) -> bool:
        """Return True when the response indicates an already-existing relationship."""
        detail = self._format_response_error(response).lower()
        body_for_log = self._response_body_for_log(response).lower()
        haystack = f"{detail} {body_for_log}"
        return any(
            marker in haystack
            for marker in (
                "duplicate key value violates unique constraint",
                "already exists",
                "grant_records_pkey",
            )
        )

    @staticmethod
    def _extract_named_entities(
        body: Dict[str, Any],
        collection_key: str,
    ) -> Set[str]:
        """Extract entity names from a list response body."""
        results: Set[str] = set()
        items = body.get(collection_key, [])
        if not isinstance(items, list):
            return results

        for item in items:
            if isinstance(item, dict):
                name = item.get("name")
                if isinstance(name, str) and name.strip():
                    results.add(name.strip())
        return results

    def _extract_privileges(self, value: Any) -> Set[str]:
        """Recursively collect privilege values from a grants response body."""
        privileges: Set[str] = set()

        if isinstance(value, dict):
            privilege = value.get("privilege")
            if isinstance(privilege, str) and privilege.strip():
                privileges.add(privilege.strip())
            for item in value.values():
                privileges.update(self._extract_privileges(item))
        elif isinstance(value, list):
            for item in value:
                privileges.update(self._extract_privileges(item))

        return privileges

    def _catalog_exists(self, catalog_name: str) -> bool:
        """Return True when the catalog already exists."""
        cached = self.catalog_exists_cache.get(catalog_name)
        if cached is not None:
            return cached

        path = f"/api/management/v1/catalogs/{self._quote_path_segment(catalog_name)}"
        exists = self._get_if_exists(path) is not None
        self.catalog_exists_cache[catalog_name] = exists
        return exists

    def _principal_exists(self, principal_name: str) -> bool:
        """Return True when the principal already exists."""
        cached = self.principal_exists_cache.get(principal_name)
        if cached is not None:
            return cached

        path = f"/api/management/v1/principals/{self._quote_path_segment(principal_name)}"
        exists = self._get_if_exists(path) is not None
        self.principal_exists_cache[principal_name] = exists
        return exists

    def _principal_role_exists(self, principal_role_name: str) -> bool:
        """Return True when the principal role already exists."""
        cached = self.principal_role_exists_cache.get(principal_role_name)
        if cached is not None:
            return cached

        path = (
            "/api/management/v1/principal-roles/"
            f"{self._quote_path_segment(principal_role_name)}"
        )
        exists = self._get_if_exists(path) is not None
        self.principal_role_exists_cache[principal_role_name] = exists
        return exists

    def _catalog_role_exists(self, catalog_name: str, catalog_role_name: str) -> bool:
        """Return True when the catalog role already exists in the catalog."""
        cache_key = (catalog_name, catalog_role_name)
        cached = self.catalog_role_exists_cache.get(cache_key)
        if cached is not None:
            return cached

        path = (
            "/api/management/v1/catalogs/"
            f"{self._quote_path_segment(catalog_name)}"
            "/catalog-roles/"
            f"{self._quote_path_segment(catalog_role_name)}"
        )
        exists = self._get_if_exists(path) is not None
        self.catalog_role_exists_cache[cache_key] = exists
        return exists

    def _principal_has_role(self, principal_name: str, principal_role_name: str) -> bool:
        """Return True when the principal already has the target principal role."""
        cached = self.principal_role_assignments_cache.get(principal_name)
        if cached is None:
            path = (
                "/api/management/v1/principals/"
                f"{self._quote_path_segment(principal_name)}"
                "/principal-roles"
            )
            body = self._get_json(path)
            cached = self._extract_named_entities(body, "roles")
            self.principal_role_assignments_cache[principal_name] = cached

        return principal_role_name in cached

    def _principal_role_has_catalog_role(
        self,
        principal_role_name: str,
        catalog_name: str,
        catalog_role_name: str,
    ) -> bool:
        """Return True when the principal role already has the target catalog role."""
        cache_key = (principal_role_name, catalog_name)
        cached = self.catalog_role_bindings_cache.get(cache_key)
        if cached is None:
            path = (
                "/api/management/v1/principal-roles/"
                f"{self._quote_path_segment(principal_role_name)}"
                "/catalog-roles/"
                f"{self._quote_path_segment(catalog_name)}"
            )
            body = self._get_json(path)
            cached = self._extract_named_entities(body, "roles")
            self.catalog_role_bindings_cache[cache_key] = cached

        return catalog_role_name in cached

    def _catalog_role_has_privilege(
        self,
        catalog_name: str,
        catalog_role_name: str,
        privilege: str,
    ) -> bool:
        """Return True when the catalog role already has the target privilege."""
        cache_key = (catalog_name, catalog_role_name)
        cached = self.catalog_role_grants_cache.get(cache_key)
        if cached is None:
            path = (
                "/api/management/v1/catalogs/"
                f"{self._quote_path_segment(catalog_name)}"
                "/catalog-roles/"
                f"{self._quote_path_segment(catalog_role_name)}"
                "/grants"
            )
            body = self._get_json(path)
            cached = self._extract_privileges(body)
            self.catalog_role_grants_cache[cache_key] = cached

        return privilege in cached

    def ensure_catalog(self, catalog: Dict[str, Any]) -> None:
        """Ensure that the target catalog exists."""
        name = catalog.get("name", "")
        catalog_type = catalog.get("type", "INTERNAL")
        properties = catalog.get("properties", {})
        storage_config = catalog.get("storageConfigInfo")

        if not name:
            raise PolarisAdminError("catalog.name is required")
        if not isinstance(properties, dict):
            raise PolarisAdminError(
                f"catalog.properties must be a mapping for catalog '{name}'"
            )
        if "default-base-location" not in properties or not properties["default-base-location"]:
            raise PolarisAdminError(
                f"catalog.properties.default-base-location is required for catalog '{name}'"
            )
        if not isinstance(storage_config, dict) or not storage_config:
            raise PolarisAdminError(
                f"catalog.storageConfigInfo is required for catalog '{name}'"
            )

        payload = {
            "catalog": {
                "name": name,
                "type": catalog_type,
                "properties": properties,
                "storageConfigInfo": storage_config,
            }
        }

        logging.info(
            "[CATALOG] ensure start | name=%s payload=%s",
            name,
            self._json_for_log(payload),
        )

        if self._catalog_exists(name):
            logging.info("[CATALOG] already exists %s", name)
            return

        response = self.request("POST", "/api/management/v1/catalogs", payload)
        if response.status_code == 201:
            self.catalog_exists_cache[name] = True
            logging.info("[CATALOG] created %s", name)
            return
        if response.status_code == 409:
            self.catalog_exists_cache[name] = True
            logging.info("[CATALOG] already exists %s", name)
            return

        raise PolarisAdminError(
            f"[CATALOG] failed name={name} http={response.status_code} "
            f"detail={self._format_response_error(response)} "
            f"payload={self._json_for_log(payload)}"
        )

    def ensure_principal_role(self, principal_role: Dict[str, Any]) -> None:
        """Ensure that the target principal role exists."""
        name = principal_role.get("name", "")
        if not name:
            raise PolarisAdminError("principalRole.name is required")

        payload: Dict[str, Any] = {"principalRole": {"name": name}}
        if "federated" in principal_role:
            payload["principalRole"]["federated"] = bool(principal_role["federated"])

        logging.info(
            "[PRINCIPAL_ROLE] ensure start | name=%s payload=%s",
            name,
            self._json_for_log(payload),
        )

        if self._principal_role_exists(name):
            logging.info("[PRINCIPAL_ROLE] already exists %s", name)
            return

        response = self.request("POST", "/api/management/v1/principal-roles", payload)
        if response.status_code == 201:
            self.principal_role_exists_cache[name] = True
            logging.info("[PRINCIPAL_ROLE] created %s", name)
            return
        if response.status_code == 409:
            self.principal_role_exists_cache[name] = True
            logging.info("[PRINCIPAL_ROLE] already exists %s", name)
            return

        raise PolarisAdminError(
            f"[PRINCIPAL_ROLE] failed name={name} http={response.status_code} "
            f"detail={self._format_response_error(response)} "
            f"payload={self._json_for_log(payload)}"
        )

    def ensure_principal(self, principal: Dict[str, Any]) -> None:
        """Ensure that the target principal exists."""
        name = principal.get("name", "")
        if not name:
            raise PolarisAdminError("principal.name is required")

        properties = principal.get("properties", {})
        if properties is None:
            properties = {}
        if not isinstance(properties, dict):
            raise PolarisAdminError(
                f"principal.properties must be a mapping for principal '{name}'"
            )

        payload = {"principal": {"name": name, "properties": properties}}

        logging.info(
            "[PRINCIPAL] ensure start | name=%s payload=%s",
            name,
            self._json_for_log(payload),
        )

        if self._principal_exists(name):
            logging.info("[PRINCIPAL] already exists %s", name)
            return

        response = self.request("POST", "/api/management/v1/principals", payload)
        if response.status_code == 201:
            self.principal_exists_cache[name] = True
            logging.info("[PRINCIPAL] created %s", name)
            return
        if response.status_code == 409:
            self.principal_exists_cache[name] = True
            logging.info("[PRINCIPAL] already exists %s", name)
            return

        raise PolarisAdminError(
            f"[PRINCIPAL] failed name={name} http={response.status_code} "
            f"detail={self._format_response_error(response)} "
            f"payload={self._json_for_log(payload)}"
        )

    def ensure_catalog_role(self, catalog: str, role: str) -> None:
        """Ensure that the target catalog role exists in the target catalog."""
        payload = {"catalogRole": {"name": role}}
        logging.info(
            "[CATALOG_ROLE] ensure start | catalog=%s role=%s payload=%s",
            catalog,
            role,
            self._json_for_log(payload),
        )

        if self._catalog_role_exists(catalog, role):
            logging.info("[CATALOG_ROLE] already exists %s/%s", catalog, role)
            return

        response = self.request(
            "POST",
            "/api/management/v1/catalogs/"
            f"{self._quote_path_segment(catalog)}"
            "/catalog-roles",
            payload,
        )
        if response.status_code == 201:
            self.catalog_role_exists_cache[(catalog, role)] = True
            logging.info("[CATALOG_ROLE] created %s/%s", catalog, role)
            return
        if response.status_code == 409:
            self.catalog_role_exists_cache[(catalog, role)] = True
            logging.info("[CATALOG_ROLE] already exists %s/%s", catalog, role)
            return

        raise PolarisAdminError(
            f"[CATALOG_ROLE] failed catalog={catalog} role={role} "
            f"http={response.status_code} detail={self._format_response_error(response)} "
            f"payload={self._json_for_log(payload)}"
        )

    def grant_to_catalog_role(self, catalog: str, role: str, privilege: str) -> None:
        """Ensure that the target catalog privilege is granted to the target catalog role."""
        payload = {"grant": {"type": "catalog", "privilege": privilege}}

        logging.info(
            "[GRANT] apply start | catalog=%s role=%s privilege=%s payload=%s",
            catalog,
            role,
            privilege,
            self._json_for_log(payload),
        )

        if self._catalog_role_has_privilege(catalog, role, privilege):
            logging.info(
                "[GRANT] already present privilege=%s on %s/%s",
                privilege,
                catalog,
                role,
            )
            return

        response = self.request(
            "PUT",
            "/api/management/v1/catalogs/"
            f"{self._quote_path_segment(catalog)}"
            "/catalog-roles/"
            f"{self._quote_path_segment(role)}"
            "/grants",
            payload,
        )
        if response.status_code in (200, 201, 204):
            self.catalog_role_grants_cache.setdefault((catalog, role), set()).add(
                privilege
            )
            logging.info(
                "[GRANT] applied privilege=%s on %s/%s",
                privilege,
                catalog,
                role,
            )
            return
        if response.status_code == 409 or self._is_duplicate_assignment_error(response):
            self.catalog_role_grants_cache.setdefault((catalog, role), set()).add(
                privilege
            )
            logging.info(
                "[GRANT] already present privilege=%s on %s/%s",
                privilege,
                catalog,
                role,
            )
            return

        raise PolarisAdminError(
            f"[GRANT] failed catalog={catalog} role={role} privilege={privilege} "
            f"http={response.status_code} detail={self._format_response_error(response)} "
            f"payload={self._json_for_log(payload)}"
        )

    def assign_catalog_role_to_principal_role(
        self,
        principal_role: str,
        catalog: str,
        catalog_role: str,
    ) -> None:
        """Ensure that the target catalog role is assigned to the target principal role."""
        payload = {"catalogRole": {"name": catalog_role}}

        logging.info(
            "[ROLE_BINDING] assign start | principal_role=%s catalog=%s "
            "catalog_role=%s payload=%s",
            principal_role,
            catalog,
            catalog_role,
            self._json_for_log(payload),
        )

        if self._principal_role_has_catalog_role(principal_role, catalog, catalog_role):
            logging.info(
                "[ROLE_BINDING] already present %s/%s -> %s",
                catalog,
                catalog_role,
                principal_role,
            )
            return

        response = self.request(
            "PUT",
            "/api/management/v1/principal-roles/"
            f"{self._quote_path_segment(principal_role)}"
            "/catalog-roles/"
            f"{self._quote_path_segment(catalog)}",
            payload,
        )
        if response.status_code in (200, 201, 204):
            self.catalog_role_bindings_cache.setdefault(
                (principal_role, catalog),
                set(),
            ).add(catalog_role)
            logging.info(
                "[ROLE_BINDING] %s/%s -> %s",
                catalog,
                catalog_role,
                principal_role,
            )
            return
        if response.status_code == 409 or self._is_duplicate_assignment_error(response):
            self.catalog_role_bindings_cache.setdefault(
                (principal_role, catalog),
                set(),
            ).add(catalog_role)
            logging.info(
                "[ROLE_BINDING] already present %s/%s -> %s",
                catalog,
                catalog_role,
                principal_role,
            )
            return

        raise PolarisAdminError(
            f"[ROLE_BINDING] failed catalog={catalog} catalog_role={catalog_role} "
            f"principal_role={principal_role} http={response.status_code} "
            f"detail={self._format_response_error(response)} "
            f"payload={self._json_for_log(payload)}"
        )

    def assign_principal_role_to_principal(
        self,
        principal: str,
        principal_role: str,
    ) -> None:
        """Ensure that the target principal role is assigned to the target principal."""
        payload = {"principalRole": {"name": principal_role}}

        logging.info(
            "[PRINCIPAL_ASSIGN] assign start | principal=%s principal_role=%s payload=%s",
            principal,
            principal_role,
            self._json_for_log(payload),
        )

        if self._principal_has_role(principal, principal_role):
            logging.info(
                "[PRINCIPAL_ASSIGN] already present %s -> %s",
                principal_role,
                principal,
            )
            return

        response = self.request(
            "PUT",
            "/api/management/v1/principals/"
            f"{self._quote_path_segment(principal)}"
            "/principal-roles",
            payload,
        )
        if response.status_code in (200, 201, 204):
            self.principal_role_assignments_cache.setdefault(principal, set()).add(
                principal_role
            )
            logging.info(
                "[PRINCIPAL_ASSIGN] %s -> %s",
                principal_role,
                principal,
            )
            return
        if response.status_code == 409 or self._is_duplicate_assignment_error(response):
            self.principal_role_assignments_cache.setdefault(principal, set()).add(
                principal_role
            )
            logging.info(
                "[PRINCIPAL_ASSIGN] already present %s -> %s",
                principal_role,
                principal,
            )
            return

        raise PolarisAdminError(
            f"[PRINCIPAL_ASSIGN] failed principal_role={principal_role} principal={principal} "
            f"http={response.status_code} detail={self._format_response_error(response)} "
            f"payload={self._json_for_log(payload)}"
        )

    def _catalogs_from_grant(self, role_name: str, grant: Any) -> List[str]:
        """Validate and return the catalog names referenced by a grant block."""
        if not isinstance(grant, dict):
            raise PolarisAdminError(
                f"catalog role '{role_name}' grant must be a mapping, got: {grant!r}"
            )

        raw_catalogs = grant.get("onCatalogs")
        if not isinstance(raw_catalogs, list) or not raw_catalogs:
            raise PolarisAdminError(
                f"catalog role '{role_name}' grant must contain a non-empty onCatalogs list, got: {grant!r}"
            )

        catalogs: List[str] = []
        seen: Set[str] = set()

        for item in raw_catalogs:
            if not isinstance(item, str) or not item.strip():
                raise PolarisAdminError(
                    f"catalog role '{role_name}' grant has an invalid catalog in onCatalogs: {grant!r}"
                )
            catalog_name = item.strip()
            if catalog_name not in seen:
                seen.add(catalog_name)
                catalogs.append(catalog_name)

        return catalogs

    def _privileges_from_grant(
        self,
        role_name: str,
        grant: Dict[str, Any],
        catalog_name: str,
    ) -> List[str]:
        """Validate and return the privilege list referenced by a grant block."""
        privileges = grant.get("privileges")
        if not isinstance(privileges, list) or not privileges:
            raise PolarisAdminError(
                f"catalog role '{role_name}' grant for catalog '{catalog_name}' "
                f"must contain a non-empty privileges list, got: {grant!r}"
            )

        cleaned: List[str] = []
        seen: Set[str] = set()

        for privilege in privileges:
            if not isinstance(privilege, str) or not privilege.strip():
                raise PolarisAdminError(
                    f"catalog role '{role_name}' has an invalid privilege in grant: {grant!r}"
                )
            privilege_name = privilege.strip()
            if privilege_name not in seen:
                seen.add(privilege_name)
                cleaned.append(privilege_name)

        return cleaned

    def validate_realm(self, realm: Dict[str, Any]) -> None:
        """Validate cross-references inside a single realm contract."""
        catalogs = {item["name"] for item in realm.get("catalogs", []) if "name" in item}
        catalog_roles = {
            item["name"]: item for item in realm.get("catalogRoles", []) if "name" in item
        }
        principal_roles = {
            item["name"] for item in realm.get("principalRoles", []) if "name" in item
        }

        for role in realm.get("catalogRoles", []):
            role_name = role.get("name", "<unknown>")
            grants = role.get("grants", [])
            if not isinstance(grants, list):
                raise PolarisAdminError(f"catalog role '{role_name}' grants must be a list")

            for grant in grants:
                catalog_names = self._catalogs_from_grant(role_name, grant)
                for catalog_name in catalog_names:
                    self._privileges_from_grant(role_name, grant, catalog_name)

                    if catalog_name not in catalogs:
                        raise PolarisAdminError(
                            f"catalog role '{role_name}' references undeclared catalog '{catalog_name}'. "
                            f"Declared catalogs in realm '{realm.get('name', '<unknown>')}': {sorted(catalogs)}"
                        )

        for principal_role in realm.get("principalRoles", []):
            principal_role_name = principal_role.get("name", "<unknown>")
            catalog_role_names = principal_role.get("catalogRoles", [])
            if not isinstance(catalog_role_names, list):
                raise PolarisAdminError(
                    f"principal role '{principal_role_name}' catalogRoles must be a list"
                )
            for catalog_role_name in catalog_role_names:
                if catalog_role_name not in catalog_roles:
                    raise PolarisAdminError(
                        f"principal role '{principal_role_name}' references undeclared "
                        f"catalog role '{catalog_role_name}'"
                    )

        for principal in realm.get("principals", []):
            principal_name = principal.get("name", "<unknown>")
            principal_role_names = principal.get("principalRoles", [])
            if not isinstance(principal_role_names, list):
                raise PolarisAdminError(
                    f"principal '{principal_name}' principalRoles must be a list"
                )
            for principal_role_name in principal_role_names:
                if principal_role_name not in principal_roles:
                    raise PolarisAdminError(
                        f"principal '{principal_name}' references undeclared principal role "
                        f"'{principal_role_name}'"
                    )

        logging.info(
            "[VALIDATION] realm=%s validation successful",
            realm.get("name", "<unknown>"),
        )

    def catalogs_for_catalog_role(self, realm: Dict[str, Any], role_name: str) -> List[str]:
        """Return the distinct catalog names targeted by a catalog role's grants."""
        target_catalogs: List[str] = []
        seen: Set[str] = set()

        for role in realm.get("catalogRoles", []):
            if role.get("name") != role_name:
                continue
            for grant in role.get("grants", []):
                for catalog_name in self._catalogs_from_grant(role_name, grant):
                    if catalog_name not in seen:
                        seen.add(catalog_name)
                        target_catalogs.append(catalog_name)

        return target_catalogs

    def apply_realm(self, realm: Dict[str, Any]) -> None:
        """Apply catalogs, roles, grants, and assignments for a single realm."""
        realm_name = realm.get("name", "")
        if not realm_name:
            raise PolarisAdminError("realm.name is required")

        self.set_realm(realm_name)
        self.validate_realm(realm)

        logging.info("[REALM] applying realm=%s", realm_name)
        logging.info(
            "[REALM] summary | realm=%s catalogs=%d catalogRoles=%d principalRoles=%d principals=%d",
            realm_name,
            len(realm.get("catalogs", [])),
            len(realm.get("catalogRoles", [])),
            len(realm.get("principalRoles", [])),
            len(realm.get("principals", [])),
        )

        for catalog in realm.get("catalogs", []):
            self.ensure_catalog(catalog)

        for catalog_role in realm.get("catalogRoles", []):
            role_name = catalog_role["name"]
            target_catalogs = self.catalogs_for_catalog_role(realm, role_name)

            for catalog_name in target_catalogs:
                self.ensure_catalog_role(catalog_name, role_name)

            for grant in catalog_role.get("grants", []):
                for catalog_name in self._catalogs_from_grant(role_name, grant):
                    for privilege in self._privileges_from_grant(
                        role_name,
                        grant,
                        catalog_name,
                    ):
                        self.grant_to_catalog_role(catalog_name, role_name, privilege)

        for principal_role in realm.get("principalRoles", []):
            principal_role_name = principal_role["name"]
            self.ensure_principal_role(principal_role)

            for catalog_role_name in principal_role.get("catalogRoles", []):
                for catalog_name in self.catalogs_for_catalog_role(
                    realm,
                    catalog_role_name,
                ):
                    self.assign_catalog_role_to_principal_role(
                        principal_role_name,
                        catalog_name,
                        catalog_role_name,
                    )

        for principal in realm.get("principals", []):
            principal_name = principal["name"]
            self.ensure_principal(principal)

            for principal_role_name in principal.get("principalRoles", []):
                self.assign_principal_role_to_principal(
                    principal_name,
                    principal_role_name,
                )

        logging.info("[REALM] completed realm=%s", realm_name)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="polaris-admin",
        description=(
            "Apply a declarative Polaris catalog and RBAC contract.\n"
            "Authentication is performed against an external OIDC provider."
        ),
        epilog=(
            "Examples:\n"
            "  polaris-admin --catalog-file /etc/polaris/catalogs.yaml\n"
            "  polaris-admin --catalog-file /etc/polaris/catalogs.yaml --realm sandbox --log-level DEBUG\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--catalog-file",
        required=True,
        metavar="PATH",
        help="Path to catalogs.yaml.",
    )
    parser.add_argument(
        "--realm",
        default=None,
        help="Optional single realm name to apply. When omitted, all realms are applied in file order.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity. Default: INFO.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the Polaris admin CLI."""
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logging.info(
        "[CLI] starting polaris-admin | catalog_file=%s realm=%s log_level=%s",
        args.catalog_file,
        args.realm or "<all>",
        args.log_level,
        )

    try:
        admin = PolarisAdmin()
        contract = admin.load_catalog_file(args.catalog_file)
        realms = admin.select_realms(contract, realm_filter=args.realm)

        for realm in realms:
            admin.apply_realm(realm)

        logging.info("[CLI] completed successfully")
        return 0

    except PolarisAdminError:
        logging.exception("[ERROR] Polaris administration failed with a functional error")
        return 1

    except requests.RequestException:
        logging.exception("[ERROR] HTTP request failed")
        return 1

    except yaml.YAMLError:
        logging.exception("[ERROR] invalid YAML")
        return 1

    except Exception:
        logging.exception("[ERROR] unexpected unhandled exception")
        return 1


if __name__ == "__main__":
    sys.exit(main())
