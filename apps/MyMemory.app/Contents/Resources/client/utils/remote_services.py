"""Remote Services — cloud-mode klienter för transcribern.

Exponerar samma interface som lokala tjänster (LLMService, graph_scope,
vector_scope) men routar via Hetzner-servern:

- LLM-anrop → M2M Service API (/api/v1/llm/generate, port 8001)
- Graf/vektor/kalender-lookups → M2C MCP (port 8000)

Se documentation/client_architecture.md → "Kommunikationsprinciper".

Transcribern (och andra klient-tjänster) importerar detta istället för
lokala services vid cloud-mode.

Konfigureras via klient-config (cloud.api_url, cloud.verify_tls) och
access-token från menubar-brokern (~/Library/Caches/MyMemory/access_token,
#234). Brokern roterar token var 10:e min — vi läser filen per request.
"""

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Optional

import requests
import urllib3
import yaml

from client.utils.broker_auth import get_access_token, reload_access_token

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LOGGER = logging.getLogger("REMOTE_SERVICES")


# --- Config + Auth ---

def _load_cloud_config() -> dict:
    """Hämta cloud-section från klient-config."""
    from server.utils.config_loader import get_config
    cfg = get_config()
    cloud = cfg.get("cloud", {})
    api_url = cloud.get("api_url", "").rstrip("/")
    if not api_url or api_url.startswith("__"):
        raise RuntimeError(
            "HARDFAIL: cloud.api_url saknas eller är placeholder i config"
        )
    return {
        "api_url": api_url,
        "verify_tls": cloud.get("verify_tls", True),
    }


# Singleton-state (endast config — token läses per request för rotation)
_cloud_config: Optional[dict] = None


def _ensure_config():
    global _cloud_config
    if _cloud_config is None:
        _cloud_config = _load_cloud_config()


def _auth_headers() -> dict:
    """Header med färsk access-token från brokern.

    HARDFAIL om broker inte har skrivit fil än — caller ska hantera
    det som "menubar inte igång" och visa lämpligt fel till användaren.
    """
    _ensure_config()
    token = get_access_token()
    if not token:
        raise RuntimeError(
            "HARDFAIL: Ingen access-token tillgänglig från menubar-brokern. "
            "Starta menubar-appen och logga in först."
        )
    return {"Authorization": f"Bearer {token}"}


# ============================================================================
# RemoteLLM — M2M LLM-proxy
# ============================================================================

@dataclass
class LLMResponse:
    """Kompatibel med server.utils.llm_service.LLMResponse."""
    text: str
    success: bool
    error: Optional[str] = None
    model: Optional[str] = None
    input_tokens: int = 0
    output_tokens: int = 0

    @property
    def tokens_used(self) -> int:
        return self.input_tokens + self.output_tokens


class RemoteLLM:
    """Drop-in-ersättning för LLMService i cloud-mode.

    Routar generate-anrop via M2M Service API (/api/v1/llm/generate).
    Samma interface: .generate(prompt, model=...), .models dict.
    """

    # Model-alias som transcribern använder → mappar till server-config
    models = {
        "pro": "pro",
        "fast": "fast",
        "lite": "lite",
        "transcribe": "transcribe",
    }

    def generate(self, prompt: str, model: str = "fast") -> LLMResponse:
        """Skicka prompt till M2M LLM-proxy och returnera LLMResponse."""
        _ensure_config()
        url = f"{_cloud_config['api_url']}/api/v1/llm/generate"

        # Mappa model-namn till alias (transcribern skickar
        # det resolvade modellnamnet men M2M vill ha alias)
        alias = model
        for key, val in self.models.items():
            if val == model or key == model:
                alias = key
                break

        try:
            resp = requests.post(
                url,
                headers={
                    **_auth_headers(),
                    "Content-Type": "application/json",
                },
                json={"prompt": prompt, "model": alias},
                verify=_cloud_config["verify_tls"],
                timeout=120,
            )
        except requests.RequestException as exc:
            return LLMResponse(
                text="", success=False,
                error=f"M2M-anslutningsfel: {exc}", model=model,
            )

        if resp.status_code == 401:
            return LLMResponse(
                text="", success=False,
                error="M2M: ogiltig eller utgången PAT (401)", model=model,
            )
        if resp.status_code != 200:
            return LLMResponse(
                text="", success=False,
                error=f"M2M: HTTP {resp.status_code}: {resp.text[:300]}",
                model=model,
            )

        data = resp.json()
        if "error" in data:
            return LLMResponse(
                text="", success=False,
                error=f"M2M: {data['error']}: {data.get('message', '')}",
                model=model,
            )

        usage = data.get("usage", {})
        return LLMResponse(
            text=data.get("text", ""),
            success=True,
            model=data.get("model", model),
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
        )


# ============================================================================
# RemoteMCP — M2C MCP-klient
# ============================================================================

class RemoteMCP:
    """Minimal MCP HTTP-klient för M2C-lookups (graf, vektor, kalender).

    Använder Streamable HTTP-transporten (POST /mcp, JSON-RPC).
    """

    def __init__(self):
        _ensure_config()
        self._url = f"{_cloud_config['api_url']}/mcp"
        self._verify = _cloud_config["verify_tls"]
        self._id = 0
        self._session_id = None

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def _headers(self) -> dict:
        h = {
            **_auth_headers(),
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self._session_id:
            h["mcp-session-id"] = self._session_id
        return h

    def _parse_sse(self, text: str) -> dict:
        """Parsa SSE-svar (strict \\n-split, inte splitlines)."""
        parts: list[str] = []
        for line in text.split("\n"):
            line = line.rstrip("\r")
            if line.startswith("data:"):
                parts.append(line[5:].lstrip())
            elif line == "" and parts:
                break
        if not parts:
            raise RuntimeError(f"SSE utan data-event: {text[:500]}")
        return json.loads("\n".join(parts))

    def initialize(self):
        """MCP handshake (initialize + notifications/initialized)."""
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "transcriber", "version": "1.0"},
            },
        }
        resp = requests.post(
            self._url, headers=self._headers(),
            json=payload, verify=self._verify, timeout=30,
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"
        self._session_id = resp.headers.get("mcp-session-id")

        # notifications/initialized
        requests.post(
            self._url, headers=self._headers(),
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            verify=self._verify, timeout=30,
        )

    def call_tool(self, name: str, arguments: dict) -> str:
        """Anropa MCP-tool. Returnerar text-output."""
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        resp = requests.post(
            self._url, headers=self._headers(),
            json=payload, verify=self._verify, timeout=60,
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"

        ct = resp.headers.get("content-type", "")
        data = self._parse_sse(resp.text) if "text/event-stream" in ct else resp.json()

        if "error" in data:
            raise RuntimeError(f"MCP-fel ({name}): {data['error']}")

        content = data.get("result", {}).get("content", [])
        for block in content:
            if block.get("type") == "text":
                return block.get("text", "")
        return json.dumps(data.get("result", {}), ensure_ascii=False)


# ============================================================================
# Convenience — singleton-access
# ============================================================================

_remote_llm: Optional[RemoteLLM] = None
_remote_mcp: Optional[RemoteMCP] = None


def get_remote_llm() -> RemoteLLM:
    """Singleton RemoteLLM. Lazy-initierad."""
    global _remote_llm
    if _remote_llm is None:
        _remote_llm = RemoteLLM()
    return _remote_llm


def get_remote_mcp() -> RemoteMCP:
    """Singleton RemoteMCP. Lazy-initierad + handshake."""
    global _remote_mcp
    if _remote_mcp is None:
        _remote_mcp = RemoteMCP()
        _remote_mcp.initialize()
    return _remote_mcp


def is_cloud_mode() -> bool:
    """True om klienten är i cloud-mode (har cloud.api_url konfigurerat)."""
    from server.utils.config_loader import get_config
    cfg = get_config()
    api_url = cfg.get("cloud", {}).get("api_url", "")
    return bool(api_url) and not api_url.startswith("__")
