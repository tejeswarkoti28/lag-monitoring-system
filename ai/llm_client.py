"""
Provider-agnostic LLM client.

Default: Gemini (google-genai). Swappable via the LLM_PROVIDER env var.
The Chatbot only depends on the LLMClient interface — adding new providers
is a new branch in build_llm_client().

Internal message format follows OpenAI's chat-completions schema (it's the
de-facto standard); each provider client adapts to its own native format.
"""
from __future__ import annotations

import abc
import json
import os
import uuid
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class LLMResponse:
    """Normalised reply across providers."""
    text: Optional[str]                       # final assistant text (None if only tool calls)
    tool_calls: list[dict]                    # [{id, name, arguments(dict)}]
    raw: Any                                  # provider-specific original response


class LLMClient(abc.ABC):
    """Minimum interface a provider must implement."""

    @abc.abstractmethod
    async def chat(
        self,
        *,
        messages: list[dict],
        tools: list[dict],
        model: Optional[str] = None,
    ) -> LLMResponse:
        """Send a turn to the LLM. `messages` and `tools` are in OpenAI format
        — adapt internally if the provider uses a different schema.
        """
        ...


# =============================================================================
# Gemini (Google) — default provider
# =============================================================================
class GeminiClient(LLMClient):
    """Google Gemini via the google-genai SDK."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        default_model: Optional[str] = None,
    ) -> None:
        from google import genai
        key = api_key or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not key:
            raise RuntimeError(
                "GEMINI_API_KEY is not set. Get a free key at https://ai.google.dev "
                "and add it to your .env."
            )
        self._client = genai.Client(api_key=key)
        self._default_model = (
            default_model
            or os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
        )

    async def chat(
        self,
        *,
        messages: list[dict],
        tools: list[dict],
        model: Optional[str] = None,
    ) -> LLMResponse:
        from google.genai import types
        system_instruction, contents = _to_gemini_messages(messages)
        gemini_tools = _to_gemini_tools(tools) if tools else None

        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=0.3,
            tools=gemini_tools,
        )

        resp = await self._client.aio.models.generate_content(
            model=model or self._default_model,
            contents=contents,
            config=config,
        )

        # Pull out function calls + text from the first candidate
        text_parts: list[str] = []
        calls: list[dict] = []
        for cand in (resp.candidates or []):
            content = getattr(cand, "content", None)
            for part in (getattr(content, "parts", None) or []):
                fc = getattr(part, "function_call", None)
                if fc is not None and getattr(fc, "name", None):
                    args = dict(fc.args or {})
                    calls.append({
                        "id": f"call_{uuid.uuid4().hex[:12]}",
                        "name": fc.name,
                        "arguments": args,
                    })
                txt = getattr(part, "text", None)
                if txt:
                    text_parts.append(txt)
            break  # we only use the first candidate

        return LLMResponse(
            text=("".join(text_parts) or None),
            tool_calls=calls,
            raw=resp,
        )


def _to_gemini_messages(messages: list[dict]) -> tuple[Optional[str], list[dict]]:
    """Translate OpenAI-format messages to Gemini's `contents` array.

    Returns (system_instruction, contents). System messages are extracted out
    because Gemini takes them via config, not in `contents`.
    """
    system_chunks: list[str] = []
    contents: list[dict] = []
    # Track tool-call names by call-id so we can attach them to tool responses
    # (Gemini's function_response requires the function name; OpenAI's tool
    # message only carries tool_call_id).
    call_id_to_name: dict[str, str] = {}

    for msg in messages:
        role = msg.get("role")
        if role == "system":
            if msg.get("content"):
                system_chunks.append(msg["content"])
            continue
        if role == "user":
            contents.append({
                "role": "user",
                "parts": [{"text": msg.get("content") or ""}],
            })
            continue
        if role == "assistant":
            parts: list[dict] = []
            text = msg.get("content")
            if text:
                parts.append({"text": text})
            for tc in (msg.get("tool_calls") or []):
                fn = tc.get("function") or {}
                name = fn.get("name")
                if not name:
                    continue
                args_raw = fn.get("arguments") or "{}"
                try:
                    args = json.loads(args_raw) if isinstance(args_raw, str) else dict(args_raw)
                except json.JSONDecodeError:
                    args = {}
                parts.append({"function_call": {"name": name, "args": args}})
                if tc.get("id"):
                    call_id_to_name[tc["id"]] = name
            if parts:
                contents.append({"role": "model", "parts": parts})
            continue
        if role == "tool":
            tool_call_id = msg.get("tool_call_id") or ""
            name = call_id_to_name.get(tool_call_id, "unknown_function")
            payload_raw = msg.get("content") or "{}"
            try:
                payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
            except json.JSONDecodeError:
                payload = {"raw": payload_raw}
            if not isinstance(payload, dict):
                payload = {"result": payload}
            contents.append({
                "role": "user",
                "parts": [{"function_response": {"name": name, "response": payload}}],
            })
            continue

    system_instruction = "\n\n".join(system_chunks) if system_chunks else None
    return system_instruction, contents


def _to_gemini_tools(tools: list[dict]) -> list[dict]:
    """Translate OpenAI tool defs to Gemini's `function_declarations` shape."""
    decls: list[dict] = []
    for t in tools:
        fn = (t.get("function") or {}) if t.get("type") == "function" else t
        if not fn.get("name"):
            continue
        decls.append({
            "name": fn["name"],
            "description": fn.get("description", ""),
            "parameters": _normalize_schema(fn.get("parameters") or {"type": "object", "properties": {}}),
        })
    return [{"function_declarations": decls}] if decls else []


def _normalize_schema(schema: dict) -> dict:
    """Gemini wants JSON schema types upper-cased (STRING, INTEGER, ...)."""
    if not isinstance(schema, dict):
        return schema
    out = dict(schema)
    if "type" in out and isinstance(out["type"], str):
        out["type"] = out["type"].upper()
    if "properties" in out and isinstance(out["properties"], dict):
        out["properties"] = {
            k: _normalize_schema(v) for k, v in out["properties"].items()
        }
    if "items" in out and isinstance(out["items"], dict):
        out["items"] = _normalize_schema(out["items"])
    return out


# =============================================================================
# OpenAI — alternate provider, kept for swappability
# =============================================================================
class OpenAIClient(LLMClient):
    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        default_model: Optional[str] = None,
    ) -> None:
        from openai import AsyncOpenAI
        key = api_key or os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError(
                "OPENAI_API_KEY is not set. Add it to .env or switch "
                "LLM_PROVIDER to 'gemini'."
            )
        self._client = AsyncOpenAI(api_key=key)
        self._default_model = (
            default_model
            or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        )

    async def chat(
        self,
        *,
        messages: list[dict],
        tools: list[dict],
        model: Optional[str] = None,
    ) -> LLMResponse:
        resp = await self._client.chat.completions.create(
            model=model or self._default_model,
            messages=messages,
            tools=tools or None,
            temperature=0.3,
        )
        msg = resp.choices[0].message
        calls: list[dict] = []
        for tc in (msg.tool_calls or []):
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            calls.append({
                "id": tc.id,
                "name": tc.function.name,
                "arguments": args,
            })
        return LLMResponse(text=msg.content, tool_calls=calls, raw=resp)


# =============================================================================
# Factory
# =============================================================================
def build_llm_client() -> LLMClient:
    """Pick a client based on LLM_PROVIDER env var (default: gemini)."""
    provider = os.environ.get("LLM_PROVIDER", "gemini").lower().strip()
    if provider == "gemini":
        return GeminiClient()
    if provider == "openai":
        return OpenAIClient()
    raise ValueError(
        f"Unknown LLM_PROVIDER={provider!r}. Supported: 'gemini', 'openai'. "
        f"Add a new client in ai/llm_client.py to support more providers."
    )
