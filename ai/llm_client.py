"""
LLM client — Gemini only.

The Chatbot uses OpenAI's chat-completions schema as its internal message
format (de-facto standard). This module translates that to Gemini's native
shape via _to_gemini_messages and _to_gemini_tools.
"""
from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class LLMResponse:
    text: Optional[str]                       # final assistant text (None if only tool calls)
    tool_calls: list[dict]                    # [{id, name, arguments(dict)}]


class GeminiClient:
    """Google Gemini via the google-genai SDK."""

    def __init__(self) -> None:
        from google import genai
        key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not key:
            raise RuntimeError(
                "GEMINI_API_KEY is not set. Get a free key at "
                "https://ai.google.dev and add it to your .env."
            )
        self._client = genai.Client(api_key=key)
        self._model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

    async def chat(
        self,
        *,
        messages: list[dict],
        tools: list[dict],
    ) -> LLMResponse:
        from google.genai import types
        system_instruction, contents = _to_gemini_messages(messages)
        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=0.3,
            tools=_to_gemini_tools(tools) if tools else None,
        )
        resp = await self._client.aio.models.generate_content(
            model=self._model, contents=contents, config=config,
        )

        text_parts: list[str] = []
        calls: list[dict] = []
        for cand in (resp.candidates or []):
            for part in (getattr(cand.content, "parts", None) or []):
                fc = getattr(part, "function_call", None)
                if fc and getattr(fc, "name", None):
                    calls.append({
                        "id": f"call_{uuid.uuid4().hex[:12]}",
                        "name": fc.name,
                        "arguments": dict(fc.args or {}),
                    })
                txt = getattr(part, "text", None)
                if txt:
                    text_parts.append(txt)
            break
        return LLMResponse(
            text=("".join(text_parts) or None),
            tool_calls=calls,
        )


def build_llm_client() -> GeminiClient:
    return GeminiClient()


def _to_gemini_messages(messages: list[dict]) -> tuple[Optional[str], list[dict]]:
    """OpenAI-format messages → Gemini's `contents` array. System messages
    are extracted because Gemini takes them via config, not in `contents`.
    """
    system_chunks: list[str] = []
    contents: list[dict] = []
    call_id_to_name: dict[str, str] = {}

    for msg in messages:
        role = msg.get("role")
        if role == "system":
            if msg.get("content"):
                system_chunks.append(msg["content"])
        elif role == "user":
            contents.append({"role": "user",
                             "parts": [{"text": msg.get("content") or ""}]})
        elif role == "assistant":
            parts: list[dict] = []
            if msg.get("content"):
                parts.append({"text": msg["content"]})
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
        elif role == "tool":
            name = call_id_to_name.get(msg.get("tool_call_id") or "", "unknown_function")
            payload_raw = msg.get("content") or "{}"
            try:
                payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
            except json.JSONDecodeError:
                payload = {"raw": payload_raw}
            if not isinstance(payload, dict):
                payload = {"result": payload}
            contents.append({"role": "user",
                             "parts": [{"function_response":
                                        {"name": name, "response": payload}}]})

    return ("\n\n".join(system_chunks) if system_chunks else None), contents


def _to_gemini_tools(tools: list[dict]) -> list[dict]:
    """OpenAI tool defs → Gemini's `function_declarations` shape."""
    decls: list[dict] = []
    for t in tools:
        fn = (t.get("function") or {}) if t.get("type") == "function" else t
        if not fn.get("name"):
            continue
        decls.append({
            "name": fn["name"],
            "description": fn.get("description", ""),
            "parameters": _normalize_schema(
                fn.get("parameters") or {"type": "object", "properties": {}}
            ),
        })
    return [{"function_declarations": decls}] if decls else []


def _normalize_schema(schema: dict) -> dict:
    """Gemini wants JSON schema types upper-cased (STRING, INTEGER, ...)."""
    if not isinstance(schema, dict):
        return schema
    out = dict(schema)
    if isinstance(out.get("type"), str):
        out["type"] = out["type"].upper()
    if isinstance(out.get("properties"), dict):
        out["properties"] = {k: _normalize_schema(v) for k, v in out["properties"].items()}
    if isinstance(out.get("items"), dict):
        out["items"] = _normalize_schema(out["items"])
    return out
