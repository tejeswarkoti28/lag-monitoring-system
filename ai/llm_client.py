"""
Provider-agnostic LLM client.

Default: OpenAI. Swappable via the LLM_PROVIDER env var. The Chatbot only
depends on the LLMClient interface — adding Anthropic/Vertex/Ollama later
is a new file in this folder + one line in build_llm_client().
"""
from __future__ import annotations

import abc
import os
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
        """Send a turn to the LLM. Tools are in OpenAI tool-spec format —
        adapt internally if the provider uses a different schema.
        """
        ...


class OpenAIClient(LLMClient):
    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        default_model: Optional[str] = None,
    ) -> None:
        # Imported lazily so the simulator can run without openai installed
        from openai import AsyncOpenAI
        key = api_key or os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError(
                "OPENAI_API_KEY is not set. Add it to .env or export it before "
                "starting the app, or set LLM_PROVIDER to a different provider."
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
        import json
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


def build_llm_client() -> LLMClient:
    """Pick a client based on LLM_PROVIDER env var."""
    provider = os.environ.get("LLM_PROVIDER", "openai").lower().strip()
    if provider == "openai":
        return OpenAIClient()
    # Add Anthropic / Vertex / Ollama branches here as needed.
    raise ValueError(
        f"Unknown LLM_PROVIDER={provider!r}. Supported: 'openai'. "
        f"Add a new client in ai/llm_client.py to support more providers."
    )
