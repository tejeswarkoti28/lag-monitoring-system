"""
Chatbot orchestrator.

Wraps the LLM tool-call loop:
  1. send user message + system prompt + tool defs to LLM
  2. if LLM returns tool_calls: execute them, feed results back
  3. repeat until LLM returns plain text (no more tool calls)
  4. return the final text
"""
from __future__ import annotations

import json
from typing import Optional

from .llm_client import GeminiClient
from .prompts import SYSTEM_PROMPT
from .tools import TOOL_DEFS, ToolRegistry


# Hard cap: protects against runaway tool loops if the LLM gets stuck calling
# tools indefinitely. 6 is plenty for any realistic question.
MAX_TOOL_HOPS = 6


class Chatbot:
    def __init__(self, *, llm: GeminiClient, tools: ToolRegistry) -> None:
        self._llm = llm
        self._tools = tools

    async def reply(
        self,
        *,
        user_message: str,
        history: Optional[list[dict]] = None,
    ) -> dict:
        """Run one user turn. Returns {"reply": str, "tool_calls": [...]}.

        `history` is a list of prior {role, content} messages from the caller.
        """
        messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
        for m in (history or []):
            role = m.get("role")
            content = m.get("content", "")
            if role in ("user", "assistant") and content:
                messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": user_message})

        executed_calls: list[dict] = []
        for hop in range(MAX_TOOL_HOPS):
            resp = await self._llm.chat(messages=messages, tools=TOOL_DEFS)

            if not resp.tool_calls:
                return {
                    "reply": resp.text or "(no response)",
                    "tool_calls": executed_calls,
                }

            # Append the assistant turn (with tool_calls) to the message history
            assistant_turn = {
                "role": "assistant",
                "content": resp.text or "",
                "tool_calls": [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": json.dumps(tc["arguments"]),
                        },
                    }
                    for tc in resp.tool_calls
                ],
            }
            messages.append(assistant_turn)

            # Execute each tool call and append its result
            for tc in resp.tool_calls:
                result = self._tools.execute(tc["name"], tc["arguments"])
                executed_calls.append({"name": tc["name"], "arguments": tc["arguments"]})
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": json.dumps(result, default=str)[:8000],
                })

        return {
            "reply": (
                "I hit the tool-call limit while gathering data — try asking a "
                "narrower question (specific team, specific time window, or one job)."
            ),
            "tool_calls": executed_calls,
        }
