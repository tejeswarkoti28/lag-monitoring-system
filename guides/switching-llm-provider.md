# Switching the AI Chatbot Provider

The chatbot is provider-agnostic. Only `ai/llm_client.py` needs to change.
Everything else — `Chatbot`, `ToolRegistry`, `/api/chat` — talks to a generic
`LLMResponse` interface and is unaffected.

---

## Current provider: Gemini

Configured via:
```
LLM_PROVIDER=gemini
GEMINI_API_KEY=your_key_here
GEMINI_MODEL=gemini-2.5-flash        # optional, this is the default
```

Get a key at: https://aistudio.google.com/app/apikey

Gemini API keys have no fixed expiry — they stay valid until you delete them or
Google revokes them for unusual activity.

---

## Switching providers

### Step 1 — Add a new client class in `ai/llm_client.py`

Every client must implement one method:
```python
async def chat(self, *, messages: list[dict], tools: list[dict]) -> LLMResponse
```

`messages` are in OpenAI format `[{role, content}]`.
`LLMResponse` has two fields: `text: str | None` and `tool_calls: list[dict]`.

**Claude (Anthropic):**
```python
class ClaudeClient:
    def __init__(self) -> None:
        import anthropic
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set.")
        self._client = anthropic.AsyncAnthropic(api_key=key)
        self._model = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")

    async def chat(self, *, messages, tools) -> LLMResponse:
        # translate OpenAI-format messages/tools → Anthropic format
        # return LLMResponse(text=..., tool_calls=[...])
        ...
```

**OpenAI:**
```python
class OpenAIClient:
    def __init__(self) -> None:
        from openai import AsyncOpenAI
        key = os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError("OPENAI_API_KEY is not set.")
        self._client = AsyncOpenAI(api_key=key)
        self._model = os.environ.get("OPENAI_MODEL", "gpt-4o")

    async def chat(self, *, messages, tools) -> LLMResponse:
        # OpenAI uses the same message format — minimal translation needed
        ...
```

### Step 2 — Update `build_llm_client()` in `ai/llm_client.py`

```python
def build_llm_client():
    provider = os.environ.get("LLM_PROVIDER", "gemini").lower()
    if provider == "gemini":
        return GeminiClient()
    if provider == "claude":
        return ClaudeClient()
    if provider == "openai":
        return OpenAIClient()
    raise RuntimeError(
        f"Unknown LLM_PROVIDER: {provider!r}. Choose gemini, claude, or openai."
    )
```

### Step 3 — Update `.env`

```
LLM_PROVIDER=claude
ANTHROPIC_API_KEY=your_key_here
CLAUDE_MODEL=claude-sonnet-4-6        # optional

# or for OpenAI:
LLM_PROVIDER=openai
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-4o                   # optional
```

Restart the app. No other files change.

---

## Translation effort per provider

| Provider | Effort | Reason |
|---|---|---|
| OpenAI | Low | Internal message format is already OpenAI-compatible |
| Claude | Medium | System messages handled differently, tool format differs |
| Gemini | Already done | See `GeminiClient` in `ai/llm_client.py` |

---

## What never changes regardless of provider

- `ai/chatbot.py` — the tool-call loop
- `ai/tools.py` — all five tool implementations
- `routes/chat.py` — the `/api/chat` endpoint
- The dashboard chat UI
