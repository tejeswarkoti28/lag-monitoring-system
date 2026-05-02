"""AI / chatbot subsystem.

Modules:
  - llm_client: Gemini client (OpenAI-style message format internally)
  - tools:      tool functions the LLM can call against our own data layer
  - prompts:    system prompts
  - chatbot:    orchestrates the LLM + tool-call loop
"""
from .chatbot import Chatbot
from .llm_client import GeminiClient, build_llm_client
from .tools import ToolRegistry

__all__ = ["Chatbot", "GeminiClient", "build_llm_client", "ToolRegistry"]
