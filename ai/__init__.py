"""AI / chatbot subsystem.

Modules:
  - llm_client: provider-agnostic LLM client (OpenAI default, swappable)
  - tools:      tool functions the LLM can call against our own data layer
  - prompts:    system prompts
  - chatbot:    orchestrates the LLM + tool-call loop
"""
from .chatbot import Chatbot
from .llm_client import LLMClient, build_llm_client
from .tools import ToolRegistry

__all__ = ["Chatbot", "LLMClient", "build_llm_client", "ToolRegistry"]
