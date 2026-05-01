"""
POST /api/chat — chatbot endpoint.

Wired in app.py via app.include_router(build_chat_router(...)). Kept as a
factory so the route closes over the live Chatbot instance instead of pulling
it from a module global.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ai.chatbot import Chatbot


class ChatMessage(BaseModel):
    role: str = Field(..., pattern="^(user|assistant)$")
    content: str


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)
    history: list[ChatMessage] = Field(default_factory=list, max_length=20)


class ChatResponse(BaseModel):
    reply: str
    tool_calls: list[dict]


def build_chat_router(chatbot: Optional[Chatbot]) -> APIRouter:
    router = APIRouter()

    @router.post("/api/chat", response_model=ChatResponse)
    async def chat(req: ChatRequest):
        if chatbot is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Chatbot is not configured. Set OPENAI_API_KEY in your "
                    ".env (or set LLM_PROVIDER) and restart the app."
                ),
            )
        try:
            result = await chatbot.reply(
                user_message=req.message,
                history=[m.model_dump() for m in req.history],
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"chat failed: {exc}")
        return ChatResponse(reply=result["reply"], tool_calls=result["tool_calls"])

    @router.get("/api/chat/health")
    def chat_health():
        return {"available": chatbot is not None}

    return router
