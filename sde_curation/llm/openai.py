"""OpenAI (or any OpenAI-compatible endpoint) using structured outputs → Pydantic."""

from __future__ import annotations

import asyncio
import logging

from pydantic import BaseModel, ValidationError

from ..config import Settings
from .base import LLMError, T

log = logging.getLogger(__name__)


class OpenAIProvider:
    name = "openai"

    def __init__(self, settings: Settings):
        if not settings.openai_api_key:
            raise LLMError("OPENAI_API_KEY is not set (or choose LLM_PROVIDER=fake)")
        from openai import AsyncOpenAI

        self.model = settings.openai_model
        self.timeout = settings.llm_timeout_s
        self.client = AsyncOpenAI(
            api_key=settings.openai_api_key,
            base_url=settings.openai_base_url or None,
            timeout=settings.llm_timeout_s,
            max_retries=2,
        )

    async def complete(self, *, system: str, user: str, schema: type[T]) -> T:
        from openai import APIError

        try:
            resp = await asyncio.wait_for(
                self.client.chat.completions.parse(
                    model=self.model,
                    messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
                    response_format=schema,
                    temperature=0,
                ),
                timeout=self.timeout + 5,
            )
        except (APIError, TimeoutError, OSError) as e:
            raise LLMError(f"{self.name}/{self.model}: {type(e).__name__}: {e}") from e
        choice = resp.choices[0]
        if getattr(choice.message, "refusal", None):
            raise LLMError(f"model refused: {choice.message.refusal}")
        parsed: BaseModel | None = choice.message.parsed
        if parsed is None:
            # provider returned text that did not validate — re-validate to get a precise error
            try:
                return schema.model_validate_json(choice.message.content or "")
            except ValidationError as e:
                raise LLMError(f"response did not match {schema.__name__}: {e}") from e
        return parsed  # type: ignore[return-value]
