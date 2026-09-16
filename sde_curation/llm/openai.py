"""OpenAI (or any OpenAI-compatible endpoint) using structured outputs → Pydantic."""

from __future__ import annotations

import asyncio
import logging

from pydantic import BaseModel, ValidationError

from ..config import Settings
from .base import Completion, LLMError, LLMRetryable, T

log = logging.getLogger(__name__)


class OpenAIProvider:
    name = "openai"

    def __init__(self, settings: Settings):
        if not settings.openai_api_key:
            raise LLMError("OPENAI_API_KEY is not set (or choose LLM_PROVIDER=fake)")
        from openai import AsyncOpenAI

        self.model = settings.openai_model
        self.temperature = settings.llm_temperature
        self.timeout = settings.llm_timeout_s
        self.max_retries = settings.llm_max_retries
        # The SDK retries 429 / 5xx / connection errors itself with exponential backoff and
        # honours Retry-After; the outer deadline below must leave room for that whole chain.
        self.client = AsyncOpenAI(
            api_key=settings.openai_api_key,
            base_url=settings.openai_base_url or None,
            timeout=settings.llm_timeout_s,
            max_retries=self.max_retries,
        )

    async def complete(
        self, *, system: str, user: str, schema: type[T], model: str | None = None
    ) -> Completion[T]:
        from openai import (
            APIConnectionError,
            APIError,
            APIStatusError,
            APITimeoutError,
            RateLimitError,
        )

        use_model = model or self.model
        try:
            resp = await asyncio.wait_for(
                self.client.chat.completions.parse(
                    model=use_model,
                    messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
                    response_format=schema,
                    **({"temperature": self.temperature} if self.temperature is not None else {}),
                ),
                timeout=self.timeout * (self.max_retries + 1) + 5,
            )
        except (RateLimitError, APITimeoutError, APIConnectionError) as e:
            raise LLMRetryable(f"{self.name}/{use_model}: {type(e).__name__}: {e}") from e
        except APIStatusError as e:
            cls = LLMRetryable if e.status_code >= 500 else LLMError
            raise cls(f"{self.name}/{use_model}: {type(e).__name__}: {e}") from e
        except (APIError, TimeoutError, OSError) as e:
            raise LLMRetryable(f"{self.name}/{use_model}: {type(e).__name__}: {e}") from e
        choice = resp.choices[0]
        if getattr(choice.message, "refusal", None):
            raise LLMError(f"model refused: {choice.message.refusal}")
        parsed: BaseModel | None = choice.message.parsed
        if parsed is None:
            # provider returned text that did not validate — re-validate to get a precise error
            try:
                parsed = schema.model_validate_json(choice.message.content or "")
            except ValidationError as e:
                raise LLMError(f"response did not match {schema.__name__}: {e}") from e
        usage = getattr(resp, "usage", None)
        details = getattr(usage, "prompt_tokens_details", None) if usage else None
        return Completion(
            parsed=parsed,  # type: ignore[arg-type]
            model=getattr(resp, "model", None) or use_model,
            tokens_in=getattr(usage, "prompt_tokens", 0) or 0,
            tokens_out=getattr(usage, "completion_tokens", 0) or 0,
            tokens_cached=getattr(details, "cached_tokens", 0) or 0,
        )
