"""OpenAI (or any OpenAI-compatible endpoint) using structured outputs → Pydantic."""

from __future__ import annotations

import asyncio
import logging
import re

from pydantic import BaseModel, ValidationError

from ..config import Settings
from .base import Completion, LLMError, LLMInputTooLong, LLMRetryable, T

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
        self.prompt_cache = settings.openai_prompt_cache
        self._no_cache_options: set[str] = set()  # models that rejected `prompt_cache_options`
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
            BadRequestError,
            RateLimitError,
        )

        use_model = model or self.model

        def request(explicit: bool):
            if explicit:
                # explicit mode: no implicit breakpoint, so only the system prompt (the same on every
                # call of a job) is written to the cache; the page text is billed as ordinary input
                sys_msg: dict = {"role": "system", "content": [
                    {"type": "text", "text": system, "prompt_cache_breakpoint": {"mode": "explicit"}}]}
                cache_opts: dict = {"prompt_cache_options": {"mode": "explicit"}}
            else:
                sys_msg, cache_opts = {"role": "system", "content": system}, {}
            return asyncio.wait_for(
                self.client.chat.completions.parse(
                    model=use_model,
                    messages=[sys_msg, {"role": "user", "content": user}],
                    response_format=schema,
                    **cache_opts,
                    **({"temperature": self.temperature} if self.temperature is not None else {}),
                ),
                timeout=self.timeout * (self.max_retries + 1) + 5,
            )

        explicit = self.prompt_cache == "system" and use_model not in self._no_cache_options
        try:
            try:
                resp = await request(explicit)
            except BadRequestError as e:
                # models before gpt-5.6 reject the option (400, not billed) — and they have no
                # cache-write charge to avoid, so they are simply asked without it from now on
                if not explicit or getattr(e, "param", None) != "prompt_cache_options":
                    raise
                log.info("%s/%s: no prompt_cache_options on this model; sending without", self.name, use_model)
                self._no_cache_options.add(use_model)
                explicit = False
                resp = await request(explicit)
        except (RateLimitError, APITimeoutError, APIConnectionError) as e:
            raise LLMRetryable(f"{self.name}/{use_model}: {type(e).__name__}: {e}") from e
        except BadRequestError as e:
            if getattr(e, "code", None) == "context_length_exceeded":
                # "…configured limit of 272000 tokens. Your messages resulted in 300010 tokens."
                m = re.search(r"limit of (\d+) tokens.*resulted in (\d+) tokens", str(e))
                raise LLMInputTooLong(f"{self.name}/{use_model}: {e}", limit=int(m[1]) if m else None,
                                      got=int(m[2]) if m else None) from e
            raise LLMError(f"{self.name}/{use_model}: {type(e).__name__}: {e}") from e
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
        written = getattr(details, "cache_write_tokens", 0) or 0
        if explicit and written > len(system) // 2:
            # more than the system prompt went into the cache: page text is being billed as cache
            # writes again (2026-09-24: ~$360 in one job) — the API or this request shape changed
            log.error("%s/%s wrote %d prompt tokens to the cache, more than the system prompt",
                      self.name, use_model, written)
        return Completion(
            parsed=parsed,  # type: ignore[arg-type]
            model=getattr(resp, "model", None) or use_model,
            tokens_in=getattr(usage, "prompt_tokens", 0) or 0,
            tokens_out=getattr(usage, "completion_tokens", 0) or 0,
            tokens_cached=getattr(details, "cached_tokens", 0) or 0,
            tokens_cache_write=written,
        )
