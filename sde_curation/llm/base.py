"""Provider-agnostic LLM interface: one call, structured output validated by a Pydantic model,
returned together with the model that answered and the token usage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, TypeVar

from pydantic import BaseModel

from ..config import Settings

T = TypeVar("T", bound=BaseModel)


class LLMError(RuntimeError):
    """The call cannot succeed as issued: bad credentials, unknown model, refusal, or a response
    that does not match the schema."""


class LLMRetryable(LLMError):
    """The provider was unavailable (rate limit, 5xx, timeout, connection) even after the
    client's own retries — the item can be re-tried later; the job as a whole is still healthy."""


@dataclass
class Completion[T: BaseModel]:
    parsed: T
    model: str
    tokens_in: int = 0
    tokens_out: int = 0
    tokens_cached: int = 0  # prompt tokens served from the provider's prompt cache


class LLMProvider(Protocol):
    name: str

    async def complete(
        self, *, system: str, user: str, schema: type[T], model: str | None = None
    ) -> Completion[T]:
        """Return a Completion whose `parsed` is an instance of `schema`, or raise LLMError
        (never a half-parsed dict). `model` overrides the provider's default for this call."""
        ...


def make_llm(settings: Settings) -> LLMProvider:
    """Registry: add a provider = add one module + one line here."""
    if settings.llm_provider == "fake":
        from .fake import FakeProvider

        return FakeProvider()
    from .openai import OpenAIProvider

    return OpenAIProvider(settings)
