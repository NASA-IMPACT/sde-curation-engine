"""Provider-agnostic LLM interface: one call, structured output validated by a Pydantic model."""

from __future__ import annotations

from typing import Protocol, TypeVar

from pydantic import BaseModel

from ..config import Settings

T = TypeVar("T", bound=BaseModel)


class LLMError(RuntimeError):
    pass


class LLMProvider(Protocol):
    name: str

    async def complete(self, *, system: str, user: str, schema: type[T]) -> T:
        """Return an instance of `schema` or raise LLMError (never a half-parsed dict)."""
        ...


def make_llm(settings: Settings) -> LLMProvider:
    """Registry: add a provider = add one module + one line here."""
    if settings.llm_provider == "fake":
        from .fake import FakeProvider

        return FakeProvider()
    from .openai import OpenAIProvider

    return OpenAIProvider(settings)
