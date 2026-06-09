from dataclasses import dataclass, field
from typing import Protocol

from openai import OpenAI


class LLMProvider(Protocol):
    @property
    def model_name(self) -> str: ...

    def generate(self, system_instruction: str, user_prompt: str) -> "GenerationResult": ...


@dataclass(frozen=True)
class GenerationResult:
    text: str
    model: str
    usage: dict = field(default_factory=dict)


class MaritacaProvider:
    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        temperature: float,
        max_output_tokens: int,
        timeout_seconds: float,
        max_retries: int,
        client: OpenAI | None = None,
    ) -> None:
        if not api_key and client is None:
            raise ValueError("MARITACA_API_KEY nao configurada.")
        self._model = model
        self._temperature = temperature
        self._max_output_tokens = max_output_tokens
        self._client = client or OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout_seconds,
            max_retries=max_retries,
        )

    @property
    def model_name(self) -> str:
        return self._model

    def generate(self, system_instruction: str, user_prompt: str) -> GenerationResult:
        response = self._client.responses.create(
            model=self._model,
            instructions=system_instruction,
            input=user_prompt,
            temperature=self._temperature,
            max_output_tokens=self._max_output_tokens,
        )
        usage = response.usage.model_dump() if response.usage else {}
        return GenerationResult(
            text=response.output_text.strip(),
            model=self._model,
            usage=usage,
        )
