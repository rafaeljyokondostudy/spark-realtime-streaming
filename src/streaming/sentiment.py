import random
import time
from functools import lru_cache

from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    OpenAI,
    RateLimitError,
)

from config.config import load_openai_config

ALLOWED_SENTIMENTS = {"POSITIVE", "NEGATIVE", "NEUTRAL"}


@lru_cache(maxsize=4)
def _get_client(api_key: str) -> OpenAI:
    # Retries are handled here so SDK retries do not multiply application retries.
    return OpenAI(api_key=api_key, max_retries=0, timeout=30.0)


def sentiment_analysis(comment: str, cfg: dict | None = None) -> str:
    if comment is None or not str(comment).strip():
        return "EMPTY"

    cfg = load_openai_config() if cfg is None else cfg
    client = _get_client(cfg["api_key"])

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    "Classify the sentiment of the review. "
                    "Return exactly one word: POSITIVE, NEGATIVE, or NEUTRAL. "
                    "Do not add punctuation or explanations."
                ),
            },
            {"role": "user", "content": str(comment)},
        ],
    )

    content = response.choices[0].message.content
    if content is None:
        raise ValueError("OpenAI returned an empty sentiment response")

    sentiment = content.strip().upper()
    if sentiment not in ALLOWED_SENTIMENTS:
        raise ValueError(f"Unexpected sentiment response: {sentiment!r}")

    return sentiment


def _is_retryable(exc: Exception) -> bool:
    if isinstance(
        exc,
        (RateLimitError, APIConnectionError, APITimeoutError),
    ):
        return True

    return isinstance(exc, APIStatusError) and exc.status_code >= 500


def sentiment_analysis_with_retry(
    comment: str,
    retries: int = 3,
    backoff_seconds: float = 1.0,
    cfg: dict | None = None,
) -> str:
    if retries < 1:
        raise ValueError("retries must be at least 1")

    last_error = None

    for attempt in range(1, retries + 1):
        try:
            return sentiment_analysis(comment, cfg=cfg)
        except Exception as exc:
            last_error = exc

            if attempt == retries or not _is_retryable(exc):
                raise

            delay = backoff_seconds * (2 ** (attempt - 1))
            delay += random.uniform(0, delay * 0.2)
            time.sleep(delay)

    raise RuntimeError("Sentiment analysis failed") from last_error
