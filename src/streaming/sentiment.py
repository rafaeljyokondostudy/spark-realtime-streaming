import logging
import random
import time

from openai import OpenAI
from config.config import load_config

logger = logging.getLogger(__name__)

VALID_SENTIMENTS = {"POSITIVE", "NEGATIVE", "NEUTRAL"}


def sentiment_analysis(comment: str) -> str:

    if not comment or not str(comment).strip():
        return "EMPTY"

    cfg = load_config()

    client = OpenAI(api_key=cfg["openai"]["api_key"])

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a sentiment classifier. "
                    "Return exactly one word: POSITIVE, "
                    "NEGATIVE, or NEUTRAL."
                ),
            },
            {"role": "user", "content": str(comment)},
        ],
        timeout=30,
    )

    sentiment = response.choices[0].message.content

    if sentiment is None:
        raise ValueError("Empty response from OpenAI")

    sentiment = sentiment.strip().upper()

    if sentiment not in VALID_SENTIMENTS:
        raise ValueError(f"Unexpected sentiment: {sentiment}")

    return sentiment


def sentiment_analysis_with_retry(
    comment: str, retries: int = 3, backoff_seconds: float = 1.0
) -> str:

    last_error = None

    for attempt in range(1, retries + 1):

        try:
            return sentiment_analysis(comment)

        except Exception as exc:

            last_error = exc

            logger.warning(
                "Sentiment analysis failed on attempt %s/%s", attempt, retries
            )

            if attempt == retries:
                break

            delay = min(backoff_seconds * (2 ** (attempt - 1)), 30)

            delay += random.uniform(0, 1)

            time.sleep(delay)

    raise RuntimeError(
        f"Sentiment analysis failed after {retries} attempts"
    ) from last_error
