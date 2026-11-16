import time

from openai import OpenAI

from config.config import load_config


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
                    "You are a sentiment classifier. Return exactly one word: POSITIVE, "
                    "NEGATIVE, or NEUTRAL. Do not add punctuation or explanations."
                ),
            },
            {"role": "user", "content": str(comment)},
        ],
        timeout=30,
    )
    return response.choices[0].message.content.strip().upper()


def sentiment_analysis_with_retry(comment: str, retries: int = 3, backoff_seconds: float = 1.0) -> str:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return sentiment_analysis(comment)
        except Exception as exc:  # pragma: no cover - external API behavior
            last_error = exc
            if attempt == retries:
                break
            time.sleep(backoff_seconds * attempt)
    raise RuntimeError(f"Sentiment analysis failed after {retries} attempts") from last_error
