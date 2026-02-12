from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from finnews.core.types import Document


def load_documents_from_jsonl(path: Path) -> Iterator[Document]:
    """
    Expected JSONL per line (minimal):
      {
        "source": "cbr",
        "url": "https://...",
        "title": "...",
        "text": "...",
        "published_at": "2026-02-10T12:34:00+03:00"   # optional
      }

    TODO(EXERCISE):
      1) Make parsing more robust: tolerate keys: raw_text/content/body, etc.
      2) Implement better datetime parsing + timezone handling.
      3) Decide what to do if published_at missing/invalid.
    """
    now = datetime.now(timezone.utc)

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                # Skip invalid line
                continue

            source = str(obj.get("source", "")).strip()
            url = str(obj.get("url", "")).strip()
            title = str(obj.get("title", "")).strip()
            text = str(obj.get("text", "")).strip()

            published_at = _parse_dt(obj.get("published_at"))

            yield Document(
                source=source,
                url=url,
                title=title,
                raw_text=text,
                published_at=published_at,
                fetched_at=now,
                processed_at=now,
                meta={
                    "line_no": line_no,
                    # TODO(EXERCISE): store raw fields, language, etc.
                },
            )


def _parse_dt(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # TODO(EXERCISE): interpret numeric timestamps (seconds vs ms) carefully.
        return datetime.fromtimestamp(float(value), tz=timezone.utc)

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            # Accept ISO 8601
            # Python 3.11: datetime.fromisoformat handles offsets like +03:00
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                # TODO(EXERCISE): policy for naive published_at
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None

    return None
