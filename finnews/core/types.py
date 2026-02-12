from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Document:
    source: str
    url: str
    title: str
    raw_text: str
    published_at: datetime | None
    fetched_at: datetime
    processed_at: datetime
    meta: dict[str, Any]


@dataclass(frozen=True)
class IngestResult:
    inserted: int
    skipped_duplicates: int
    skipped_invalid: int
