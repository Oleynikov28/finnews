from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from finnews.core.normalize import normalize_text
from finnews.core.types import Document, IngestResult


DDL = """
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    raw_text TEXT NOT NULL,
    norm_text TEXT NOT NULL,
    published_at TEXT NULL,
    fetched_at TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    meta_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_documents_source_published_at
ON documents(source, published_at);
"""


class SQLiteStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(DDL)

    def ingest_documents(self, docs: Iterable[Document]) -> IngestResult:
        inserted = 0
        skipped_duplicates = 0
        skipped_invalid = 0

        with self.connect() as conn:
            cur = conn.cursor()

            for d in docs:
                if not self._is_valid(d):
                    skipped_invalid += 1
                    continue

                norm_text = normalize_text(d.raw_text)

                # TODO(EXERCISE): implement a stronger duplicate policy if needed.
                # For Stage 0 we dedupe only by URL (UNIQUE constraint).
                try:
                    cur.execute(
                        """
                        INSERT INTO documents
                          (source, url, title, raw_text, norm_text, published_at, fetched_at, processed_at, meta_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            d.source,
                            d.url,
                            d.title,
                            d.raw_text,
                            norm_text,
                            _dt_to_iso(d.published_at),
                            _dt_to_iso(d.fetched_at),
                            _dt_to_iso(d.processed_at),
                            json.dumps(d.meta, ensure_ascii=False, sort_keys=True),
                        ),
                    )
                    inserted += 1
                except sqlite3.IntegrityError as e:
                    # URL duplicate (or other constraint)
                    skipped_duplicates += 1
                    continue

            conn.commit()

        return IngestResult(
            inserted=inserted,
            skipped_duplicates=skipped_duplicates,
            skipped_invalid=skipped_invalid,
        )

    def count_documents(self) -> int:
        with self.connect() as conn:
            (n,) = conn.execute("SELECT COUNT(*) FROM documents").fetchone()
            return int(n)

    @staticmethod
    def _is_valid(d: Document) -> bool:
        # TODO(EXERCISE): extend validation rules:
        # - enforce reasonable lengths
        # - require title or text, etc.
        if not d.source or not d.url or not d.title:
            return False
        if not d.raw_text or not d.raw_text.strip():
            return False
        if len(d.title) < 4 or len(d.raw_text) < 20 or len(d.raw_text) > 1e6:
            return False
        if not(d.url.startswith('http')):
            return False
        return True


def _dt_to_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        # TODO(EXERCISE): decide a policy for naive datetimes (assume UTC? local? reject?)
        # For Stage 0: treat naive as UTC (explicitly).
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat()