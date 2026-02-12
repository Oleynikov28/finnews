from __future__ import annotations

import sys
from pathlib import Path

from finnews.ingest.from_jsonl import load_documents_from_jsonl
from finnews.storage.sqlite import SQLiteStore


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/run_ingest.py data/sample_news.jsonl")
        return 2

    input_path = Path(sys.argv[1]).resolve()
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 2

    db_path = Path("data/documents.db").resolve()
    store = SQLiteStore(db_path=db_path)

    store.init_schema()

    docs = load_documents_from_jsonl(input_path)
    res = store.ingest_documents(docs)

    total = store.count_documents()
    print(
        f"Inserted={res.inserted} "
        f"SkippedDuplicates={res.skipped_duplicates} "
        f"SkippedInvalid={res.skipped_invalid} "
        f"TotalInDB={total} "
        f"DB={db_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
