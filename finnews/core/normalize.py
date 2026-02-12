from __future__ import annotations

import re


_WS_RE = re.compile(r"\s+")
_ZW_RE = re.compile(r"[\u200B-\u200D\uFEFF]")  # zero-width chars


def normalize_text(text: str) -> str:
    """
    Goal: stable normalization for dedup & downstream NLP.

    Keep it conservative:
    - remove zero-width characters
    - normalize whitespace
    - strip

    TODO(EXERCISE):
      1) Add normalization rules you consider useful for RU news:
         - unify quotes («» -> ")
         - replace non-breaking spaces
         - normalize 'ё' vs 'е' (decide and document)
         - remove excessive punctuation / repeated dots
      2) Ensure the function is idempotent (normalize(normalize(x)) == normalize(x)).
    """
    if text is None:
        return ""

    s = text
    s = _ZW_RE.sub("", s)
    s = s.replace("\xa0", " ")  # NBSP
    s = _WS_RE.sub(" ", s).strip()

    # TODO(EXERCISE): add RU-specific normalization steps here.
    s = s.replace('ё', 'е').replace("Ё", "Е")
    s = re.sub(r"[«»“„]", '"', s)
    s = re.sub(r"([!?.])\1{1,}", r"\1", s)
    s = re.sub(r"(?<=\d)[\s\u202F\u00A0](?=\d)", "", s)
    s = re.sub(r"(?<=\d),(?=\d)", ".", s)

    return s
