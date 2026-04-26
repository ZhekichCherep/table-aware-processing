"""Encoding and separator detection for CSV files.

Russian Excel exports are routinely Windows-1251 with `;` as the separator,
and BOM-prefixed UTF-8 is also common. We auto-detect both, with a small,
deterministic preference order to keep behaviour predictable.
"""

from __future__ import annotations

import csv
from pathlib import Path

# Order matters: we try utf-8 first (BOM-aware), then cp1251 (very common in RU).
ENCODING_CANDIDATES: tuple[str, ...] = ("utf-8-sig", "utf-8", "cp1251", "latin-1")


def detect_encoding(path: Path, *, sample_bytes: int = 65_536) -> str:
    """Pick the best encoding for the file head.

    Strategy:
    1. BOM-prefixed UTF-8 wins outright.
    2. Try strict UTF-8 — succeeds on ASCII and clean UTF-8 (the common case).
    3. Try cp1251 strictly — covers most Russian csv exports from Excel.
       charset-normalizer is biased towards Baltic ISO-8859 variants on short
       Cyrillic samples, so we trust an explicit cp1251 round-trip first.
    4. Fall through to charset-normalizer if available, else latin-1.
    """
    with path.open("rb") as fp:
        head = fp.read(sample_bytes)

    if head.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    # 2. Strict UTF-8 — fails on cp1251.
    try:
        head.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        pass

    # 3. cp1251 strict + sanity check — count Cyrillic letters in the result.
    try:
        decoded = head.decode("cp1251")
        cyrillic = sum(1 for ch in decoded if "\u0400" <= ch <= "\u04ff")
        if cyrillic >= 3:
            return "cp1251"
    except UnicodeDecodeError:
        pass

    # 4. charset-normalizer as a last hint.
    try:
        from charset_normalizer import from_bytes
    except ImportError:
        from_bytes = None

    if from_bytes is not None:
        match = from_bytes(head).best()
        if match is not None and match.encoding:
            return match.encoding.lower().replace("_", "-")

    for enc in ("cp1252", "latin-1"):
        try:
            head.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def detect_separator(path: Path, encoding: str, *, sample_chars: int = 8192) -> str:
    """Use csv.Sniffer to pick a separator from the canonical candidates."""
    with path.open("r", encoding=encoding, errors="replace") as fp:
        sample = fp.read(sample_chars)
    if not sample.strip():
        return ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except csv.Error:
        # Fallback: count occurrences in the first non-empty line.
        for line in sample.splitlines():
            if not line.strip():
                continue
            counts = {sep: line.count(sep) for sep in (",", ";", "\t", "|")}
            best, n = max(counts.items(), key=lambda kv: kv[1])
            return best if n > 0 else ","
        return ","
