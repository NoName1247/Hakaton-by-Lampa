from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Optional

from dateutil import parser as date_parser


_CYR_TO_LAT = str.maketrans(
    {
        "А": "A",
        "В": "B",
        "Е": "E",
        "К": "K",
        "М": "M",
        "Н": "H",
        "О": "O",
        "Р": "P",
        "С": "C",
        "Т": "T",
        "У": "Y",
        "Х": "X",
    }
)


def normalize_kcsr(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if raw is None:
        return None, None
    s = str(raw).strip()
    if not s:
        return None, None
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", "", s)
    s = s.upper().translate(_CYR_TO_LAT)
    digits = re.sub(r"\D+", "", s) or None
    return s, digits


def safe_float(v: object) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("\u00a0", " ").replace(" ", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def safe_date(v: object) -> Optional[datetime.date]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return date_parser.parse(s, dayfirst=True).date()
    except Exception:
        return None


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_many(paths: Iterable[Path]) -> str:
    h = hashlib.sha256()
    for p in sorted(paths, key=lambda x: str(x)):
        h.update(str(p).encode("utf-8"))
        h.update(b"\0")
        h.update(sha256_file(p).encode("ascii"))
        h.update(b"\n")
    return h.hexdigest()


def iter_csv_rows_with_preamble(path: Path, header_startswith: str) -> Iterator[dict[str, str]]:
    """
    RCHB files contain a preamble before the real CSV header.
    We scan lines until header line starts with marker text.
    """
    with path.open("r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    header_idx = None
    for i, line in enumerate(lines[:200]):  # protection
        if line.strip().startswith(header_startswith):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(f"Header marker not found in {path.name}")
    content = "".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(content), delimiter=";")
    for row in reader:
        yield {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


def iter_csv_rows(path: Path, delimiter: str = ",") -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            yield {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


def dataset_version_from_checksum(checksum: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return f"ds_{ts}_{checksum[:12]}"


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

