from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from rapidfuzz import fuzz as _fuzz
except Exception:  # pragma: no cover - fallback for environments without rapidfuzz
    _fuzz = None
    from difflib import SequenceMatcher


BASE_DIR = Path(__file__).resolve().parents[1]


def _candidate_paths(filename: str) -> list[Path]:
    return [
        BASE_DIR / "inputs" / filename,
        BASE_DIR / "Quarterly Report" / filename,
        BASE_DIR / "Dirty" / filename,
        BASE_DIR / filename,
    ]


def _resolve_path(filename: str) -> Path:
    for candidate in _candidate_paths(filename):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Could not find reference file: {filename}")


def _clean_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value)
    text = text.replace("_x000D_\n", " ").replace("_x000D_", " ")
    text = text.replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def norm(value) -> str:
    text = _clean_text(value).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[-/]", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _token_sort(text: str) -> str:
    tokens = [token for token in norm(text).split() if token]
    tokens.sort()
    return " ".join(tokens)


def _ratio(left: str, right: str) -> int:
    if _fuzz is not None:
        return int(_fuzz.ratio(left, right))
    return int(round(SequenceMatcher(None, left, right).ratio() * 100))


def _token_sort_ratio(left: str, right: str) -> int:
    if _fuzz is not None:
        return int(_fuzz.token_sort_ratio(left, right))
    return _ratio(_token_sort(left), _token_sort(right))


def _token_set_ratio(left: str, right: str) -> int:
    if _fuzz is not None:
        return int(_fuzz.token_set_ratio(left, right))
    left_tokens = set(norm(left).split())
    right_tokens = set(norm(right).split())
    if not left_tokens or not right_tokens:
        return 0
    overlap = len(left_tokens & right_tokens)
    return int(round((2 * overlap / (len(left_tokens) + len(right_tokens))) * 100))


def clean_text(value) -> str:
    return _clean_text(value)


def strip_bp_prefix(value) -> str:
    text = clean_text(value)
    return re.sub(r"^BP\s+", "", text, flags=re.I).strip()


def is_sku_like(value) -> bool:
    text = clean_text(value)
    if not text:
        return False
    if re.match(r"^[A-Z]{2,5}-", text):
        return True
    if re.match(r"^\d", text):
        return True
    lowered = text.lower()
    return "mazara" in lowered and bool(re.search(r"\b\d+(?:\.\d+)?\s*(?:g|kg|ml|l|ltr|x)\b", lowered))


def is_brand_row(value, brand_names_norm: set[str] | None = None) -> bool:
    text = clean_text(value)
    if not text:
        return False
    if is_sku_like(text):
        return False
    if text.lower().startswith(("grand total", "printed by", "order generation", "head office")):
        return False
    if brand_names_norm is not None:
        return norm(text) in brand_names_norm or text.lower().startswith("bp ")
    return True


@lru_cache(maxsize=1)
def _load_brand_rows() -> tuple[tuple[str, str], ...]:
    path = _resolve_path("Brand & SKU list.xls")
    df = pd.read_excel(path, header=None)
    rows: list[tuple[str, str]] = []
    current_brand = ""
    for raw in df.iloc[:, 0].tolist():
        text = clean_text(raw)
        if not text:
            continue
        if is_sku_like(text):
            if current_brand:
                rows.append((text, current_brand))
        else:
            current_brand = text
    return tuple(rows)


@lru_cache(maxsize=1)
def _load_brand_headers() -> tuple[str, ...]:
    path = _resolve_path("Brand & SKU list.xls")
    df = pd.read_excel(path, header=None)
    brands: list[str] = []
    for raw in df.iloc[:, 0].tolist():
        text = clean_text(raw)
        if not text or is_sku_like(text):
            continue
        brands.append(text)
    return tuple(dict.fromkeys(brands))


@lru_cache(maxsize=1)
def _load_ledgers() -> tuple[str, ...]:
    path = _resolve_path("Ledgers List.xls")
    df = pd.read_excel(path, header=None)
    ledgers: list[str] = []
    for raw in df.iloc[:, 0].tolist():
        text = clean_text(raw)
        if not text:
            continue
        if text.lower() == "retailer list":
            continue
        ledgers.append(text)
    return tuple(dict.fromkeys(ledgers))


@lru_cache(maxsize=1)
def _load_lookup_maps() -> tuple[dict[str, str], dict[str, str], set[str]]:
    sku_to_brand: dict[str, str] = {}
    prefix_to_brand: dict[str, str] = {}
    brand_names: set[str] = set()
    for sku, brand in _load_brand_rows():
        sku_to_brand[sku] = brand
        brand_names.add(norm(brand))
        match = re.match(r"^([A-Z]{2,5})-", sku)
        if match and match.group(1) not in prefix_to_brand:
            prefix_to_brand[match.group(1)] = brand
    for brand in _load_brand_headers():
        brand_names.add(norm(brand))
    return sku_to_brand, prefix_to_brand, brand_names


sku_to_brand, prefix_to_brand, brand_names_norm = _load_lookup_maps()
ledgers_list = list(_load_ledgers())
ledgers_lower = [norm(ledger) for ledger in ledgers_list]


def token_score(token: str, text_norm: str, threshold: int = 82) -> float:
    if not token or not text_norm:
        return 0.0
    if token in text_norm:
        return 1.0
    best = 0
    for word in text_norm.split():
        best = max(best, _ratio(token, word))
        if best >= threshold:
            return 0.85
    return 0.0


def fuzzy_token_hit(token: str, text_norm: str, threshold: int = 82) -> bool:
    return token_score(token, text_norm, threshold=threshold) > 0


def lookup_brand(sku_name: str) -> str | None:
    cleaned = clean_text(sku_name)
    if not cleaned:
        return None

    exact = sku_to_brand.get(cleaned)
    if exact:
        return exact

    prefix_match = re.match(r"^([A-Z]{2,5})-", cleaned)
    if prefix_match:
        mapped = prefix_to_brand.get(prefix_match.group(1))
        if mapped:
            return mapped

    cleaned_norm = norm(cleaned)
    best_brand = None
    best_score = 0
    for sku, brand in sku_to_brand.items():
        score = _token_sort_ratio(cleaned_norm, norm(sku))
        if score > best_score:
            best_score = score
            best_brand = brand
    if best_brand and best_score >= 85:
        return best_brand
    return None


def smart_ledger_match(keyword: str) -> str:
    cleaned = clean_text(keyword)
    if not cleaned:
        return cleaned

    normalized = norm(cleaned)
    tokens = [token for token in normalized.split() if len(token) >= 3]
    if not tokens:
        return cleaned.title()

    best_score = -1.0
    best_ledger = None
    for ledger, ledger_norm in zip(ledgers_list, ledgers_lower):
        if not ledger_norm:
            continue
        total = 0.0
        for token in tokens:
            total += token_score(token, ledger_norm)
        base = (total / len(tokens)) * 100.0
        bonus = min(max(len(ledger_norm.split()) - 1, 0), 6)
        score = base + bonus
        if score > best_score:
            best_score = score
            best_ledger = ledger

    if best_ledger and best_score >= 50:
        return best_ledger

    fallback_score = -1
    fallback_ledger = None
    for ledger, ledger_norm in zip(ledgers_list, ledgers_lower):
        score = _token_set_ratio(normalized, ledger_norm)
        if score > fallback_score:
            fallback_score = score
            fallback_ledger = ledger

    if fallback_ledger and fallback_score >= 65:
        return fallback_ledger

    return cleaned.title()


def brand_from_text(value: str | None) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    if text.lower().startswith("bp "):
        return strip_bp_prefix(text)
    return text
