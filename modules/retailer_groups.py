"""
retailer_groups.py

Canonical chain-level retailer grouping used by the Retailer Intelligence
surfaces. This keeps obvious branch networks like Justrite and Jendol together
for leadership conversations while preserving store-level drilldown.
"""

from __future__ import annotations

import re
from typing import Any


RETAILER_GROUP_DEFINITIONS: list[dict[str, Any]] = [
    {"slug": "justrite", "name": "Justrite", "aliases": ["justrite"]},
    {"slug": "jendol", "name": "Jendol", "aliases": ["jendol supermarket", "jendol supermaket", "jendol supermarkt"]},
    {"slug": "prince-ebeano", "name": "Prince Ebeano", "aliases": ["prince ebeano", "prince ebano"]},
    {"slug": "grocery-bazaar", "name": "Grocery Bazaar", "aliases": ["grocery bazaar"]},
    {"slug": "foodco", "name": "Foodco", "aliases": ["foodco nigeria", "foodco"]},
    {"slug": "twins-faja", "name": "Twins Faja Supermarket", "aliases": ["twins faja supermarket"]},
    {"slug": "prime-mart", "name": "Prime Mart Superstore", "aliases": ["prime mart superstore", "prime mart superstores", "primemart superstore", "primemart superstores"]},
    {"slug": "supersaver", "name": "Supersaver", "aliases": ["supersaver supermarket", "supersaver"]},
    {"slug": "blenco", "name": "Blenco", "aliases": ["blenco supermarket", "blenco shoppers center", "blenco shoppers centre", "blenco"]},
    {"slug": "renees", "name": "Renees Supermarket", "aliases": ["renees supermarket", "renee supermarket", "reneees superstores", "renees supermaket", "renees"]},
    {"slug": "am-to-pm", "name": "Am to Pm Supermarket", "aliases": ["am to pm supermarket", "am to pm supermarke"]},
    {"slug": "home-affairs", "name": "Home Affairs Supermarket", "aliases": ["home affairs supermarket"]},
    {"slug": "reno", "name": "Reno Supermarket", "aliases": ["reno supermarket"]},
    {"slug": "spar", "name": "Spar", "aliases": ["spar"]},
    {"slug": "market-square", "name": "Sundry Market Square", "aliases": ["sundry market square"]},
    {"slug": "buymore", "name": "Buymore Supermarket", "aliases": ["buymore supermarket"]},
    {"slug": "de-royal-prince", "name": "De Royal Prince", "aliases": ["de royal prince"]},
    {"slug": "globus", "name": "Globus Supermarket", "aliases": ["globus supermarket"]},
    {"slug": "prestige", "name": "Prestige Superstore", "aliases": ["prestige superstore"]},
    {"slug": "enej", "name": "Enej Supermarket", "aliases": ["enej supermarket"]},
    {"slug": "hutoos", "name": "Hutoos Supermarket", "aliases": ["hutoos supermarket"]},
    {"slug": "mattoris", "name": "Mattoris Supermarket", "aliases": ["mattoris supermarket"]},
    {"slug": "ozzy", "name": "Ozzy Supermarket", "aliases": ["ozzy supermarket", "ozzy superstore"]},
]


def normalize_retailer_group_key(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[.;()]", " ", text)
    text = re.sub(r"\bsupermaket\b", "supermarket", text)
    text = re.sub(r"\bsupermarkt\b", "supermarket", text)
    text = re.sub(r"\bsuperstores\b", "superstore", text)
    text = re.sub(r"\bcentre\b", "center", text)
    text = re.sub(r"\bebano\b", "ebeano", text)
    text = re.sub(r"\breneees\b", "renees", text)
    text = re.sub(r"\brenee\b", "renees", text)
    text = re.sub(r"\bjakhonde\b", "jakande", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


_GROUP_BY_SLUG = {row["slug"]: row for row in RETAILER_GROUP_DEFINITIONS}
_ALIASES_TO_GROUP: dict[str, dict[str, Any]] = {}
for definition in RETAILER_GROUP_DEFINITIONS:
    for alias in definition["aliases"]:
        _ALIASES_TO_GROUP[normalize_retailer_group_key(alias)] = definition


def retailer_group_for_name(retailer_name: str) -> dict[str, Any] | None:
    retailer_name = str(retailer_name or "").strip()
    if not retailer_name:
        return None
    base = retailer_name.split(",")[0].strip()
    normalized = normalize_retailer_group_key(base)
    direct = _ALIASES_TO_GROUP.get(normalized)
    if direct:
        return direct
    for alias_key, definition in _ALIASES_TO_GROUP.items():
        if normalized.startswith(alias_key):
            return definition
    return None


def retailer_group_slug(retailer_name: str) -> str | None:
    match = retailer_group_for_name(retailer_name)
    return match["slug"] if match else None


def retailer_group_name(retailer_name: str) -> str | None:
    match = retailer_group_for_name(retailer_name)
    return match["name"] if match else None


def retailer_group_definition(group_slug: str) -> dict[str, Any] | None:
    return _GROUP_BY_SLUG.get(str(group_slug or "").strip())


def retailer_group_choices() -> list[dict[str, str]]:
    return [{"slug": row["slug"], "name": row["name"]} for row in RETAILER_GROUP_DEFINITIONS]

