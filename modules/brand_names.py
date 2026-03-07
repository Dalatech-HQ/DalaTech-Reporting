"""
brand_names.py — Canonical brand naming helpers.

Forecasting quality depends on stable brand identities across imports and DB rows.
This module centralizes lightweight normalization for obvious duplicates.
"""

import re


_CANONICAL_MAP = {
    'amanblessed': 'Aman Blessed',
    'atunfoods': 'Atun',
    'atun': 'Atun',
    'augustsecret': 'August Secrets',
    'augustsecrets': 'August Secrets',
    'bboon': 'B-boon',
    'bboonfoods': 'B-boon',
    'bboonfoods': 'B-boon',
    'bboonfoodslimited': 'B-boon',
    'bboonfoodsenterprise': 'B-boon',
    'bboonfoodsenterprises': 'B-boon',
    'biniowan': 'Biniowan',
    'bpbiniowanenterprises': 'Biniowan',
    'cressolife': 'Cresso Life',
    'cressolife': 'Cresso Life',
    'etifarm': 'Eti Farms',
    'etifarms': 'Eti Farms',
    'fabfresh': 'Fabfresh',
    'fabfreshfoods': 'Fabfresh',
    'jkb': 'JKB',
    'jkbfoods': 'JKB',
    'kitchensmith': 'Kitchen Smith',
    'moobi': 'Moobi',
    'qfruits': 'Q-Fruits',
    'qfruit': 'Q-Fruits',
    'qfruitslimited': 'Q-Fruits',
    'sooya': 'Sooya',
    'sooyah': 'Sooya',
    'wholeeats': 'Wholeeats',
    'wilson': 'Wilson',
    'wilsons': 'Wilson',
    'zayith': 'Zayith',
}


def _slug(value: str) -> str:
    return re.sub(r'[^a-z0-9]+', '', value.lower())


def normalize_name_key(value: str) -> str:
    """Return a normalized lookup key for brands, SKUs, and aliases."""
    if value is None:
        return ''
    value = str(value).strip().lower()
    value = value.replace('&', ' and ')
    value = re.sub(r'[^a-z0-9]+', ' ', value)
    value = re.sub(r'\s+', ' ', value).strip()
    return value


def canonicalize_brand_name(name: str) -> str:
    """Return the canonical display name for a brand."""
    if name is None:
        return ''

    value = str(name).strip()
    if not value:
        return ''

    value = value.replace('&', ' and ')
    value = re.sub(r"'[Ss]\b", '', value)
    value = re.sub(r'\s+', ' ', value).strip()

    mapped = _CANONICAL_MAP.get(_slug(value))
    if mapped:
        return mapped

    # Split camel case and normalize spacing for unknown brands.
    value = re.sub(r'([a-z])([A-Z])', r'\1 \2', value)
    value = value.title()
    value = re.sub(r'\s+', ' ', value).strip()
    return value
