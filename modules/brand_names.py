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
    'bboonfoodslimited': 'B-boon',
    'bboonfoodsenterprise': 'B-boon',
    'bboonfoodsenterprises': 'B-boon',
    'biniowan': 'Biniowan',
    'bpbiniowanenterprises': 'Biniowan',
    'catherinesign': 'Catherine Sign',
    'cevo': 'Cevo Crystal',
    'cevocrystal': 'Cevo Crystal',
    'cevocrystalservice': 'Cevo Crystal',
    'cevocrystalservices': 'Cevo Crystal',
    'cressolife': 'Cresso Life',
    'etifarm': 'Eti Farms',
    'etifarms': 'Eti Farms',
    'fabfresh': 'Fabfresh',
    'fabfreshfoods': 'Fabfresh',
    'jkb': 'JKB',
    'jkbfoods': 'JKB',
    'keziafoods': 'Kezia',
    'kitchensmith': 'Kitchen Smith',
    'mazarafoods': 'Mazara',
    'moobi': 'Moobi',
    'qfruits': 'Q-Fruits',
    'qfruit': 'Q-Fruits',
    'qfruitslimited': 'Q-Fruits',
    'respite': 'Respite Tea',
    'respitetea': 'Respite Tea',
    'skiddofoods': 'Skiddo',
    'sooya': 'Sooyah Bristo',
    'sooyah': 'Sooyah Bristo',
    'sooyahbristo': 'Sooyah Bristo',
    'tosh': 'Tosh Cocodia',
    'toshcocodia': 'Tosh Cocodia',
    'wholeeats': 'Wholeeats',
    'wilson': 'Wilson',
    'wilsons': 'Wilson',
    'madalabeverages': 'Bp Madala Beverages Limited',
    'madalabeverageslimited': 'Bp Madala Beverages Limited',
    'felon': 'Bp Felon Innovation Ltd',
    'feloninnovation': 'Bp Felon Innovation Ltd',
    'feloninnovationltd': 'Bp Felon Innovation Ltd',
    'dizauregi': 'Bp Dizauregi',
    'mamaologi': 'Mama Ologi',
    'bisquate': 'BisQuate',
    'zayith': 'Zayith',
    'zeef': 'Zeef',
    'zeeffood': 'Zeef',
    'zeeffoods': 'Zeef',
}

_GENERIC_BRAND_SUFFIXES = {
    'enterprise', 'enterprises', 'ent', 'services', 'service',
    'foods', 'food', 'ventures', 'venture', 'limited', 'ltd',
    'company', 'co', 'global', 'group', 'industries', 'industry',
    'nigeria', 'ng', 'solutions', 'solution', 'partners', 'trading',
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


def normalize_brand_compare_key(value: str) -> str:
    """Reduce a brand name to its distinguishing stem for duplicate checks."""
    tokens = normalize_name_key(value).split()
    while len(tokens) > 1 and tokens[-1] in _GENERIC_BRAND_SUFFIXES:
        tokens.pop()
    return ' '.join(tokens)


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


def brand_match_terms(name: str) -> list[str]:
    """Return useful brand alias terms for matching survey names and free text."""
    canonical = canonicalize_brand_name(name)
    terms = []
    seen = set()

    def _add(value: str):
        text = str(value or '').strip()
        key = normalize_name_key(text)
        if not text or not key or key in seen:
            return
        seen.add(key)
        terms.append(text)

    _add(canonical)
    compare_key = normalize_brand_compare_key(canonical)
    _add(compare_key)

    # Strip common prefixes so "Bp Madala Beverages Limited" also matches "Madala Beverages Limited Feedback"
    for prefix in ('bp ', 'bp'):
        if canonical.lower().startswith(prefix):
            _add(canonical[len(prefix):].strip())
            break

    canonical_key = normalize_name_key(canonical)
    for raw_key, mapped in _CANONICAL_MAP.items():
        if normalize_name_key(mapped) != canonical_key:
            continue
        pretty = re.sub(r'([a-z])([A-Z])', r'\1 \2', raw_key).strip()
        _add(pretty.title())

    return terms
