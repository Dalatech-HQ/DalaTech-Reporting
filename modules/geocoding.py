"""
geocoding.py — Store location geocoding for map visualizations.

Uses Google Maps Geocoding API to convert store names (e.g., "Jendol Supermarket, Ajah")
into latitude/longitude coordinates for map display.

API Key should be set in environment: GOOGLE_MAPS_API_KEY
Free tier: 40,000 requests/month (more than sufficient for our use case)
"""

import os
import json
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import requests

# Cache file for geocoded locations
CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cache', 'geocode_cache.json')
os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)


def _load_cache() -> Dict:
    """Load cached geocoding results."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}


def _save_cache(cache: Dict):
    """Save geocoding cache."""
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def geocode_store(store_name: str, api_key: Optional[str] = None) -> Optional[Tuple[float, float]]:
    """
    Geocode a store name to lat/lng coordinates.
    
    Args:
        store_name: Store name (e.g., "Jendol Supermarket, Ajah")
        api_key: Google Maps API key (falls back to env var)
    
    Returns:
        Tuple of (latitude, longitude) or None if failed
    """
    # Check cache first
    cache = _load_cache()
    if store_name in cache:
        loc = cache[store_name]
        return (loc['lat'], loc['lng'])
    
    # Get API key
    key = api_key or os.environ.get('GOOGLE_MAPS_API_KEY')
    if not key:
        # Return None if no API key - will use placeholder coordinates
        return None
    
    # Add context to improve accuracy (Nigeria focus)
    search_query = f"{store_name}, Lagos, Nigeria"
    
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        'address': search_query,
        'key': key,
        'region': 'ng',  # Bias results to Nigeria
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get('status') == 'OK' and data.get('results'):
            location = data['results'][0]['geometry']['location']
            lat, lng = location['lat'], location['lng']
            
            # Cache result
            cache[store_name] = {'lat': lat, 'lng': lng, 'cached_at': datetime.now().isoformat()}
            _save_cache(cache)
            
            return (lat, lng)
        
    except Exception as e:
        print(f"Geocoding error for '{store_name}': {e}")
    
    return None


def geocode_stores_batch(store_names: List[str], api_key: Optional[str] = None) -> Dict[str, Tuple[float, float]]:
    """
    Geocode multiple stores with rate limiting.
    
    Returns:
        Dict mapping store_name to (lat, lng)
    """
    results = {}
    key = api_key or os.environ.get('GOOGLE_MAPS_API_KEY')
    
    for store_name in store_names:
        coords = geocode_store(store_name, key)
        if coords:
            results[store_name] = coords
        # Rate limit: 50 requests per second (Google's limit)
        time.sleep(0.02)
    
    return results


def get_map_center(coords: List[Tuple[float, float]]) -> Tuple[float, float]:
    """
    Calculate the center point for a map from a list of coordinates.
    """
    if not coords:
        # Default to Lagos, Nigeria
        return (6.5244, 3.3792)
    
    avg_lat = sum(c[0] for c in coords) / len(coords)
    avg_lng = sum(c[1] for c in coords) / len(coords)
    
    return (avg_lat, avg_lng)


def generate_map_data_with_coords(store_data: List[Dict], api_key: Optional[str] = None) -> List[Dict]:
    """
    Enrich store data with geocoded coordinates.
    
    Args:
        store_data: List of store dicts from historical.get_repeat_purchase_map_data()
        api_key: Google Maps API key
    
    Returns:
        Store data with lat/lng added where available
    """
    key = api_key or os.environ.get('GOOGLE_MAPS_API_KEY')
    
    enriched_data = []
    for store in store_data:
        store_copy = store.copy()
        coords = geocode_store(store['store_name'], key)
        if coords:
            store_copy['latitude'] = coords[0]
            store_copy['longitude'] = coords[1]
        enriched_data.append(store_copy)
    
    return enriched_data


def is_geocoding_available() -> bool:
    """Check if Google Maps API key is configured."""
    return bool(os.environ.get('GOOGLE_MAPS_API_KEY'))
