# hotel/cache_utils.py
from django.core.cache import cache

SEARCH_PREFIX = "hotel_search:"

def invalidate_hotel_search_cache():
    """
    Blow away hotel search cache immediately after bookings change availability.
    If django-redis is configured, use delete_pattern. Otherwise fall back to cache.clear().
    """
    try:
        # Works with django-redis
        cache.delete_pattern(f"{SEARCH_PREFIX}*")  # type: ignore[attr-defined]
    except Exception:
        # Dev / LocMem or other backends: nuke all cache (simple + reliable)
        cache.clear()
