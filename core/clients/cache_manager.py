from threading import Thread
from django.core.cache import cache


class CacheManager:
    """
    Cache Manager class to manage high level cache operations
    """

    def get_and_refresh(self, key, get_data, **kwargs):
        """
        Get data from cache and refresh it if it's not available and refresh it in background
        params:
            key: str -> cache key name
            get_data: function -> function to get new data
            kwargs: dict -> params to pass to get_data function
        """
        data = cache.get(key)

        if data is None:
            data = get_data(**kwargs)
            cache.set(key, data, 60 * 30)
        else:
            thread = Thread(target=self.set_cache, args=(key, get_data), kwargs=kwargs)
            thread.start()
        return data

    def set_cache(self, key, get_data, **kwargs):
        data = get_data(**kwargs)
        cache.set(key, data, 60 * 30)
