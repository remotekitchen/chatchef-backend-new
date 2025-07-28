import requests
import math
import hashlib
from django.core.cache import cache
from datetime import timedelta
from django.db.models import Prefetch, Subquery, OuterRef, Count, Q, Exists
from chatchef.settings.defaults import mapbox_api_key
from rest_framework.response import Response
from hotel.utils.helpers import get_date_range, haversine
import logging
from hotel.models import Hotel, NearbyPlace
from django.core.management.base import BaseCommand
logger = logging.getLogger(__name__)
def get_lat_lon_from_city(city_name: str, api_key: str):
    try:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{city_name}.json?access_token={api_key}"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        features = data.get('features')
        if not features:
            return None
        coordinates = features[0]['geometry']['coordinates']
        return float(coordinates[1]), float(coordinates[0])  # (lat, lon)
    except (requests.RequestException, ValueError, KeyError):
        return None

