import requests
from django.core.management.base import BaseCommand
from hotel.models import Hotel, NearbyPlace
from hungrytiger.settings.defaults import mapbox_api_key
from hotel.utils.helpers import haversine

MAPBOX_PLACE_CATEGORIES = [
    "restaurant", "cafe", "bar", "bus_station", "subway",
    "train_station", "supermarket", "shopping_mall",
    "tourist_attraction", "park"
]

class Command(BaseCommand):
    help = "Sync nearby places for all hotels"

    def handle(self, *args, **options):
        for hotel in Hotel.objects.filter(latitude__isnull=False, longitude__isnull=False):
            self.stdout.write(f"Syncing nearby places for hotel: {hotel.name} (ID {hotel.id})")

            nearby_places = []
            for category in MAPBOX_PLACE_CATEGORIES:
                try:
                    url = (
                        f"https://api.mapbox.com/geocoding/v5/mapbox.places/{category}.json"
                        f"?proximity={hotel.longitude},{hotel.latitude}"
                        f"&access_token={mapbox_api_key}&limit=10"
                    )
                    response = requests.get(url, timeout=5)
                    response.raise_for_status()
                    data = response.json()
                    for feature in data.get("features", []):
                        coords = feature["geometry"]["coordinates"]
                        name = feature.get("text")

                        nearby_places.append(NearbyPlace(
                            hotel=hotel,
                            name=name,
                            latitude=coords[1],
                            longitude=coords[0],
                            category=category
                        ))
                except Exception as e:
                    self.stderr.write(f"Failed to fetch {category} for {hotel.name}: {e}")

            # Delete old nearby places for hotel
            hotel.nearby_places.all().delete()

            # Bulk create new nearby places
            NearbyPlace.objects.bulk_create(nearby_places)
            self.stdout.write(f"Added {len(nearby_places)} nearby places for {hotel.name}")
