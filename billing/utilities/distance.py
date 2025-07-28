from math import radians, sin, cos, sqrt, asin

def calculate_haversine_distance(lat1, lng1, lat2, lng2):
    """
    Calculates the great-circle distance (in kilometers) between two points.
    """
    try:
        lat1 = float(lat1)
        lng1 = float(lng1)
        lat2 = float(lat2)
        lng2 = float(lng2)

        R = 6371.0

        lat1, lng1, lat2, lng2 = map(radians, [lat1, lng1, lat2, lng2])

        dlat = lat2 - lat1
        dlng = lng2 - lng1

        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlng / 2) ** 2
        c = 2 * asin(sqrt(a))
        distance_km = R * c

        return float("{0:.2f}".format(distance_km))
    except Exception as e:
        print(f"[ERROR] Haversine calculation failed: {e}")
        return None
