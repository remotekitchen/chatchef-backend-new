from django.utils.timezone import now
import pytz

def get_dynamic_timezone_time(restaurant):
    try:
        restaurant_time_zone = restaurant.timezone
        if not restaurant_time_zone:
            raise ValueError("The restaurant does not have a time zone configured.")

        print(restaurant_time_zone, '------------------> Restaurant time zone')
        local_time_zone = pytz.timezone(restaurant_time_zone)  # Validate and get the timezone
        current_time = now()  # Get the current time in UTC
        print(current_time, '------------------> Current UTC time')
        return current_time.astimezone(local_time_zone)

    except pytz.UnknownTimeZoneError:
        print(f"Unknown time zone: {restaurant_time_zone}")
        raise ValueError(f"Invalid time zone '{restaurant_time_zone}' for restaurant.")

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        raise ValueError("An error occurred while determining the restaurant's time zone.")
      