import django_filters
from hotel.models import Hotel

class HotelFilter(django_filters.FilterSet):
    class Meta:
        model = Hotel
        fields = {
            field.name: ['exact']
            for field in Hotel._meta.fields
            if field.name.startswith("has_")
        }
