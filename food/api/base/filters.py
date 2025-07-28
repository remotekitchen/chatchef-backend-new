from django_filters import rest_framework as filters

from food.models import Menu


class MenuFilter(filters.FilterSet):
    locations = filters.CharFilter(field_name='locations__slug', lookup_expr='contains')
    # min_price = filters.NumberFilter(field_name="price", lookup_expr='gte')
    # max_price = filters.NumberFilter(field_name="price", lookup_expr='lte')

    class Meta:
        model = Menu
        fields = ['locations']
