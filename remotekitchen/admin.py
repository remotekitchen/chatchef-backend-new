from django.contrib import admin

from remotekitchen.models import Cuisine, SearchKeyword, DeliveryFeeRule
from food.models import Restaurant
admin.site.register(Cuisine)
from billing.models import Order

@admin.register(SearchKeyword)
class SearchKeywordAdmin(admin.ModelAdmin):
    list_display = ('keyword', 'user', 'created_at', 'result_count')
    search_fields = ('keyword',)
    list_filter = ('created_at',)  # Filter by date
    ordering = ('-created_at',)  # Order by latest searches
    
@admin.register(DeliveryFeeRule)
class DeliveryFeeRuleAdmin(admin.ModelAdmin):
    list_display = ('id', 'get_restaurants', 'first_order_fee', 'second_order_fee', 'third_or_more_fee')
    search_fields = ('restaurants__name',)
    filter_horizontal = ('restaurants',)

    def get_restaurants(self, obj):
        return ", ".join([restaurant.name for restaurant in obj.restaurants.all()])
    get_restaurants.short_description = "Restaurants"

    def formfield_for_manytomany(self, db_field, request, **kwargs):
        if db_field.name == "restaurants":
            obj_id = request.resolver_match.kwargs.get('object_id')

            if obj_id:
                # Editing an existing rule
                try:
                    current_rule = DeliveryFeeRule.objects.get(pk=obj_id)
                    current_restaurant_ids = set(current_rule.restaurants.values_list('id', flat=True))
                except DeliveryFeeRule.DoesNotExist:
                    current_restaurant_ids = set()
            else:
                # Adding a new rule
                current_restaurant_ids = set()

            # Restaurants used in other rules
            used_restaurant_ids = set(
                DeliveryFeeRule.objects.exclude(id=obj_id).values_list('restaurants', flat=True)
            )

            # Allow restaurants not used or already selected in current rule
            allowed_ids = (set(Restaurant.objects.values_list('id', flat=True)) - used_restaurant_ids) | current_restaurant_ids

            # Filter the queryset accordingly
            kwargs["queryset"] = Restaurant.objects.filter(id__in=allowed_ids)

        return super().formfield_for_manytomany(db_field, request, **kwargs)


