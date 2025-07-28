from django.contrib import admin
from django.contrib.admin import display

from .models import *


@admin.register(SpecialHour)
class SpecialHourAdmin(admin.ModelAdmin):
    list_display = ['date', 'opens_at', 'closes_at', 'is_closed']
    search_fields = ['date', 'is_closed']


@admin.register(Restaurant)
class RestaurantAdmin(admin.ModelAdmin):
    list_display = ['name', 'owner','store_type', 'company','display_cuisines',]
    search_fields = ['name','store_type']
    filter_horizontal = ('cuisines',)  # Enables multiple selections
    readonly_fields = [
        'boosted_monthly_sales_count',
        'boosted_total_sales_count',
        'boosted_average_ticket_size',
        'boosted_total_gross_revenue',
    ]
    def display_cuisines(self, obj):
        return ", ".join([c.name for c in obj.cuisines.all()])
    display_cuisines.short_description = "Cuisines"


@admin.register(Location)
class LocationAdmin(admin.ModelAdmin):
    list_display = ['name', 'restaurant']


@admin.register(CuisineType)
class CuisineTypeAdmin(admin.ModelAdmin):
    list_display = ['name']


@admin.register(Menu)
class MenuAdmin(admin.ModelAdmin):
    list_display = ['title', 'restaurant', 'company', 'get_cuisine_types']
    search_fields = ['restaurant__name']

    @display(description='Cuisine Types')
    def get_cuisine_types(self, obj: Menu):
        return ','.join([t.name for t in obj.cuisine_types.all()])


@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    pass


@admin.register(Image)
class ImageAdmin(admin.ModelAdmin):
    pass


@admin.register(MenuItem)
class MenuItemAdmin(admin.ModelAdmin):
    list_display = ['name', 'base_price','discounted_price', 'get_category', 'restaurant','available_start_time','available_end_time']
    filter_horizontal = ['category']
    list_filter = ['category']
    search_fields = ['name', 'description', 'restaurant__name']

    @display(description='Category')
    def get_category(self, obj: MenuItem):
        return ','.join([category.name for category in obj.category.all()])

    def availability_status(self, obj):
        if obj.available_start_time and obj.available_end_time:
            status = "🟢 Available" if obj.is_available else "🔴 Not Available"
            time_window = f"{obj.available_start_time.strftime('%H:%M')}-{obj.available_end_time.strftime('%H:%M')}"
            return f"{status} ({time_window})"
        return "No time restriction"
    availability_status.short_description = "Current Status"

@admin.register(ModifierGroup)
class ModifierGroupAdmin(admin.ModelAdmin):
    list_display = ['name', 'menu']


@admin.register(OpeningHour)
class OpeningHourAdmin(admin.ModelAdmin):
    list_display = ['day_index']


@admin.register(RemoteKitchenCuisine)
class RemoteKitchenCuisineAdmin(admin.ModelAdmin):
    list_display = ('id', 'name')
    search_fields = ('name',)


    # @display(description='Timetable')
    # def timetable(self, obj: OpeningHour):
    #     start_end=obj.timetable_set.values_list('start_time', 'end_time')
    #     return ','.join([category.name for category in obj.category.all()])
admin.site.register(TimeTable)
admin.site.register(POS_DATA)
admin.site.register(RestaurantOMSUsagesTracker)
admin.site.register(ModifierGroupOrder)
admin.site.register(ModifiersItemsOrder)

