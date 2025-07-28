from django.contrib import admin

# Register your models here.

from django.contrib import admin
from hotel.models import (Hotel, RoomType, RoomAvailability, HotelImage, HotelPolicy, 
                          Guest, Booking, BookingPayment, HotelReview,RoomImage,HotelTag, HotelTagAssignment, NearbyPlace,
                            Coupon, BookingCoupon,
                            SavedHotel,
                            HotelViewHistory,
                            SearchSuggestion,
                            HotelSearchHistory,RoomHourlyAvailability)

@admin.register(Hotel)
class HotelAdmin(admin.ModelAdmin):
    list_display = ("id","name", "city", "country", "review_score", "is_active")
    list_filter = ("city", "is_active", "star_rating")
    search_fields = ("name", "city", "country")

@admin.register(RoomType)
class RoomTypeAdmin(admin.ModelAdmin):
    list_display = ("id","name", "hotel", "price_per_night", "max_occupancy")
    list_filter = ("hotel",)
    search_fields = ("name",)

@admin.register(RoomAvailability)
class RoomAvailabilityAdmin(admin.ModelAdmin):
    list_display = ('id',"room_type", "date", "available_rooms", "price_override")
    list_filter = ("room_type", "date")



@admin.register(RoomHourlyAvailability)
class RoomHourlyAvailabilityAdmin(admin.ModelAdmin):
    list_display = ("room_type", "date", "start_time", "end_time", "available_rooms")
    list_filter = ("room_type", "date")
    search_fields = ("room_type__name",)
    ordering = ("room_type", "date", "start_time")

@admin.register(HotelImage)
class HotelImageAdmin(admin.ModelAdmin):
    list_display = ("hotel", "room_type", "image_type", "is_primary")

@admin.register(RoomImage)
class RoomImageAdmin(admin.ModelAdmin):
    list_display = ("room_type", "image_type", "is_primary", "display_order", "preview")
    list_filter = ("room_type__hotel", "image_type", "is_primary")
    search_fields = ("room_type__name", "room_type__hotel__name", "title", "alt_text")
    readonly_fields = ("created_at", "updated_at", "preview")

    def preview(self, obj):
        if obj.image:
            return f'<img src="{obj.image.url}" width="100" style="object-fit: contain;" />'
        return "-"
    preview.allow_tags = True
    preview.short_description = "Preview"

@admin.register(HotelPolicy)
class HotelPolicyAdmin(admin.ModelAdmin):
    list_display = ("hotel", "pets_allowed", "has_shuttle_service", "has_breakfast")

@admin.register(Guest)
class GuestAdmin(admin.ModelAdmin):
    list_display = ("full_name", "email", "city", "country")
    search_fields = ("first_name", "last_name", "email")

@admin.register(Booking)
class BookingAdmin(admin.ModelAdmin):
    list_display = ("id","booking_number", "guest", "hotel", "status", "payment_status", "check_in_date", "check_out_date")
    list_filter = ("status", "payment_status", "check_in_date")
    search_fields = ("booking_number",)

@admin.register(BookingPayment)
class BookingPaymentAdmin(admin.ModelAdmin):
    list_display = ("booking", "amount", "payment_type", "status", "payment_date")
    list_filter = ("status", "payment_type")

@admin.register(HotelReview)
class HotelReviewAdmin(admin.ModelAdmin):
    list_display = ("id","hotel", "user", "rating", "created_at")
    list_filter = ("rating",)
# END: Admin Registrations




@admin.register(HotelTag)
class HotelTagAdmin(admin.ModelAdmin):
    list_display = ('name', 'is_flash_deal', 'display_order')
    search_fields = ('name',)
    ordering = ('display_order', 'name')


@admin.register(HotelTagAssignment)
class HotelTagAssignmentAdmin(admin.ModelAdmin):
    list_display = ('hotel', 'tag')
    search_fields = ('hotel__name', 'tag__name')
    autocomplete_fields = ('hotel', 'tag')
    list_filter = ('tag',)


@admin.register(NearbyPlace)
class NearbyPlaceAdmin(admin.ModelAdmin):
    list_display = ('name', 'category', 'hotel')
    search_fields = ('name', 'category', 'hotel__name')
    list_filter = ('category',)
    autocomplete_fields = ('hotel',)
    ordering = ('hotel',)


@admin.register(Coupon)
class CouponAdmin(admin.ModelAdmin):
    list_display = ('code', 'discount_amount', 'valid_from', 'valid_until', 'is_active')
    search_fields = ('code',)
    list_filter = ('is_active',)
    date_hierarchy = 'valid_until'


@admin.register(BookingCoupon)
class BookingCouponAdmin(admin.ModelAdmin):
    list_display = ('booking', 'coupon', 'discount_amount')
    search_fields = ('booking__booking_number', 'coupon__code')
    autocomplete_fields = ('booking', 'coupon')


@admin.register(SavedHotel)
class SavedHotelAdmin(admin.ModelAdmin):
    list_display = ('user', 'hotel', 'created_at')
    search_fields = ('user__username', 'hotel__name')
    autocomplete_fields = ('user', 'hotel')
    ordering = ('-created_at',)


@admin.register(HotelViewHistory)
class HotelViewHistoryAdmin(admin.ModelAdmin):
    list_display = ('user', 'session_id', 'hotel', 'viewed_at')
    search_fields = ('user__username', 'session_id', 'hotel__name')
    autocomplete_fields = ('user', 'hotel')
    ordering = ('-viewed_at',)
    list_filter = ('hotel',)


@admin.register(SearchSuggestion)
class SearchSuggestionAdmin(admin.ModelAdmin):
    list_display = ('keyword', 'is_default')
    search_fields = ('keyword',)
    list_filter = ('is_default',)


@admin.register(HotelSearchHistory)
class HotelSearchHistoryAdmin(admin.ModelAdmin):
    list_display = ('user', 'session_id', 'city', 'checkin', 'checkout', 'adults', 'children', 'rooms', 'created_at')
    search_fields = ('user__username', 'session_id', 'city')
    list_filter = ('city', 'checkin', 'checkout')
    autocomplete_fields = ('user',)
    ordering = ('-created_at',)