from django.db import models

# Create your models here.
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from decimal import Decimal
# from django.contrib.gis.db import models as gis_models
from django.conf import settings
from accounts.models import User
from django.utils import timezone

class Hotel(models.Model):
    owner = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="owned_hotels"
    )
    name = models.CharField(max_length=255)
    city = models.CharField(max_length=100)
    address = models.CharField(max_length=255)
    postal_code = models.CharField(max_length=20, blank=True, null=True)
    country = models.CharField(max_length=100)
    
    # Replace location_coordinates with proper latitude and longitude fields
    latitude = models.DecimalField(max_digits=9, decimal_places=6, blank=True, null=True)
    longitude = models.DecimalField(max_digits=9, decimal_places=6, blank=True, null=True)
    # Keep the location_coordinates for backward compatibility if needed
    location_coordinates = models.CharField(max_length=50, blank=True, null=True)
    
    description = models.TextField(blank=True, null=True)

    phone = models.CharField(max_length=20, blank=True, null=True)
    email = models.EmailField(blank=True, null=True)
    website = models.URLField(blank=True, null=True)

    base_price_per_night = models.DecimalField(max_digits=10, decimal_places=2, validators=[MinValueValidator(Decimal('0.01'))])
    currency = models.CharField(max_length=3, default="BDT")
    tax_rate = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal('0.00'))

    review_score = models.DecimalField(max_digits=3, decimal_places=1, validators=[MinValueValidator(0), MaxValueValidator(10)], blank=True, null=True)
    review_count = models.PositiveIntegerField(default=0)

    staff_rating = models.DecimalField(max_digits=3, decimal_places=1, validators=[MinValueValidator(0), MaxValueValidator(10)], blank=True, null=True)
    facilities_rating = models.DecimalField(max_digits=3, decimal_places=1, validators=[MinValueValidator(0), MaxValueValidator(10)], blank=True, null=True)
    cleanliness_rating = models.DecimalField(max_digits=3, decimal_places=1, validators=[MinValueValidator(0), MaxValueValidator(10)], blank=True, null=True)
    comfort_rating = models.DecimalField(max_digits=3, decimal_places=1, validators=[MinValueValidator(0), MaxValueValidator(10)], blank=True, null=True)
    value_for_money_rating = models.DecimalField(max_digits=3, decimal_places=1, validators=[MinValueValidator(0), MaxValueValidator(10)], blank=True, null=True)
    location_rating = models.DecimalField(max_digits=3, decimal_places=1, validators=[MinValueValidator(0), MaxValueValidator(10)], blank=True, null=True)

    is_active = models.BooleanField(default=True)
    prepayment_required = models.BooleanField(default=False)
    is_recommended = models.BooleanField(default=False)
    is_new_listing = models.BooleanField(default=False)
    is_promoted = models.BooleanField(default=False)

    distance_from_center = models.DecimalField(max_digits=5, decimal_places=1, blank=True, null=True)
    
    # Added check-in/check-out times for better user experience
    check_in_from = models.TimeField(blank=True, null=True)
    check_in_until = models.TimeField(blank=True, null=True)
    check_out_from = models.TimeField(blank=True, null=True)
    check_out_until = models.TimeField(blank=True, null=True)
    
    # Added star rating as commonly used in hotel listings
    star_rating = models.DecimalField(max_digits=2, decimal_places=1, validators=[MinValueValidator(1), MaxValueValidator(5)], blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    has_parking = models.BooleanField(default=False)
    has_airport_shuttle = models.BooleanField(default=False)
    has_room_service = models.BooleanField(default=False)
    has_non_smoking_rooms = models.BooleanField(default=False)
    has_family_rooms = models.BooleanField(default=False)
    has_air_conditioning = models.BooleanField(default=False)
    has_dining_area = models.BooleanField(default=False)
    has_fireplace = models.BooleanField(default=False)
    has_balcony = models.BooleanField(default=False)
    has_garden = models.BooleanField(default=False)
    has_private_bathroom = models.BooleanField(default=False)
    has_safety_deposit_box = models.BooleanField(default=False)
    has_tv = models.BooleanField(default=False)
    has_flat_screen_tv = models.BooleanField(default=False)
    has_internet = models.BooleanField(default=False)
    has_free_wifi = models.BooleanField(default=False)
    has_mosquito_net = models.BooleanField(default=False)
    has_fan = models.BooleanField(default=False)
    has_ironing_facilities = models.BooleanField(default=False)
    has_desk = models.BooleanField(default=False)
    has_seating_area = models.BooleanField(default=False)
    has_24h_front_desk = models.BooleanField(default=False)
    has_wake_up_service = models.BooleanField(default=False)
    
    # Added common amenities found in hotels
    has_swimming_pool = models.BooleanField(default=False)
    has_fitness_center = models.BooleanField(default=False)
    has_spa = models.BooleanField(default=False)
    has_restaurant = models.BooleanField(default=False)
    has_bar = models.BooleanField(default=False)
    has_business_center = models.BooleanField(default=False)
    has_conference_facilities = models.BooleanField(default=False)
    has_elevator = models.BooleanField(default=False)
    has_heating = models.BooleanField(default=False)
    has_baggage_storage = models.BooleanField(default=False)

    speaks_english = models.BooleanField(default=False)
    speaks_hindi = models.BooleanField(default=False)
    speaks_bengali = models.BooleanField(default=True)

    is_ground_floor = models.BooleanField(default=False)
    


    class Meta:
        ordering = ['-review_score', 'name']
        indexes = [
            models.Index(fields=['city']),
            models.Index(fields=['name']),  
            models.Index(fields=['review_score']),
            models.Index(fields=['base_price_per_night']),
            # Added index for latitude and longitude fields
            models.Index(fields=['latitude', 'longitude']),
            models.Index(fields=['is_active']),  # Optional
            models.Index(fields=['star_rating']),  
        ]

    def __str__(self):
        return f"{self.name}, {self.city}"
    
    def get_coordinates(self):
        """Returns the latitude and longitude as a tuple if available."""
        if self.latitude is not None and self.longitude is not None:
            return (self.latitude, self.longitude)
        return None
    class Meta:
        unique_together = ('owner', 'name', 'address')


class RoomType(models.Model):
    hotel = models.ForeignKey(Hotel, on_delete=models.CASCADE, related_name='room_types')
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, null=True)
   
    max_adults = models.PositiveSmallIntegerField(default=2)
    max_children = models.PositiveSmallIntegerField(default=5)

 # How many guests this room can hold (e.g., 2, 4, 6).
    max_occupancy = models.PositiveSmallIntegerField(default=2)
    number_of_beds = models.PositiveSmallIntegerField(default=1)
    bed_type = models.CharField(max_length=50, blank=True, null=True)
    room_size = models.DecimalField(max_digits=6, decimal_places=2, blank=True, null=True)
    price_per_night = models.DecimalField(max_digits=10, decimal_places=2, validators=[MinValueValidator(Decimal('0.01'))])
    discount_price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    
    # Added fields for room inventory management
    total_rooms = models.PositiveSmallIntegerField(default=1)  # Total number of this room type
    room_code = models.CharField(max_length=10, blank=True, null=True)  # For internal reference
    

    is_hourly = models.BooleanField(default=False)
    hourly_price = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)

    # Added room amenities
    has_air_conditioning = models.BooleanField(default=False)
    has_private_bathroom = models.BooleanField(default=False)
    has_balcony = models.BooleanField(default=False)
    has_tv = models.BooleanField(default=False)
    has_refrigerator = models.BooleanField(default=False)
    has_toiletries = models.BooleanField(default=False)
    has_towels = models.BooleanField(default=False)
    has_slippers = models.BooleanField(default=False)
    has_clothes_rack = models.BooleanField(default=False)
    
    # Added additional room amenities
    has_safe = models.BooleanField(default=False)
    has_desk = models.BooleanField(default=False)
    has_minibar = models.BooleanField(default=False)
    has_coffee_maker = models.BooleanField(default=False)
    has_bathtub = models.BooleanField(default=False)
    has_hairdryer = models.BooleanField(default=False)
    has_iron = models.BooleanField(default=False)
    has_seating_area = models.BooleanField(default=False)
    has_view = models.BooleanField(default=False)
    view_type = models.CharField(max_length=50, blank=True, null=True)  # e.g., "Sea View", "City View"

    is_refundable = models.BooleanField(default=True)
    refundable_until_days = models.PositiveSmallIntegerField(blank=True, null=True)  # Optional: e.g., 2 days before check-in

    cancellation_policy = models.TextField(blank=True, null=True)
    smoking_allowed = models.BooleanField(default=False)
    min_age_checkin = models.PositiveSmallIntegerField(default=18)

    extra_bed_fee = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    early_checkin_fee = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    late_checkout_fee = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    
    class Meta:
        unique_together = ('hotel', 'name')

    def __str__(self):
        return f"{self.name} at {self.hotel.name}"

    def get_current_price(self):
        return self.discount_price if self.discount_price is not None else self.price_per_night

    def clean(self):
        self.max_occupancy = self.max_adults + self.max_children

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)


class RoomAvailability(models.Model):
    room_type = models.ForeignKey(RoomType, on_delete=models.CASCADE, related_name='availability')
    date = models.DateField()
    available_rooms = models.PositiveSmallIntegerField(default=0)
    price_override = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    
    # Added fields for special pricing scenarios
    is_special_offer = models.BooleanField(default=False)
    special_offer_name = models.CharField(max_length=100, blank=True, null=True)
    min_nights_stay = models.PositiveSmallIntegerField(default=1)

    class Meta:
        unique_together = ('room_type', 'date')
        indexes = [models.Index(fields=['date']), models.Index(fields=['available_rooms']), ]

    def __str__(self):
        return f"{self.room_type} - {self.date} - {self.available_rooms} available"

    def is_low_availability(self):
        return self.available_rooms <= 3
    
    def get_effective_price(self):
        """Returns the effective price for this date, taking into account any overrides."""
        if self.price_override is not None:
            return self.price_override
        return self.room_type.get_current_price()



class RoomHourlyAvailability(models.Model):
    room_type = models.ForeignKey(RoomType, on_delete=models.CASCADE, related_name='hourly_availability')
    date = models.DateField()
    start_time = models.TimeField()   # e.g. 14:00
    end_time = models.TimeField()     # e.g. 15:00
    available_rooms = models.PositiveSmallIntegerField(default=0)

    class Meta:
        unique_together = ('room_type', 'date', 'start_time', 'end_time')
        indexes = [
            models.Index(fields=['date', 'start_time', 'end_time']),
            models.Index(fields=['room_type', 'date']),
        ]

    def __str__(self):
        return f"{self.room_type.name} on {self.date} from {self.start_time} to {self.end_time}: {self.available_rooms} rooms"





class HotelImage(models.Model):
    hotel = models.ForeignKey(Hotel, on_delete=models.CASCADE, related_name='images')
    room_type = models.ForeignKey(RoomType, on_delete=models.SET_NULL, related_name='images', null=True, blank=True)
    image = models.ImageField(upload_to='hotel_images/')
    alt_text = models.CharField(max_length=255, blank=True, null=True)
    is_primary = models.BooleanField(default=False)
    image_type = models.CharField(max_length=50, choices=[
        ('exterior', 'Exterior'),
        ('room', 'Room'),
        ('bathroom', 'Bathroom'),
        ('dining', 'Dining'),
        ('amenities', 'Amenities'),
        ('view', 'View'),
        ('other', 'Other')
    ], default='other')
    
    # Added fields to track image details
    title = models.CharField(max_length=100, blank=True, null=True)
    caption = models.TextField(blank=True, null=True)
    display_order = models.PositiveSmallIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['hotel']),
            models.Index(fields=['room_type']),
        ]
        ordering = ['display_order', 'id']

    def __str__(self):
        base_str = f"Image for {self.hotel.name}"
        if self.room_type:
            return f"{base_str} - {self.room_type.name}"
        return base_str
    
    
# Added new model for hotel policies and rules
class HotelPolicy(models.Model):
    hotel = models.OneToOneField(Hotel, on_delete=models.CASCADE, related_name='policy')
    pets_allowed = models.BooleanField(default=False)
    pets_fee = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    
    children_allowed = models.BooleanField(default=True)
    children_policy = models.TextField(blank=True, null=True)
    
    has_shuttle_service = models.BooleanField(default=False)
    shuttle_service_fee = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    
    has_breakfast = models.BooleanField(default=False)
    breakfast_fee = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    breakfast_included = models.BooleanField(default=False)
    
    extra_person_fee = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    
    general_policy = models.TextField(blank=True, null=True)
    cancellation_policy = models.TextField(blank=True, null=True)
    payment_options = models.TextField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"Policies for {self.hotel.name}"


class RoomImage(models.Model):
    """Dedicated model for room-specific images"""
    room_type = models.ForeignKey(RoomType, on_delete=models.CASCADE, related_name='room_images')
    image = models.ImageField(upload_to='room_images/')
    alt_text = models.CharField(max_length=255, blank=True, null=True)
    title = models.CharField(max_length=100, blank=True, null=True)
    caption = models.TextField(blank=True, null=True)
    is_primary = models.BooleanField(default=False)
    image_type = models.CharField(max_length=50, choices=[
        ('room_view', 'Room View'),
        ('bathroom', 'Bathroom'),
        ('bed', 'Bed'),
        ('amenities', 'Amenities'),
        ('other', 'Other')
    ], default='room_view')
    display_order = models.PositiveSmallIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['room_type']),
        ]
        ordering = ['display_order', 'id']
    
    def __str__(self):
        return f"Image for {self.room_type.name} at {self.room_type.hotel.name}"


class Guest(models.Model):
    """Guest information model"""
    USER_TYPE_CHOICES = [
        ("individual", "Individual"),
        ("corporate", "Corporate"),
    ]
    user_type = models.CharField(
        max_length=20,
        choices=USER_TYPE_CHOICES,
        default="individual"
    )
    first_name = models.CharField(max_length=100)
    last_name = models.CharField(max_length=100)
    email = models.EmailField()
    phone = models.CharField(max_length=20, blank=True, null=True)
    address = models.CharField(max_length=255, blank=True, null=True)
    city = models.CharField(max_length=100, blank=True, null=True)
    state = models.CharField(max_length=100, blank=True, null=True)
    country = models.CharField(max_length=100, blank=True, null=True)
    postal_code = models.CharField(max_length=20, blank=True, null=True)
    nationality = models.CharField(max_length=100, blank=True, null=True)
    passport_number = models.CharField(max_length=50, blank=True, null=True)
    date_of_birth = models.DateField(blank=True, null=True)
    
    special_requests = models.TextField(blank=True, null=True)
    
    # Link to User model if authenticated
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, blank=True, null=True, related_name='guest_profiles')
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.first_name} {self.last_name}"
    
    @property
    def full_name(self):
        return f"{self.first_name} {self.last_name}"



class Booking(models.Model):
    """Booking model for hotel reservations"""
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('confirmed', 'Confirmed'),
        ('cancelled', 'Cancelled'),
        ('completed', 'Completed'),
        ('no_show', 'No Show'),
        ("checked_in", "Checked In"),

    ]
    
    PAYMENT_STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('partial', 'Partially Paid'),
        ('paid', 'Paid'),
        ('refunded', 'Refunded'),
        ('failed', 'Failed'),
    ]
    
    BOOKING_TYPE_CHOICES = [
        ("daily", "Daily"),
        ("hourly", "Hourly"),
    ]

    BOOKING_SOURCE_CHOICES = [
    ('walk_in', 'Walk-In by Staff'),
    ('guest_self_qr', 'Guest Self Check-In'),
    ('online', 'Online/Website'),
    ('phone', 'Phone Booking'),
    ('third_party', 'Third Party'),
]
    
    booking_type = models.CharField(
        max_length=20,
        choices=BOOKING_TYPE_CHOICES,
        default="daily"
    )

    hours = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Only applicable for hourly bookings"
    )
    booking_number = models.CharField(max_length=20, unique=True)
    guest = models.ForeignKey(Guest, on_delete=models.CASCADE, related_name='bookings')
    hotel = models.ForeignKey(Hotel, on_delete=models.CASCADE, related_name='bookings')
    room_type = models.ForeignKey(
        RoomType,
        on_delete=models.PROTECT,
        related_name='bookings',
        null=True,          # allow null for multi-room bookings
        blank=True
    )

    
    check_in_date = models.DateField()
    check_out_date = models.DateField()

    check_in_time = models.TimeField(null=True, blank=True, help_text="Only for hourly bookings")
    check_out_time = models.TimeField(null=True, blank=True, help_text="Only for hourly bookings")

    number_of_guests = models.PositiveSmallIntegerField(default=1)
    number_of_adults = models.PositiveSmallIntegerField(default=1)
    number_of_children = models.PositiveSmallIntegerField(default=0)
    number_of_rooms = models.PositiveSmallIntegerField(default=1)
    
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    payment_status = models.CharField(max_length=20, choices=PAYMENT_STATUS_CHOICES, default='pending')
    
    base_price = models.DecimalField(max_digits=10, decimal_places=2)  # Price per night
    total_nights = models.PositiveSmallIntegerField()
    total_room_price = models.DecimalField(max_digits=10, decimal_places=2)  # Base price * nights * rooms
    
    taxes = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    additional_fees = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    discount_amount = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    total_price = models.DecimalField(max_digits=10, decimal_places=2)  # Final price including taxes and fees
    
    special_requests = models.TextField(blank=True, null=True)
    
    # Payment and channel information
    payment_method = models.CharField(max_length=50, blank=True, null=True)

    booking_source = models.CharField(
    max_length=50,
    choices=BOOKING_SOURCE_CHOICES,
    default='walk_in',
)
  # website, phone, third-party, etc.
    booking_channel = models.CharField(max_length=50, blank=True, null=True)  # e.g., Booking.com, Expedia
    
    confirmation_sent = models.BooleanField(default=False)
    reminder_sent = models.BooleanField(default=False)
    
    cancellation_reason = models.TextField(blank=True, null=True)
    cancellation_date = models.DateTimeField(blank=True, null=True)

    # Booking model additions
    estimated_arrival = models.TimeField(blank=True, null=True)
    crib_request = models.BooleanField(default=False)
    extra_bed_request = models.BooleanField(default=False)

    
   # Add these two new fields 
#    This means out of all the rooms you booked, 2 rooms will have extra beds.
    extra_bed_rooms_count = models.PositiveIntegerField(    
        default=0,
        help_text="Number of rooms with extra beds"
    )
    # This means in each of those 2 rooms, there will be 1 extra bed.
    extra_bed_count_per_room = models.PositiveIntegerField(
        default=0,
        help_text="Number of extra beds per room"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['booking_number']),
            models.Index(fields=['check_in_date']),
            models.Index(fields=['check_out_date']),
            models.Index(fields=['check_in_time']),      # ✅ Add this
            models.Index(fields=['check_out_time']),
            models.Index(fields=['status']),
            models.Index(fields=['hotel']),
            models.Index(fields=['guest']),
             # New composite indexes
            models.Index(fields=['hotel', 'check_in_date', 'check_out_date'], name='idx_hotel_checkin_checkout'),
            models.Index(fields=['guest', 'status'], name='idx_guest_status'),

            # Optional additional indexes if filtered often:
            models.Index(fields=['booking_type'], name='idx_booking_type'),
            models.Index(fields=['payment_status'], name='idx_payment_status'),
        ]
    
    def __str__(self):
        return f"Booking #{self.booking_number} - {self.guest.full_name}"
    
    def save(self, *args, **kwargs):
        # Set total_nights only for daily bookings
        if self.booking_type == "daily" and not self.total_nights and self.check_in_date and self.check_out_date:
            delta = self.check_out_date - self.check_in_date
            self.total_nights = delta.days

        # Set hours to None if not hourly
        if self.booking_type != "hourly":
            self.hours = None

        # Calculate total_room_price
        if self.base_price and self.number_of_rooms:
            if self.booking_type == "daily" and self.total_nights:
                self.total_room_price = self.base_price * self.total_nights * self.number_of_rooms
            elif self.booking_type == "hourly" and self.hours:
                self.total_room_price = self.base_price * self.hours * self.number_of_rooms

        # Calculate total_price
        if not self.total_price:
            self.total_price = (
                self.total_room_price + self.taxes + self.additional_fees - self.discount_amount
            )

        # Generate booking number if not set
        if not self.booking_number:
            import uuid
            import datetime
            year = datetime.datetime.now().strftime('%y')
            hotel_prefix = f"H{self.hotel.id}"
            random_part = str(uuid.uuid4().int)[:6]
            self.booking_number = f"{hotel_prefix}{year}{random_part}"

        super().save(*args, **kwargs)

    
    def is_cancelable(self):
        """Check if booking can be cancelled based on hotel policy"""
        # Implementation depends on cancellation policy
        if self.status in ['completed', 'cancelled', 'no_show']:
            return False
            
        # Example: Can cancel if check-in date is at least 24 hours away
        import datetime
        today = datetime.date.today()
        return (self.check_in_date - today).days >= 1
    
    def get_nights(self):
        """Calculate number of nights"""
        if self.check_in_date and self.check_out_date:
            return (self.check_out_date - self.check_in_date).days
        return 0



class BookingRoomItem(models.Model):
    booking = models.ForeignKey(
        'Booking',  # your existing Booking model
        related_name='room_items',
        on_delete=models.CASCADE
    )
    room_type = models.ForeignKey(
        'RoomType',
        on_delete=models.PROTECT
    )
    number_of_rooms = models.PositiveIntegerField(default=1)

    extra_bed_rooms_count = models.PositiveIntegerField(default=0)
    extra_bed_count_per_room = models.PositiveIntegerField(default=0)

    # Store pricing info at booking time for history integrity
    base_price = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal('0.00'))
    total_price = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal('0.00'))
    extra_bed_fee_total = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal('0.00'))

    def __str__(self):
        return f"{self.number_of_rooms} x {self.room_type.name} for Booking {self.booking_id}"
    
    class Meta:
        indexes = [
            models.Index(fields=['booking']),    # For filtering room items by booking
            models.Index(fields=['room_type']),  # For filtering room items by room type
        ]




class BookingPayment(models.Model):
    """Model to track payments for bookings"""
    PAYMENT_TYPE_CHOICES = [
        ('deposit', 'Deposit'),
        ('full', 'Full Payment'),
        ('partial', 'Partial Payment'),
        ('refund', 'Refund'),
    ]
    
    PAYMENT_METHOD_CHOICES = [
        ('credit_card', 'Credit Card'),
        ('debit_card', 'Debit Card'),
        ('paypal', 'PayPal'),
        ('bank_transfer', 'Bank Transfer'),
        ('cash', 'Cash'),
        ('other', 'Other'),
    ]
    
    PAYMENT_STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('refunded', 'Refunded'),
    ]
    
    booking = models.ForeignKey(Booking, on_delete=models.CASCADE, related_name='payments')
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    payment_type = models.CharField(max_length=20, choices=PAYMENT_TYPE_CHOICES)
    payment_method = models.CharField(max_length=20, choices=PAYMENT_METHOD_CHOICES)
    status = models.CharField(max_length=20, choices=PAYMENT_STATUS_CHOICES, default='pending')
    
    transaction_id = models.CharField(max_length=100, blank=True, null=True)
    payment_date = models.DateTimeField(default=timezone.now)
    
    notes = models.TextField(blank=True, null=True)
    
    processed_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, blank=True, null=True, related_name='processed_payments')
    
    class Meta:
        indexes = [
            models.Index(fields=['booking']),
            models.Index(fields=['status']),
            models.Index(fields=['payment_date']),
        ]
    
    def __str__(self):
        return f"Payment {self.id} for Booking #{self.booking.booking_number} - {self.amount}"
        
    def save(self, *args, **kwargs):
        is_new = self._state.adding  # Check if new object is being created
        old_status = None
        if not is_new:
            try:
                old_status = BookingPayment.objects.get(pk=self.pk).status
            except BookingPayment.DoesNotExist:
                pass

        # Update payment_date if status is completed
        if self.status == 'completed':
            self.payment_date = timezone.now()

        super().save(*args, **kwargs)

        # Always recalculate booking.payment_status based on all payments
        payments = self.booking.payments.all()

        if all(p.status == 'refunded' for p in payments):
            self.booking.payment_status = 'refunded'
        elif any(p.status == 'failed' for p in payments):
            self.booking.payment_status = 'payment_failed'
        elif any(p.status == 'partial' or (p.status == 'completed' and p.payment_type == 'partial') for p in payments):
            self.booking.payment_status = 'partial'
        elif any(p.status == 'completed' and p.payment_type == 'full' for p in payments):
            self.booking.payment_status = 'paid'
        else:
            self.booking.payment_status = 'unpaid'

        self.booking.save(update_fields=["payment_status"])





class HotelReview(models.Model):
    hotel = models.ForeignKey('Hotel', on_delete=models.CASCADE, related_name='reviews')
    
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    booking = models.OneToOneField('Booking', on_delete=models.SET_NULL, null=True, blank=True)
    rating = models.DecimalField(max_digits=3, decimal_places=1, validators=[MinValueValidator(0), MaxValueValidator(10)])
    comment = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    # Optional breakdown categories
    staff = models.DecimalField(max_digits=3, decimal_places=1, null=True, blank=True)
    cleanliness = models.DecimalField(max_digits=3, decimal_places=1, null=True, blank=True)
    location = models.DecimalField(max_digits=3, decimal_places=1, null=True, blank=True)

    def __str__(self):
        return f"Review by {self.user} for {self.hotel.name}"

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)

        # Auto update hotel aggregate rating and count
        reviews = HotelReview.objects.filter(hotel=self.hotel)
        self.hotel.review_count = reviews.count()
        self.hotel.review_score = reviews.aggregate(avg=models.Avg('rating'))['avg'] or 0

        # Optional: update breakdowns if you want
        if reviews.exists():
            self.hotel.staff_rating = reviews.aggregate(avg=models.Avg('staff'))['avg'] or 0
            self.hotel.cleanliness_rating = reviews.aggregate(avg=models.Avg('cleanliness'))['avg'] or 0
            self.hotel.location_rating = reviews.aggregate(avg=models.Avg('location'))['avg'] or 0

        self.hotel.save()



# additional

class HotelTag(models.Model):
    name = models.CharField(max_length=100)
    icon = models.CharField(max_length=100, blank=True, null=True)  # optional icon path or name
    is_flash_deal = models.BooleanField(default=False)
    is_special_deal = models.BooleanField(default=False)
    is_seasonal_deal = models.BooleanField(default=False) 
    display_order = models.PositiveSmallIntegerField(default=0)


    def __str__(self):
        return self.name

class HotelTagAssignment(models.Model):
    hotel = models.ForeignKey('Hotel', on_delete=models.CASCADE, related_name='tag_assignments')
    tag = models.ForeignKey(HotelTag, on_delete=models.CASCADE)

    class Meta:
        unique_together = ('hotel', 'tag')

    def __str__(self):
        return f"{self.hotel.name} - {self.tag.name}"


class NearbyPlace(models.Model):
    hotel = models.ForeignKey("Hotel", on_delete=models.CASCADE, related_name="nearby_places")
    name = models.CharField(max_length=255)
    latitude = models.FloatField(default=0.0)
    longitude = models.FloatField(default=0.0)
    category = models.CharField(max_length=50)
    # distance_m = models.IntegerField(null=True, blank=True)
    # user_rating = models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)  # optional

    class Meta:
        indexes = [
            models.Index(fields=['hotel']),
        ]

    def __str__(self):
        return f"{self.name} ({self.category}) near {self.hotel.name}"

class Coupon(models.Model):
    code = models.CharField(max_length=50, unique=True)
    description = models.TextField(blank=True, null=True)
    discount_amount = models.DecimalField(max_digits=10, decimal_places=2)
    valid_from = models.DateTimeField()
    valid_until = models.DateTimeField()
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"Coupon {self.code}"

class BookingCoupon(models.Model):
    booking = models.ForeignKey('Booking', on_delete=models.CASCADE, related_name="applied_coupons")
    coupon = models.ForeignKey(Coupon, on_delete=models.SET_NULL, null=True)
    discount_amount = models.DecimalField(max_digits=10, decimal_places=2)

    def __str__(self):
        coupon_code = self.coupon.code if self.coupon else "Deleted"
        return f"Coupon {coupon_code} on Booking #{self.booking.booking_number}"
    
    class Meta:
        indexes = [
            models.Index(fields=['coupon']),         # For filtering by coupon
            models.Index(fields=['booking']),        # For filtering by booking
            # If you query often to check if a guest already used a coupon for a hotel,
            # consider adding a composite index covering those joins:
            models.Index(fields=['coupon', 'booking']),
        ]



class SavedHotel(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    hotel = models.ForeignKey('Hotel', on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'hotel')
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.user} saved {self.hotel.name}"


class HotelViewHistory(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    session_id = models.CharField(max_length=100, blank=True, null=True)
    hotel = models.ForeignKey('Hotel', on_delete=models.CASCADE)
    viewed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["user"]),
            models.Index(fields=["session_id"]),
            models.Index(fields=["hotel"]),
        ]
        ordering = ['-viewed_at']
        # Optional but safer
        constraints = [
            models.UniqueConstraint(
                fields=['user', 'hotel', 'session_id'],
                name='unique_view_per_session'
            )
]


    def __str__(self):
        return f"{self.user or self.session_id} viewed {self.hotel.name}"


class SearchSuggestion(models.Model):
    keyword = models.CharField(max_length=100)
    is_default = models.BooleanField(default=False)  # used to show in initial dropdown


class HotelSearchHistory(models.Model):
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    session_id = models.CharField(max_length=100, blank=True, null=True)
    city = models.CharField(max_length=100)
    checkin = models.DateField()
    checkout = models.DateField()
    adults = models.PositiveSmallIntegerField(default=1)
    children = models.PositiveSmallIntegerField(default=0)
    rooms = models.PositiveSmallIntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']
    