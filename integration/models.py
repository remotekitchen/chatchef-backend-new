import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _

from accounts.models import Company
# from billing.models import Order
from core.models import BaseModel
from food.models import Location, MenuItem, Restaurant


class Platform(BaseModel):
    options = (('test', 'test'), ('production', 'production'))
    name = models.CharField(max_length=200, verbose_name=_('Name'))
    client_id = models.UUIDField(verbose_name=_(
        'Client ID'), unique=True, default=uuid.uuid4, editable=False)
    client_secret = models.UUIDField(verbose_name=_(
        'Client secret'), unique=True, default=uuid.uuid4, editable=False)
    token = models.TextField(verbose_name=_('API token'), blank=True)
    logo = models.URLField(max_length=255, verbose_name=_('Logo'), blank=True)
    extra = models.JSONField(verbose_name=_('Extra data'), blank=True)
    mode = models.CharField(max_length=200, verbose_name=_(
        'Mode'), choices=options, default='test')

    class Meta:
        verbose_name = _('Platform')
        verbose_name_plural = _('Platforms')

    def save(self, *args, **kwargs):
        self.key = self.name.lower().replace(' ', '-')
        return super(Platform, self).save(*args, **kwargs)

    def __str__(self):
        return self.name


class Onboarding(BaseModel):
    status = (('pending', 'pending'), ('active', 'active'))
    client = models.ForeignKey(
        Platform, on_delete=models.CASCADE, verbose_name=_("Platform"), blank=True)
    onboarding = models.ForeignKey(
        Restaurant, on_delete=models.CASCADE, verbose_name=_("restaurant"), blank=True)
    locations = models.ForeignKey(
        Location, on_delete=models.CASCADE, verbose_name=_("locations"), blank=True)
    status = models.CharField(max_length=25, choices=status)
    store_key = models.UUIDField(verbose_name=_(
        'store key'), unique=True, default=uuid.uuid4, editable=False)

    def __str__(self) -> str:
        return f"{self.store_key}"


class Credential(BaseModel):
    company = models.ForeignKey(Company, verbose_name=_(
        'Company'), on_delete=models.SET_NULL, null=True)
    platform = models.ForeignKey(Platform, verbose_name=_(
        'Platform'), on_delete=models.SET_NULL, null=True)
    credentials = models.JSONField(verbose_name=_(
        'Credentials'), default=dict, blank=True)
    token = models.TextField(verbose_name=_('Token'), blank=True)

    class Meta:
        verbose_name = _('Credential')
        verbose_name_plural = _('Credentials')

    def __str__(self):
        return f'{self.company.name} :: {self.platform}'


class PlatformMenuItem(BaseModel):
    menu_item = models.ForeignKey(MenuItem, verbose_name=_(
        'Menu Item'), on_delete=models.SET_NULL, null=True)
    platform = models.ManyToManyField(
        Platform, verbose_name=_('Platform'), blank=True)
    name = models.CharField(max_length=255, verbose_name=_('Name'), blank=True)
    base_price = models.FloatField(verbose_name=_('Base price'), blank=True)

    class Meta:
        verbose_name = _('Platform Menu Item')
        verbose_name_plural = _('Platform Menu Items')

    def __str__(self):
        return f'{self.menu_item.name} :: {self.name}'
