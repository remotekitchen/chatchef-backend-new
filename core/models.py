import uuid

from django.contrib.sites.models import Site
from django.db import models
from django.utils.text import slugify
from django.utils.translation import gettext as _


class BaseModel(models.Model):
    """
        Base model for inheriting common fields
    """
    created_date = models.DateTimeField(auto_now_add=True)
    modified_date = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Constant(BaseModel):
    key = models.CharField(max_length=255, verbose_name=_('Key'), unique=True)
    value = models.TextField(verbose_name=_('Value'))

    def __str__(self):
        return self.key


class ExtraData(BaseModel):
    label = models.CharField(
        max_length=255, verbose_name=_('Label'), blank=True)
    value = models.TextField(verbose_name=_('Value'), blank=True)
    extra = models.JSONField(verbose_name=_('Extra'), default=dict)

    class Meta:
        verbose_name = _('Extra Data')
        verbose_name_plural = _('Extra Data')

    def __str__(self):
        return self.label


class SluggedModel(BaseModel):
    uid = models.UUIDField(verbose_name=_('Uid'), default=uuid.uuid4, editable=False, unique=True,
                           blank=True)
    slug = models.SlugField(max_length=250, verbose_name=_(
        'Slug'), unique=True, blank=True)
    slug_keyword_field = 'name'

    class Meta:
        abstract = True

    def get_slug(self, keyword, uid):
        base_slug = slugify(keyword)
        truncated_uuid = str(uid)[:8]  # Truncate to first 8 characters
        return f"{base_slug}-{truncated_uuid}"

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = self.get_slug(
                getattr(self, self.slug_keyword_field), self.uid)
            # if update_fields is not None and 'slug' not in update_fields:
            #     update_fields.append('slug')
        super().save(*args, **kwargs)


class BaseAddress(BaseModel):
    country = models.CharField(max_length=40, verbose_name=_('Country'))
    state = models.CharField(
        max_length=40, verbose_name=_('State'), blank=True)
    city = models.CharField(max_length=40, verbose_name=_('City'))
    street_number = models.CharField(
        max_length=40, verbose_name=_('Street Number'), blank=True)
    street_name = models.CharField(
        max_length=40, verbose_name=_('Street Name'), blank=True)
    zip = models.CharField(max_length=20, verbose_name=_('ZIP'), blank=True)

    # additional fields
    address_line = models.CharField(max_length=255, verbose_name=_('Address Line'), blank=True)
    business_name = models.CharField(max_length=40, verbose_name=_('Business Name'), blank=True)
    delivery_instructions = models.TextField(max_length=500, verbose_name=_('Delivery Instructions'),
                                             blank=True)
    label = models.CharField(max_length=50, verbose_name=_('Label'), blank=True)

    class Meta:
        abstract = True


class Address(BaseAddress):
    class Meta:
        verbose_name = _('Address')
        verbose_name_plural = _('Addresses')

    def get(self, attr):
        return getattr(self, attr)


class AppStore(BaseModel):
    class AppType(models.TextChoices):
        OMS = 'oms', _('OMS')
        DO = 'do', _('DO')

    version = models.CharField(
        max_length=30, default='0.0.0', verbose_name=_('App Version'))
    files = models.FileField(upload_to='apps', verbose_name=_('file'))
    type = models.CharField(
        max_length=30, verbose_name=_('apps type'), choices=AppType.choices,
        default=AppType.OMS
    )

    def __str__(self) -> str:
        return f"{self.type} --> {self.version}"

    class Meta:
        verbose_name = _('App Store')
        ordering = ['-id']

    @property
    def url(self):
        try:
            domain = Site.objects.get(name="ROOT").domain
        except:
            domain = "api.chatchefs.com"
        try:
            if self.files and len(self.files) > 0:
                return f"https://{domain}{self.files.url}"
        except:
            pass
        return self.remote_url
