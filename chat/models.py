from django.db import models
from django.utils import timezone

from accounts.models import User
from core.models import BaseModel
from django.utils.translation import gettext_lazy as _


class Message(BaseModel):
    class MessageTypeChoices(models.TextChoices):
        USER_MESSAGE = 'user_message', _('USER MESSAGE'),
        SYSTEM_MESSAGE = 'system_message', _('SYSTEM MESSAGE')

    user = models.ForeignKey(User, verbose_name=_('User'), on_delete=models.CASCADE)
    content = models.TextField(verbose_name=_('Content'), blank=True)
    message_type = models.CharField(max_length=15, verbose_name=_('Message Type'), choices=MessageTypeChoices.choices,
                                    default=MessageTypeChoices.USER_MESSAGE)
    timestamp = models.DateTimeField(verbose_name=_('Timestamp'), default=timezone.now)

    class Meta:
        verbose_name = _('Message')
        verbose_name_plural = _('Messages')
        ordering = ['-id']

    def __str__(self):
        return f'{self.user.email} :: {self.content}'
