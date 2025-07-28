from rest_framework import serializers

from chat.models import Message


class BaseMessageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Message
        exclude = ['id', 'created_date', 'modified_date']
        extra_kwargs = {
            'user': {
                'required': False
            }
        }
