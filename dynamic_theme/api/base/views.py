from django.db import transaction

from rest_framework import status
from rest_framework.generics import (ListCreateAPIView, GenericAPIView,
                                     RetrieveUpdateDestroyAPIView)
from rest_framework.response import Response

from core.api.mixins import GetObjectWithParamMixin
from dynamic_theme.models import Theme
from .serializers import ThemeSerializer
from core.api.permissions import HasRestaurantAccess

class BaseThemeListCreateAPIView(ListCreateAPIView):
    queryset = Theme.objects.all()
    serializer_class = ThemeSerializer
    filterset_fields=['restaurant','location']
    permission_classes = [HasRestaurantAccess]


class BaseThemeRetrieveUpdateDestroyAPIView(GetObjectWithParamMixin, 
                                            RetrieveUpdateDestroyAPIView):
    
    model_class=Theme
    serializer_class = ThemeSerializer
    filterset_fields=['id']
    permission_classes = [HasRestaurantAccess]

    def update(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.serializer_class(instance, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)

        if 'is_active' in request.data and request.data['is_active']:
            with transaction.atomic():
                Theme.objects.filter(
                    restaurant=instance.restaurant,
                    location=instance.location
                ).update(is_active=False)
                instance.is_active = True
                serializer.save()
        else:
            serializer.save()

        return Response(serializer.data)

class BaseGetThemeDataAPIView(GenericAPIView):
    serializer_class = ThemeSerializer

    def get(self, request, *args, **kwargs):
        restaurant_slug = self.request.query_params.get('restaurant_slug')
        theme_obj = Theme.objects.filter(restaurant__slug=restaurant_slug).first()
        serializer = self.serializer_class(instance=theme_obj)
        return Response({
            'data': serializer.data,
        }, status=status.HTTP_200_OK)

