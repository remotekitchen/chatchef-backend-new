from threading import Thread

from django.core.cache import cache
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_control, cache_page
from rest_framework.response import Response
from core.api.paginations import StandardResultsSetPagination
from core.clients.cache_manager import CacheManager
from food.api.base.views import (BaseCategoryListCreateAPIView,
                                 BaseMenuItemListCreateAPIView,
                                 BaseMenuListCreateAPIView,
                                 BaseMenuRetrieveUpdateDestroyAPIView,
                                 BaseModifierGroupListCreateAPIView)
from food.api.v2.serializers import (MenuDetailSerializer,
                                     MenuItemGETSerializer, MenuListSerializer)


class MenuListAPIView(BaseMenuListCreateAPIView):
    http_method_names = ["get"]

    def get_serializer_class(self):
        return MenuListSerializer

    # @method_decorator(cache_page(timeout=60 * 30))
    # def get(self, request, *args, **kwargs):
        # query = request.query_params
        # restaurant, location, page, direct_order = (
        #     query.get("restaurant"),
        #     query.get("location"),
        #     query.get("page", 1),
        #     query.get("direct_order", None),
        # )
        # if direct_order is not None:
        #     cache_key = f"menu_list:{restaurant}:{location}:{page}:{direct_order}"
        #     cache_manager = CacheManager()
        #     response = cache_manager.get_and_refresh(
        #         cache_key, self.get_data, request=request
        #     )
        #     return Response(response)
        # return super().get(request, *args, **kwargs)

    # def get_data(self, request):
    #     data = super().get(request).data
    #     return data


class MenuRetrieveAPIView(BaseMenuRetrieveUpdateDestroyAPIView):
    http_method_names = ["get"]

    def get_serializer_class(self):
        return MenuDetailSerializer

    # @method_decorator(cache_page(timeout=60 * 30))
    # @method_decorator(cache_control(public=True, max_age=60 * 30))
    def get(self, request, *args, **kwargs):
        menu_id = request.query_params.get("id")
        rating = request.query_params.get("rating")
        if rating:
            return super().get(request, *args, **kwargs)

        if menu_id is not None:
            cache_key = f"menu_detail:{menu_id}"
            cache_manager = CacheManager()
            response = cache_manager.get_and_refresh(
                cache_key, self.get_data, request=request
            )
            return Response(response)
        return super().get(request, *args, **kwargs)

    def get_data(self, request):
        data = super().get(request).data
        return data


class MenuItemListCreateAPIView(BaseMenuItemListCreateAPIView):
    pagination_class = None
    serializer_class = MenuItemGETSerializer


class CategoryListCreateAPIView(BaseCategoryListCreateAPIView):
    pagination_class = None
    # pagination_class = StandardResultsSetPagination


class ModifierGroupListCreateAPIView(BaseModifierGroupListCreateAPIView):
    pagination_class = None
