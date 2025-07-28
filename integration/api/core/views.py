import datetime

import environ
import jwt
from django.shortcuts import render
from rest_framework import status, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from billing.api.base.serializers import BaseOrderSerializer
from billing.api.base.views import BaseCostCalculationAPIView
from billing.models import Order
from core.api.paginations import (StandardExternalResultsSetPagination,
                                  StandardResultsSetPagination)
from core.api.permissions import HasPlatformAccess, HasRestaurantPerformAccess
from food.models import Location, Menu, MenuItem, Restaurant
from integration.api.core.serializers import (
    BaseExternalItemSerializer, BaseExternalMenuSerializer,
    BasePlatformSerializer, ExternalOrderPaymentDetailsSerializer,
    ExternalOrderSerializer, IntegrationTokenSerializer, OnboardingSerializer)
from integration.models import Onboarding, Platform

env = environ.Env()

OAUTH_KEY = env.str('CHATCHEFS_EXTERNAL_KEY')


def allow_application(request):
    context = {}
    client_id = request.GET.get('client_id')
    client_secret = request.GET.get('client_secret')
    user = request.user

    application = Platform.objects.get(
        client_id=client_id, client_secret=client_secret) if Platform.objects.filter(
        client_id=client_id, client_secret=client_secret).exists() else None

    context['application'] = application
    context['connecting'] = True
    request.session['app'] = application.id

    return render(request, 'public/connect.html', context)


def stores(request):
    context = {}
    stores = Restaurant.objects.filter(owner=request.user)
    context['stores'] = stores
    return render(request, 'public/connect.html', context)


def connect_with_app(request, pk):
    context = {}
    app = request.session.get('app')
    application = Platform.objects.get(id=app)
    restaurant = Restaurant.objects.get(id=pk)
    if Onboarding.objects.filter(client=application, onboarding=restaurant).exists():
        onboarding = Onboarding.objects.get(
            client=application, onboarding=restaurant)
    else:

        onboarding = Onboarding.objects.create(
            client=application, onboarding=restaurant, status='active')
    context['onboarding'] = onboarding
    context['application'] = application
    context['restaurant'] = restaurant
    return render(request, 'public/connect.html', context)


class BasePlatformReadOnlyModelView(viewsets.ReadOnlyModelViewSet):
    permission_classes = [IsAuthenticated]
    serializer_class = BasePlatformSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        return Platform.objects.filter(mode='production')


class BaseConnections(APIView):
    def post(self, request):
        sr = OnboardingSerializer(data=request.data)
        if sr.is_valid(raise_exception=True):
            client_id = sr.data['client_id']
            restaurant_id = sr.data['restaurant_id']
            location_id = sr.data['location_id']
            onboarding_status = sr.data['status']

            print(sr.data)
            print('-------------------- 01')

            try:
                if not Platform.objects.filter(id=client_id).exists() or not Restaurant.objects.filter(id=restaurant_id).exists() or not Location.objects.filter(id=location_id).exists():
                    return Response({'error': 'invalid details'}, status=status.HTTP_400_BAD_REQUEST)

                print('-------------------- 02')

                platform = Platform.objects.get(id=client_id)
                restaurant = Restaurant.objects.get(id=restaurant_id)
                location = Location.objects.get(id=location_id)

                print(platform)
                print(restaurant)
                print(location)

                print('-------------------- 03')
                if onboarding_status == 'connect':
                    print('connecting')
                    if Onboarding.objects.filter(client=platform, onboarding=restaurant, locations=location).exists():
                        onboarding = Onboarding.objects.get(
                            client=platform, onboarding=restaurant, locations=location)
                        print('-------------------- 04')
                        return Response({'message': 'store already connected', 'key': onboarding.store_key}, status=status.HTTP_200_OK)
                    else:
                        onboarding = Onboarding.objects.create(
                            client=platform, onboarding=restaurant, locations=location, status='active')
                        print('-------------------- 05')
                        return Response({'message': 'store connected', 'key': onboarding.store_key}, status=status.HTTP_202_ACCEPTED)
                elif onboarding_status == 'disconnect':
                    print('disconnecting')
                    if not Onboarding.objects.filter(
                            client=platform, onboarding=restaurant, locations=location).exists():
                        return Response({'message': 'store disconnected'}, status=status.HTTP_202_ACCEPTED)

                    onboarding = Onboarding.objects.get(
                        client=platform, onboarding=restaurant, locations=location)
                    onboarding.delete()
                    return Response({'message': 'store disconnected'}, status=status.HTTP_202_ACCEPTED)
            except Exception as error:
                return Response({'error': error}, status=status.HTTP_400_BAD_REQUEST)


class BaseIntegrationTokenView(APIView):
    def post(self, request):
        sr = IntegrationTokenSerializer(data=request.data)
        if sr.is_valid(raise_exception=True):
            client_id = sr.data['client_id']
            client_secret = sr.data['client_secret']
            scope = sr.data['scope']
            if Platform.objects.filter(client_id=client_id, client_secret=client_secret).exists():
                client = Platform.objects.get(
                    client_id=client_id, client_secret=client_secret)
                expiration_time = datetime.datetime.utcnow() + datetime.timedelta(days=30)
                payload = {'exp': expiration_time, 'scope': scope,
                           'client_id': client_id, 'client_secret': client_secret}
                token = jwt.encode(payload, OAUTH_KEY, algorithm='HS256')
                client.token = token
                client.save()
                context = {
                    'message': "token generated successfully",
                    'token': token,
                    'tokenType': 'Bearer',
                    'expiresIn': expiration_time
                }
                return Response(context, status=status.HTTP_202_ACCEPTED)

            return Response({'message': 'Invalid authorization'}, status=status.HTTP_403_FORBIDDEN)


class BaseMenuSender(APIView):
    permission_classes = [HasPlatformAccess, HasRestaurantPerformAccess]

    def get(self, request):
        store_key = request.headers.get('onboarding')
        if not Onboarding.objects.filter(store_key=store_key).exists():
            return Response({'error': 'invalid onboarding details!'})

        restaurant = Onboarding.objects.get(store_key=store_key).onboarding.id
        menus = Menu.objects.filter(restaurant=restaurant)
        menuData = BaseExternalMenuSerializer(menus, many=True).data

        context = {
            'message': 'Please configure all menu details in your system',
            "storeId": f"{store_key}",
            'payload': {"menus": menuData, }
        }
        return Response(context)


class BaseExternalOrderCreateListView(APIView):
    permission_classes = [HasPlatformAccess, HasRestaurantPerformAccess]
    pagination_class = StandardExternalResultsSetPagination

    def get(self, request):
        paginator = self.pagination_class()
        orders = Order.objects.filter(
            restaurant=request.onboarding.onboarding.id, location=request.onboarding.locations.id)
        result_page = paginator.paginate_queryset(orders, request)

        sr = ExternalOrderSerializer(result_page, many=True)
        return paginator.get_paginated_response(sr.data)

    def post(self, request):
        sr = ExternalOrderSerializer(data=request.data)
        if sr.is_valid(raise_exception=True):
            serialized_data = sr.data
            serialized_data['restaurant'] = request.onboarding.onboarding.id
            serialized_data['company'] = request.onboarding.onboarding.company
            serialized_data['location'] = request.onboarding.locations.id
            serialized_data['order_type'] = 'external'
            serialized_data['external_platform'] = request.platform.id

            order_serializer = BaseOrderSerializer(data=serialized_data)
            order_serializer.is_valid(raise_exception=True)
            order = order_serializer.save()
            order.is_paid = False
            order.save()

            return Response({'message': 'Action completed successfully',
                             'order': {
                                 'id': f'{order.order_id}',
                                 'store': f'{request.store_key}',
                                 'platform': f'{order.external_platform.name}',
                                 'is_paid': f'{order.is_paid}',
                                 'next_step': 'Please update the payment details to mark the order as complete.'
                             }
                             })

    def put(self, request, pk):
        order = Order.objects.get(order_id=pk)
        sr = ExternalOrderSerializer(order, data=request.data)
        if sr.is_valid(raise_exception=True):
            sr.save()
            return Response({'message': 'Action completed successfully'})

    def patch(self, request, pk):
        if not Order.objects.filter(order_id=pk).exists():
            return Response({'message': 'Invalid Action  ID'})
        order = Order.objects.get(order_id=pk)
        sr = ExternalOrderPaymentDetailsSerializer(data=request.data)
        if sr.is_valid(raise_exception=True):
            payment_info = sr.save()
            if sr.data['processingStatus'] == 'PROCESSED':
                order.is_paid = True
                order.save()
                payment_info.order = order
                payment_info.save()
                return Response({'message': 'Action completed successfully', 'order': {
                                 'id': f'{order.order_id}',
                                 'store': f'{request.store_key}',
                                 'platform': f'{order.external_platform.name}',
                                 'is_paid': f'{order.is_paid}',
                                 'next_step': 'Please update order status when order is completed'
                                 }})
        return Response({'message': 'Action completed successfully'})


class BaseExternalCostCalculationView(BaseCostCalculationAPIView):
    permission_classes = [HasPlatformAccess, HasRestaurantPerformAccess]


class BaseOrderStateUpdate(APIView):
    permission_classes = [HasPlatformAccess, HasRestaurantPerformAccess]

    def patch(self, request, pk):
        if not Order.objects.filter(order_id=pk).exists():
            return Response({'message': 'Invalid Action  ID'})
        order = Order.objects.get(order_id=pk)
        sr = ExternalOrderSerializer(order, data=request.data, partial=True)
        print(request.data)
        if sr.is_valid(raise_exception=True):
            if order.is_paid:
                sr.save()
                return Response({'message': 'Action completed successfully', 'order': {
                                 'id': f'{order.order_id}',
                                 'store': f'{request.store_key}',
                                 'platform': f'{order.external_platform.name}',
                                 'is_paid': f'{order.is_paid}',
                                 'order_state': f'{order.status}'
                                 }})

        return Response({'message': 'please update payment information'}, status=status.HTTP_402_PAYMENT_REQUIRED)
