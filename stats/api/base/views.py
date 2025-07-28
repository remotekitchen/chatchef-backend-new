import datetime

from django.db.models import F, Sum, Count
from django.db.models.functions import ExtractMonth, ExtractYear
from django.utils import timezone
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from billing.models import Order


class BaseSaleListAPIView(APIView):
    # serializer_class = BaseHourlyOrderSerializer
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = self.request.query_params
        time_period_type = query.get('time_period_type', 'daily')
        time_period = query.get('time_period', "30")
        time_period = int(time_period)
        queryset = {}
        if time_period_type == 'daily':
            start_date = timezone.now() - datetime.timedelta(days=time_period)
            queryset = Order.objects \
                .filter(company=request.user.company, status=Order.StatusChoices.COMPLETED,
                        receive_date__date__gt=start_date) \
                .annotate(date=F('receive_date__date')) \
                .values('date') \
                .annotate(total_sale=Sum('subtotal'), total_volume=Sum('quantity'), cnt=Count('id'))
        if time_period_type == 'monthly':
            start_date = timezone.now() - datetime.timedelta(days=time_period * 30)
            queryset = Order.objects \
                .filter(company=request.user.company, status=Order.StatusChoices.COMPLETED,
                        receive_date__date__gt=start_date) \
                .annotate(month=ExtractMonth('receive_date'),
                        year=ExtractYear('receive_date'),) \
                .values('month', 'year') \
                .annotate(total_sale=Sum('subtotal'), total_volume=Sum('quantity'), cnt=Count('id'))
        return Response(queryset)
