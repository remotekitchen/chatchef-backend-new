import ast
import calendar
import os.path
import time
from datetime import datetime, timedelta

from django.db.models import Count, Q, Sum, Max
from rest_framework import status, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from accounts.api.base.serializers import BaseRestaurantUserGETSerializer
from accounts.models import RestaurantUser
from analytics.api.base.serializers import (
    BaseVisitorAnalyticsModelSerializer, ParseDate)
from analytics.api.base.utils import get_visitor_analytics_count
from analytics.models import VisitorAnalytics
from billing.models import Order, OrderItem
from communication.models import CustomerInfo
from food.models import MenuItem, Restaurant
from marketing.email_sender import send_email
from QR_Code.models import QrCode
from django.utils import timezone



class BaseDashboardOrder(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, pk=None):
        context = {}

        restaurant_id_param = request.query_params.get('restaurant')
        order_method_param = request.query_params.get('order_method')
        date_param = request.query_params.get('date')
        start_param = request.query_params.get('start')
        end_param = request.query_params.get('end')
        hungrytiger_param = request.query_params.get('hungrytiger')
        
        # Determine restaurants based on hungrytiger param
        if hungrytiger_param and hungrytiger_param.lower() == 'true':
            # Get all restaurants where is_remote_kitchen=True
            restaurants = Restaurant.objects.filter(is_remote_Kitchen=True)
        else:
            if restaurant_id_param:
                restaurant_ids = [int(id) for id in restaurant_id_param.split(',')]
                restaurants = Restaurant.objects.filter(id__in=restaurant_ids)
            else:
                restaurants = Restaurant.objects.filter(owner=self.request.user)

        restaurant_id = list(restaurants.values_list('id', flat=True))

        # if restaurant_id is None:
        #     restaurants = Restaurant.objects.filter(owner=self.request.user)
        #     restaurant_id = restaurants.values_list('id', flat=True)

        order_filters = {'restaurant__in': restaurant_id, "is_paid": True}
        exclude_test_order = Q(customer__icontains="test") | Q(
            customer__icontains="anik") | Q(customer__icontains="tim") | Q(customer__icontains="maria") | Q(customer__icontains="humaira")

        rejected_canceled_order = Q(
            status=Order.StatusChoices.CANCELLED) | Q(status=Order.StatusChoices.REJECTED)

        if order_method_param:
            order_filters['order_method'] = order_method_param

        today = datetime.now()
        current_month = today.month
        current_year = today.year
        last_same_day = today - timedelta(days=7)
        last_same_day = last_same_day.date()

        today_time_filters = {'created_date__date': today.date(), }

        # order graph details
        data = []
        current_time = datetime.now()

        query_date_times = []
        if date_param == 'today':
            current_time = current_time.replace(
                hour=23, minute=59, second=0, microsecond=0)
            for i in range(13):
                if current_time not in query_date_times:
                    query_date_times.append(current_time)
                current_time = current_time - timedelta(hours=2)
        elif date_param == 'yesterday':
            yesterday = today - timedelta(days=1)
            yesterday = yesterday.replace(
                hour=23, minute=59, second=0, microsecond=0)
            today_time_filters = {'created_date__date': yesterday.date(), }

            current_time = current_time - timedelta(days=1)
            for i in range(13):
                if current_time not in query_date_times:
                    query_date_times.append(current_time)
                current_time = current_time - timedelta(hours=2)
        elif start_param and end_param:
            sr = ParseDate(data={'start': start_param, 'end': end_param})
            sr.is_valid(raise_exception=True)
            start_date = datetime.strptime(sr.data['start'], "%Y-%m-%d")
            end_date = datetime.strptime(sr.data['end'], "%Y-%m-%d")

            today_time_filters = {
                'created_date__range': (start_date, end_date), }

            date_difference = end_date - start_date
            current_time = end_date
            for i in range(date_difference.days):
                if current_time not in query_date_times:
                    query_date_times.append(current_time)
                current_time = current_time - timedelta(days=1)

        else:
            query_date = today - timedelta(days=int(date_param))
            today_time_filters = {
                'created_date__range': (query_date.date(), today.date()), }
            current_time = current_time - timedelta(days=1)
            for i in range(int(date_param)):
                if current_time not in query_date_times:
                    query_date_times.append(current_time)
                current_time = current_time - timedelta(days=1)

        # Total Customer
        context["total_customer"] = RestaurantUser.objects.filter(
            restaurant__in=restaurant_id, **today_time_filters).count()

        # Today order details
        today_queryset = Order.objects.filter(
            **order_filters, **today_time_filters).exclude(exclude_test_order)
        today_queryset = today_queryset.exclude(rejected_canceled_order)

        context['today_order'] = today_total_order = today_queryset.count()
        context['today_sale'] = today_sale_amount = today_queryset.aggregate(Sum('total'))[
            'total__sum'] or 0

        # This month order details
        this_month_filters = today_time_filters

        this_month_queryset = Order.objects.filter(
            **order_filters, **this_month_filters).exclude(exclude_test_order)
        this_month_queryset = this_month_queryset.exclude(
            rejected_canceled_order)

        context['this_month_order'] = this_month_queryset.count()
        context['this_month_sale'] = this_month_queryset.aggregate(Sum('total'))[
            'total__sum'] or 0

        # compare with last same day
        last_same_day_filters = {
            'created_date': last_same_day, **order_filters}
        last_same_day_queryset = Order.objects.filter(
            **last_same_day_filters).exclude(exclude_test_order)
        last_same_day_sale = round(last_same_day_queryset.aggregate(
            Sum('total'))['total__sum'] or 0, 2)
        last_same_day_order_count = last_same_day_queryset.count()

        context['last_same_day_sale'] = last_same_day_sale
        context['last_day_sale_compare'] = (
            round(((today_sale_amount - last_same_day_sale) /
                  last_same_day_sale) * 100, 2)
            if last_same_day_sale != 0 else 100 if today_sale_amount != 0 else 0
        )
        context['last_same_day_order_compare'] = (
            round(((today_total_order - last_same_day_order_count) /
                  last_same_day_order_count) * 100, 2)
            if last_same_day_order_count != 0 else 100 if today_total_order != 0 else 0
        )

        context['last_same_day_order'] = last_same_day_order_count
        context['last_same_day_index'] = last_same_day.strftime("%d %b %Y %A")

        for i in range(len(query_date_times)):
            if i + 1 < len(query_date_times):
                _filters = {
                    'created_date__range': (
                        query_date_times[i + 1],
                        query_date_times[i]
                    ), **order_filters,
                }

                compare_date_start = query_date_times[i] - \
                    timedelta(days=len(query_date_times))
                compare_date_end = compare_date_start - timedelta(days=1)

                _filters_compare = {
                    'created_date__range': (
                        compare_date_end,
                        compare_date_start
                    ), **order_filters, 'status': 'completed'
                }

                compare_queryset = Order.objects.filter(
                    **_filters_compare).exclude(exclude_test_order)

                queryset = Order.objects.filter(
                    **_filters).exclude(exclude_test_order)
                amount = queryset.exclude(rejected_canceled_order).aggregate(
                    Sum('total'))['total__sum'] or 0

                compare_queryset_amount = compare_queryset.filter(
                    status='completed').aggregate(Sum('total'))['total__sum'] or 0

                order_compare = round((((queryset.filter(
                    status='completed').count() - compare_queryset.count())/compare_queryset.count())*100) or 0, 2) if compare_queryset.count() else 0

                sales_compare = round(
                    ((amount - compare_queryset_amount)/compare_queryset_amount)*100 or 0, 2) if compare_queryset_amount != 0 else 0

                data.append({
                    'start': f"{query_date_times[i + 1]}",
                    'end': f"{query_date_times[i]}",
                    'compare_date': f'{compare_date_start}',
                    'total_orders': queryset.exclude(rejected_canceled_order).count(),
                    'completed_orders': queryset.filter(status='completed').count(),
                    'pending_orders': queryset.filter(status='pending').count(),
                    'cancelled_orders': queryset.filter(rejected_canceled_order).count(),
                    'amount': float(amount),
                    'order_compare': f'{order_compare} %',
                    'sales_compare': f'{sales_compare} %'
                })
        
        
        
        # Combine order_filters + date filters for detailed order list
        order_details_filters = {**order_filters, **today_time_filters}

        order_details_qs = Order.objects.filter(
            **order_details_filters
        ).exclude(exclude_test_order).exclude(rejected_canceled_order)

        order_details = order_details_qs.values(
            'id', 'customer', 'dropoff_phone_number', 'order_method'
        )

        order_details_list = [
            {
                'order_id': order['id'],
                'customer_name': order['customer'],
                'phone_number': order['dropoff_phone_number'],
                'order_type': order['order_method'],
            }
            for order in order_details
]

        context['orderDataCount'] = len(data)
        context['orderData'] = data[::-1]
        context['orderDetails'] = order_details_list
        return Response(context, status=status.HTTP_200_OK)


class BaseBusinessPerformanceAPIView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, pk=None):
        context = {}

        restaurant_id_param = request.query_params.get('restaurant')
        order_method_param = request.query_params.get('order_method')
        date_param = request.query_params.get('date')
        start_param = request.query_params.get('start')
        end_param = request.query_params.get('end')

        restaurant_id = [int(id) for id in restaurant_id_param.split(
            ',')] if restaurant_id_param else None

        if restaurant_id is None:
            restaurants = Restaurant.objects.filter(owner=self.request.user)
            restaurant_id = restaurants.values_list('id', flat=True)

        # if restaurant_id_param:
        #     restaurant_id = [int(id) for id in restaurant_id_param.split(',')]
        # else:
        #     if request.user.is_superuser:
        #         # Superuser defaults to all restaurants
        #         restaurant_id = Restaurant.objects.all().values_list('id', flat=True)
        #     else:
        #         # Normal user defaults to their own
        #         restaurant_id = Restaurant.objects.filter(owner=request.user).values_list('id', flat=True)


        order_filters = {'restaurant__in': restaurant_id, "is_paid": True}
        if order_method_param:
            order_filters['order_method'] = order_method_param

        today = datetime.now()
        current_month = today.month
        current_year = today.year
        last_same_day = today - timedelta(days=7)
        last_same_day = last_same_day.date()
        exclude_test_order = Q(customer__icontains="test") | Q(
            customer__icontains="anik") | Q(customer__icontains="tim") | Q(customer__icontains="maria") | Q(customer__icontains="humaira")
        rejected_canceled_order = Q(
            status="cancelled") | Q(status="rejected")

        __filters = {
            'created_date__month': current_month, 'created_date__year': current_year, }

        data = []
        current_time = datetime.now()

        query_date_times = []
        if date_param == 'today':
            current_time = current_time.replace(
                hour=23, minute=59, second=0, microsecond=0)
            __filters = {'created_date__date': today.date(), }

            for i in range(13):
                if current_time not in query_date_times:
                    query_date_times.append(current_time)
                current_time = current_time - timedelta(hours=2)
        elif date_param == 'yesterday':
            yesterday = today - timedelta(days=1)
            yesterday = yesterday.replace(
                hour=23, minute=59, second=0, microsecond=0)

            __filters = {'created_date__date': yesterday.date(), }

            current_time = current_time - timedelta(days=1)

            for i in range(13):
                if current_time not in query_date_times:
                    query_date_times.append(current_time)
                current_time = current_time - timedelta(hours=2)
        elif start_param and end_param:
            sr = ParseDate(data={'start': start_param, 'end': end_param})
            sr.is_valid(raise_exception=True)
            start_date = datetime.strptime(sr.data['start'], "%Y-%m-%d")
            end_date = datetime.strptime(sr.data['end'], "%Y-%m-%d")

            print("date range sales query")
            __filters = {'created_date__range': (start_date, end_date), }

            date_difference = end_date - start_date
            current_time = end_date
            for i in range(date_difference.days):
                if current_time not in query_date_times:
                    query_date_times.append(current_time)
                current_time = current_time - timedelta(days=1)

        else:
            print("date range sales query")
            query_date = today - timedelta(days=int(date_param))
            __filters = {'created_date__range': (
                query_date.date(), today.date()), }

            current_time = current_time - timedelta(days=1)
            for i in range(int(date_param)):
                if current_time not in query_date_times:
                    query_date_times.append(current_time)
                current_time = current_time - timedelta(days=1)

        # Gross Sales

        sales_report_query = Order.objects.filter(
            **order_filters, **__filters).exclude(exclude_test_order)
        sales_report_query = sales_report_query.exclude(
            rejected_canceled_order)

        context['gross_sales'] = gross_sale = sales_report_query.aggregate(Sum('total'))[
            'total__sum'] or 0

        # net sales
        discount_amount = sales_report_query.aggregate(
            Sum("delivery_fee"))["delivery_fee__sum"] or 0

        tax_amount = sales_report_query.aggregate(Sum("tax"))["tax__sum"] or 0

        stripe_fee_amount = sales_report_query.aggregate(Sum("stripe_fee"))[
            "stripe_fee__sum"] or 0

        tips_for_restaurant_amount = sales_report_query.aggregate(
            Sum("tips_for_restaurant"))["tips_for_restaurant__sum"] or 0

        context["net_sales"] = net_sales = gross_sale - \
            (discount_amount + tax_amount +
             stripe_fee_amount + tips_for_restaurant_amount)

        context['order_volume'] = order_value = sales_report_query.count()

        avg_order_value = 0
        if gross_sale and order_value:
            avg_order_value = gross_sale / order_value

        context["avg_order_value"] = avg_order_value

        for i in range(len(query_date_times)):
            if i + 1 < len(query_date_times):
                _filters = {
                    'created_date__range': (
                        query_date_times[i + 1],
                        query_date_times[i]
                    ), **order_filters,
                }

                compare_date_start = query_date_times[i] - \
                    timedelta(days=len(query_date_times))
                compare_date_end = compare_date_start - timedelta(days=1)

                _filters_compare = {
                    'created_date__range': (
                        compare_date_end,
                        compare_date_start
                    ), **order_filters, 'status': 'completed'
                }

                compare_queryset = Order.objects.filter(
                    **_filters_compare).exclude(exclude_test_order)

                queryset = Order.objects.filter(
                    **_filters).exclude(exclude_test_order)
                amount = queryset.exclude(rejected_canceled_order).aggregate(
                    Sum('total'))['total__sum'] or 0

                compare_queryset_amount = compare_queryset.filter(
                    status='completed').aggregate(Sum('total'))['total__sum'] or 0

                order_compare = round((((queryset.filter(
                    status='completed').count() - compare_queryset.count())/compare_queryset.count())*100) or 0, 2) if compare_queryset.count() else 0

                sales_compare = round(
                    ((amount - compare_queryset_amount)/compare_queryset_amount)*100 or 0, 2) if compare_queryset_amount != 0 else 0

                data.append({
                    'start': f"{query_date_times[i + 1]}",
                    'end': f"{query_date_times[i]}",
                    'compare_date': f'{compare_date_start}',
                    'total_orders': queryset.exclude(rejected_canceled_order).count(),
                    'completed_orders': queryset.filter(status='completed').count(),
                    'pending_orders': queryset.filter(status='pending').count(),
                    'cancelled_orders': queryset.filter(rejected_canceled_order).count(),
                    'amount': float(amount),
                    'order_compare': f'{order_compare} %',
                    'sales_compare': f'{sales_compare} %'
                })

        context['orderDataCount'] = len(data)
        context['orderData'] = data[::-1]
        return Response(context, status=status.HTTP_200_OK)


class BaseMenuPerformanceAPI(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, pk=None):
        context = {}
        items_list = []
        current_time = datetime.now()
        today = current_time.date()
        last_same_day = current_time - timedelta(days=7)
        last_same_day_same_day = last_same_day - timedelta(days=7)
        last_same_day = last_same_day.date()
        last_same_day_same_day = last_same_day_same_day.date()
        restaurants = Restaurant.objects.filter(owner=self.request.user)
        restaurant_id = restaurants.values_list('id', flat=True)

        # this week menu item performance

        order_query_set = Order.objects.filter(restaurant__in=restaurant_id, created_date__range=(
            last_same_day,
            today
        ))
        order_items_id = order_query_set.values_list('id', flat=True)
        query_set = OrderItem.objects.filter(order__in=order_items_id)
        query = query_set.values('menu_item').annotate(
            times_sold=Count('menu_item'), total_quantity=Sum('quantity')
        )
        total_quantity_sum = query.aggregate(
            total_quantity_sum=Sum('total_quantity'))
        total_sold = total_quantity_sum['total_quantity_sum'] or 0
        context['this_week_total_sold'] = total_sold

        # compare menu performance last week
        compare_order_query_set = Order.objects.filter(restaurant__in=restaurant_id, created_date__range=(
            last_same_day_same_day,
            last_same_day
        ))
        compare_order_items_id = compare_order_query_set.values_list(
            'id', flat=True)
        compare_query_set = OrderItem.objects.filter(
            order__in=compare_order_items_id)
        compare_query = compare_query_set.values('menu_item').annotate(
            times_sold=Count('menu_item'), total_quantity=Sum('quantity')
        )
        compare_total_quantity_sum = compare_query.aggregate(
            total_quantity_sum=Sum('total_quantity'))

        compare_sold = compare_total_quantity_sum['total_quantity_sum'] or 0
        context['week_performance'] = f"{ round(((total_sold - compare_sold) / compare_sold)*100 or 0, 2)}"

        for item in query:
            item_details = MenuItem.objects.get(id=item['menu_item'])
            compare_this_week = OrderItem.objects.filter(menu_item=item_details.id, created_date=today).values('menu_item').annotate(
                times_sold=Count('menu_item'), total_quantity=Sum('quantity')
            )

            compare_prev_week = OrderItem.objects.filter(menu_item=item_details.id, created_date=last_same_day).values('menu_item').annotate(
                times_sold=Count('menu_item'), total_quantity=Sum('quantity')
            )

            weekly_growth = 0

            if compare_this_week and compare_prev_week:
                weekly_growth = round(((compare_this_week[0]['total_quantity'] -
                                        compare_prev_week[0]['total_quantity'])/compare_prev_week[0]['total_quantity']) * 100 or 0, 2)
            items_list.append({
                "item_name": item_details.name,
                "item_id": item_details.id,
                "item_price": item_details.base_price,
                "sold_time": item['times_sold'],
                "total_quantity": item['total_quantity'],
                "weekly_growth": f"{weekly_growth} %",
                "this_week_compare": f"{compare_this_week}",
                "this_prev_compare": f"{compare_prev_week}",
                "today": f"{today}",
                "last_same_day": f"{last_same_day}"
            })

        context['menu_performance'] = sorted(
            items_list, key=lambda x: x['total_quantity'], reverse=True)[:10]

        return Response(context, status=status.HTTP_200_OK)


class BaseTrafficMonitorModelView(viewsets.ModelViewSet):
    serializer_class = BaseVisitorAnalyticsModelSerializer
    queryset = VisitorAnalytics.objects.all()



class BaseTrafficMonitorDashboardAPIView(APIView):
    def get(self, request):

        restaurant = request.query_params.get("restaurant", None)
        location = request.query_params.get("location", None)
        start = request.query_params.get("start", None)
        end = request.query_params.get("end", None)
        q_exp = Q(restaurant=restaurant) & Q(
            location__id=location) & Q(created_date__range=(start, end))

        queryset = VisitorAnalytics.objects.filter(q_exp)
        unique_viewers = self.get_unique_viewers(queryset)
        cart = get_visitor_analytics_count(queryset, "cart")
        order_confirm = get_visitor_analytics_count(queryset, "order_confirm")
        payment_confirmation = get_visitor_analytics_count(
            queryset, "payment_confirm")
        do = get_visitor_analytics_count(queryset, "do")
        context = {
            "do": do,
            "cart": cart,
            "order_confirm": order_confirm,
            "payment_confirm": payment_confirmation,
            "unique_viewers": unique_viewers,
            "cart_addition_rate": self.get__rate(total=cart, unique=do),
            "order_confirmation_rate": self.get__rate(total=order_confirm, unique=cart),
            "payment_confirmation_rate": self.get__rate(total=payment_confirmation, unique=order_confirm),
            "view_rate": self.get__rate(total=do, unique=unique_viewers),
            "new_users": self.get_new_users(restaurant, start, end),
            "top_customers": self.get_top_customers(restaurant),
            "new_customers_by_source": self.new_customer_by_source(queryset),
            "repeat_customers": self.repeat_customers(restaurant),
            "churned_customers": self.churned_customers(restaurant) 
        }
        return Response(context)
    
    # users who did not order for last 45 days
    def churned_customers(self, restaurant):
        from django.db.models import Max
        from django.utils import timezone

        # Calculate the threshold date (timezone-aware)
        threshold_date = timezone.now() - timedelta(days=45)

        # Step 1: Find users who placed orders in the last 45 days
        recent_order_users = Order.objects.filter(
            restaurant=restaurant,
            receive_date__gte=threshold_date
        ).values_list('user', flat=True)

        print(list(recent_order_users), "recent_order_users ----------->")
        
        # Step 2: Include all users in the restaurant, annotate last order date
        all_users = RestaurantUser.objects.filter(
            restaurant=restaurant
        ).annotate(
            last_order_date=Max('user__order__receive_date')
        ).values(
            'user__id', 'user__first_name', 'user__last_name', 'user__email', 'user__phone', 'last_order_date'
        )
        
        # Step 3: Filter users whose last order date is before the threshold (or null)
        churned_users = [
            user for user in all_users
            if not user['last_order_date'] or user['last_order_date'] < threshold_date
        ]

        total_count = len(churned_users)

        return {
            "total_count": total_count,
            "churned_users": churned_users
        }
    def repeat_customers(self, restaurant_id):
        # Find users who ordered >=2 times in the last 60 days
        users = Order.objects.filter(
            created_date__range=(datetime.now() - timedelta(days=60), datetime.now())
        ).values('user').annotate(
            total=Count('user')
        ).filter(
            total__gte=2
        ).values_list('user', flat=True)
        
        # Filter by specific restaurant
        repeat_customers_count = RestaurantUser.objects.filter(
            user__in=users,
            restaurant=restaurant_id  # Filter by specific restaurant
        ).count()
        
        return repeat_customers_count
    def get_unique_viewers(self, queryset):
        return queryset.values('user').distinct().count()

    def get__rate(self, total, unique):
         return float("{0:.2f}".format((total / unique) * 100)) if unique else 0

    def get_new_users(self, restaurant, start, end):
        q_exq = Q(restaurant=restaurant) & Q(created_date__range=(start, end))
        return RestaurantUser.objects.filter(q_exq).count()

    def get_top_customers(self, restaurant):
        q_exq = Q(restaurant=restaurant)
        
        # Get all users who have made orders at the given restaurant
        users = Order.objects.filter(q_exq).values('user').distinct().values_list('user', flat=True)
        
        # Get the order counts for each user
        order_count = Order.objects.filter(q_exq).values('user').annotate(
            total=Count('user')).order_by('-total')
        
        # Get the top 3 users based on their order count
        top_users = order_count[:3]

        context = []
        for order in top_users:
            user = RestaurantUser.objects.filter(user=order['user']).first()
            if user:
                context.append({
                    "user": BaseRestaurantUserGETSerializer(user).data,
                    "order_count": order['total']
                })
        
        return context
          
      

    def new_customer_by_source(self, queryset):
        context = {
            "banner": queryset.filter(source="banner").count(),
            "poster": queryset.filter(source="poster").count(),
            "business_card": queryset.filter(source="business_card").count(),
            "flyer": queryset.filter(source="flyer").count(),
            "coupon": queryset.filter(source="coupon").count(),
            "table": queryset.filter(source="table").count(),
            "facebook": queryset.filter(source="facebook").count(),
            "whats_app": queryset.filter(source="whats_app").count(),
            "instagram": queryset.filter(source="instagram").count(),
            "google": queryset.filter(source="google").count(),
        }
        return context
