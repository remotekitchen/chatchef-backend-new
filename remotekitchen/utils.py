from core.api.paginations import StandardResultsSetPagination
from billing.models import Order
from remotekitchen.models import  DeliveryFeeRule

class StandardRemoteKitchenResultsSetPagination(StandardResultsSetPagination):
    page_size = 50



def get_customer_order_count(user, restaurant):
    return Order.objects.filter(customer=user, restaurant=restaurant, status="completed").count()



def get_delivery_fee_rule(user, restaurant):
    order_count = get_customer_order_count(user, restaurant)

    print("order count",  order_count)

    rule = DeliveryFeeRule.objects.filter(restaurants=restaurant).first()
    print("rule", rule, restaurant)
    if not rule:
        rule = DeliveryFeeRule.objects.filter(restaurants__isnull=True).first()

    if not rule:
        return 0  
    if order_count == 0:
        return rule.first_order_fee
    elif order_count == 1:
        return rule.second_order_fee
    else:
        return rule.third_or_more_fee
