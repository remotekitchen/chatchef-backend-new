from analytics.api.base.serializers import BaseVisitorAnalyticsModelSerializer


def create_visitor_analytics(restaurant, location, source="na", count="do", user=None):
    data = {
        "user": user,
        "restaurant": restaurant,
        "location": location,
        "source": source,
        "count": count
    }
    sr = BaseVisitorAnalyticsModelSerializer(data=data)
    sr.is_valid(raise_exception=True)
    sr.save()
    return sr.instance


def get_visitor_analytics_count(queryset, count, source=None):
    return queryset.filter(source=source, count=count).count() if source != None else queryset.filter(count=count).count()
