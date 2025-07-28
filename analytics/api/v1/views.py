from analytics.api.base.views import (BaseBusinessPerformanceAPIView,
                                      BaseDashboardOrder,
                                      BaseMenuPerformanceAPI,
                                      BaseTrafficMonitorDashboardAPIView,
                                      BaseTrafficMonitorModelView)


class DashboardOrder(BaseDashboardOrder):
    pass


class BusinessPerformanceAPIView(BaseBusinessPerformanceAPIView):
    pass


class MenuPerformanceAPI(BaseMenuPerformanceAPI):
    pass


class TrafficMonitorModelView(BaseTrafficMonitorModelView):
    pass


class TrafficMonitorDashboardAPIView(BaseTrafficMonitorDashboardAPIView):
    pass
