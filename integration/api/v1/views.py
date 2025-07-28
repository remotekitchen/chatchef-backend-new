from integration.api.core.views import (BaseConnections,
                                        BaseExternalCostCalculationView,
                                        BaseExternalOrderCreateListView,
                                        BaseIntegrationTokenView,
                                        BaseMenuSender, BaseOrderStateUpdate,
                                        BasePlatformReadOnlyModelView)


class IntegrationTokenView(BaseIntegrationTokenView):
    pass


class MenuSender(BaseMenuSender):
    pass


class PlatformReadOnlyModelView(BasePlatformReadOnlyModelView):
    pass


class Connections(BaseConnections):
    pass


class ExternalOrderCreateListView(BaseExternalOrderCreateListView):
    pass


class OrderStateUpdate(BaseOrderStateUpdate):
    pass


class ExternalCostCalculationView(BaseExternalCostCalculationView):
    pass
