from django.utils import timezone

from core.api.mixins import UserCompanyListCreateMixin


class ActivationCampaignListCreateMixin(UserCompanyListCreateMixin):
    def get_queryset(self):
        direct_order = self.request.query_params.get('direct_order', False)
        queryset = super().get_queryset()
        current_time = timezone.now()
        if direct_order:
            queryset = queryset.filter(durations__start_date__lte=current_time, durations__end_date__gte=current_time)
        return queryset
