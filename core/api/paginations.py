from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response


class StandardResultsSetPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'

    def get_paginated_response(self, data):
        page_size_limit = self.request.GET.get('page_size', 10)
        self.page_size = page_size_limit
        return Response({
            'links': {
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            },
            'count': self.page.paginator.count,
            'results': data
        })



class HotelStandardResultsSetPagination(PageNumberPagination):
    page_size = 300
    page_size_query_param = 'page_size'

    def get_paginated_response(self, data):
        page_size_limit = self.request.GET.get('page_size', 300)
        self.page_size = page_size_limit
        return Response({
            'links': {
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            },
            'count': self.page.paginator.count,
            'results': data
        })


class StandardExternalResultsSetPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'

    def get_paginated_response(self, data):
        print('store', )
        page_size_limit = self.request.GET.get('page_size', 10)
        self.page_size = page_size_limit
        return Response({
            'message': 'Action completed successfully',
            'store': f'{self.request.store_key}',
            'platform': f'{self.request.onboarding.client.client_id}',
            'count': self.page.paginator.count,
            'results': data
        })


class CustomPageSizePagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 200

    def get_page_size(self, request):
        """
        This method is called BEFORE slicing the queryset.
        Here you can dynamically decide page size.
        """
        page_size = super().get_page_size(request)
        if page_size is None:
            return self.page_size
        return page_size

    def get_paginated_response(self, data):
        return Response({
            'links': {
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            },
            'count': self.page.paginator.count,
            'results': data
        })