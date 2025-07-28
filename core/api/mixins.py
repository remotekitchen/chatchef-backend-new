from rest_framework import exceptions
from rest_framework.exceptions import NotFound
from rest_framework.generics import get_object_or_404

from accounts.models import Company
from core.utils import get_logger
from food.models import Restaurant

logger = get_logger()


class UserCompanyListCreateMixin:
    """
        Mixin for inheriting with ListCreateAPIView. Automatically creates object passing user company and returns
        queryset filtering with company
    """
    model_class = None
    request = None
    user_field_name = None

    def get_queryset(self):

        # give permission for superuser
        if self.request.user.is_authenticated and self.request.user.is_superuser:
            # Superuser: return all without filtering by company
            return self.model_class.objects.all()
        
        direct_order = self.request.query_params.get('direct_order', None)
        # If the user is not authenticated or from direct order our queryset is all the menu items,
        # else we are returning only the authenticated user's items
        if direct_order or not self.request.user.is_authenticated:
            return self.model_class.objects.all()
        company = Company.objects.filter(user=self.request.user).first()
        if company is None:
            raise exceptions.NotFound('User has no company!')
        return self.model_class.objects.filter(company=company)

    def perform_create(self, serializer):
        # for superuser
        if self.request.user.is_superuser:
                # Check if company provided in the request data
                company_id = self.request.data.get('company', None)
                if company_id:
                    company = Company.objects.filter(id=company_id).first()
                    if not company:
                        raise exceptions.ValidationError("Invalid company id.")
                    serializer.save(company=company)
                    return
                # If no company provided, assign default company (optional fallback)
                default_company = Company.objects.first()
                if not default_company:
                    raise exceptions.ValidationError("No default company found to assign.")
                serializer.save(company=default_company)
                return
        
        # existing code
        company = Company.objects.filter(user=self.request.user).first()
        """
            Some models might have user field, some might not.
            model_class that have relation to User model should pass the name of user field
        """
        user_kwargs = {}
        if self.user_field_name is not None:
            user_kwargs[self.user_field_name] = self.request.user
        serializer.save(company=company, **user_kwargs)


class MenuRestaurantListCreateMixin:
    model_class = None
    request = None
    user_field_name = None

    def get_queryset(self):
        menu = self.request.query_params.get('menu', None)
        if menu is None:
            raise exceptions.ParseError('menu must be provided in query params!')
        return self.model_class.objects.filter(menu_id=menu)

    # def perform_create(self, serializer):
    #     menu_id = self.request.data.get('menu')
    #     restaurant = get_object_or_404(Restaurant)
    #     """
    #         Some models might have user field, some might not have.
    #         model_class that have relation to User model should pass the name of user field
    #     """
    #     user_kwargs = {}
    #     if self.user_field_name is not None:
    #         user_kwargs[self.user_field_name] = self.request.user
    #     serializer.save(company=company, **user_kwargs)


class GetObjectWithParamMixin:
    query_field = 'id'
    request = None
    model_class = None

    # def check_object_permission(self, request, obj):
    #     pass

    def get_object(self):
        # query = self.request.query_params.get(self.query_field)
        queryset = self.get_queryset()
        obj = self.filter_queryset(queryset).first()
        if obj is None:
            raise NotFound('not found')
        # kwargs = {
        #     self.query_field: query
        # }
        # obj = get_object_or_404(queryset, **kwargs)
        self.check_object_permissions(self.request, obj)
        return obj

    def get_queryset(self):
        return self.model_class.objects.all()
