from django.urls import include, path

urlpatterns = [
    path("api/v1/", include("remotekitchen.api.v1.urls"))
]
