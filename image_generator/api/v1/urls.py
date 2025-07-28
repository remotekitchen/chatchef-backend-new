from django.urls import path

from image_generator.api.v1.views import AiImagesAPIView, AiImagesVariationAPIView, GetImagesListAPIView, \
    GetFilteredImageAPIView, GetWebImageUrlAPIView, ChangeBgAPIView, ReadGoogleSheetAPIView, WriteGoogleSheetAPIView, \
    UpdateGoogleSheetAPIView, ImageRecognitionAPIView, MenuImageRecognitionAPIView

app_name = 'image-api-v1'
urlpatterns = [
    path('ai-image/', AiImagesAPIView.as_view(), name='ai-image'),
    path('ai-image-variation/', AiImagesVariationAPIView.as_view(), name='ai-image-variation'),
    path('get-images/<str:text>/', GetImagesListAPIView.as_view(), name='get-images'),
    path('get-filtered-images/', GetFilteredImageAPIView.as_view(), name='get-filtered-images'),
    path('get-web-image-url/', GetWebImageUrlAPIView.as_view(), name='get-web-image-url'),
    path('change-bg/', ChangeBgAPIView.as_view(), name='change-bg'),
    path('read-google-sheet/', ReadGoogleSheetAPIView.as_view(), name='read-google-sheet'),
    path('write-google-sheet/<str:colFrom>/<str:colTo>/', WriteGoogleSheetAPIView.as_view(), name='write-google-sheet'),
    path('update-google-sheet/', UpdateGoogleSheetAPIView.as_view(), name='update-google-sheet'),
    path('image-recognition/', ImageRecognitionAPIView.as_view(), name='image-recognition'),
    path('menu-image-recognition/', MenuImageRecognitionAPIView.as_view(), name='menu-image-recognition')
]
