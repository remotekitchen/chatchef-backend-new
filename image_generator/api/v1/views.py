from image_generator.api.base.views import BaseAiImagesAPIView, BaseAiImagesVariationAPIView, BaseGetImagesListAPIView, \
    BaseGetFilteredImageAPIView, BaseGetWebImageUrlAPIView, BaseChangeBgAPIView, BaseReadGoogleSheetAPIView, \
    BaseWriteGoogleSheetAPIView, BaseUpdateGoogleSheetAPIView, BaseImageRecognitionAPIView, \
    BaseMenuImageRecognitionAPIView
from image_generator.api.v1.serializers import ImageUrlSerializer


class AiImagesAPIView(BaseAiImagesAPIView):
    pass


class AiImagesVariationAPIView(BaseAiImagesVariationAPIView):
    pass


class GetImagesListAPIView(BaseGetImagesListAPIView):
    serializer_class = ImageUrlSerializer


class GetFilteredImageAPIView(BaseGetFilteredImageAPIView):
    pass


class GetWebImageUrlAPIView(BaseGetWebImageUrlAPIView):
    pass


class ChangeBgAPIView(BaseChangeBgAPIView):
    pass


class ReadGoogleSheetAPIView(BaseReadGoogleSheetAPIView):
    pass


class WriteGoogleSheetAPIView(BaseWriteGoogleSheetAPIView):
    pass


class UpdateGoogleSheetAPIView(BaseUpdateGoogleSheetAPIView):
    pass


class ImageRecognitionAPIView(BaseImageRecognitionAPIView):
    pass


class MenuImageRecognitionAPIView(BaseMenuImageRecognitionAPIView):
    pass
