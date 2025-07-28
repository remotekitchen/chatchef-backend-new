from rest_framework import serializers

from image_generator.models import ImageUrl


class BaseImageUrlSerializer(serializers.ModelSerializer):
    class Meta:
        model = ImageUrl
        fields = ['dish_name', 'weblink']


class SheetDetailsSerializer(serializers.Serializer):
    url = serializers.URLField()
    sheetName = serializers.CharField(max_length=255)

    def update(self, instance, validated_data):
        pass

    def create(self, validated_data):
        pass


class ExcelURLSerializer(serializers.Serializer):
    SD = SheetDetailsSerializer()
    DishName = serializers.CharField(max_length=255)
    ImageV1 = serializers.URLField()
    ImageV2 = serializers.URLField()
    ImageV3 = serializers.URLField()

    def update(self, instance, validated_data):
        pass

    def create(self, validated_data):
        pass


class CellDataSerializer(serializers.Serializer):
    SD = SheetDetailsSerializer()
    cellNo = serializers.CharField(max_length=255)
    cellSrc = serializers.URLField()

    def update(self, instance, validated_data):
        pass

    def create(self, validated_data):
        pass
