from core.utils import get_logger
from food.models import Image

logger = get_logger()


def save_item_images(self, serializer):
    files = self.request.FILES.getlist('image_files', [])
    images = []
    for file in files:
        images.append(Image(local_url=file))
    objs = Image.objects.bulk_create(images)
    serializer.instance.images.add(*[obj.id for obj in objs])
    original_image_file = self.request.FILES.getlist(
        'original_image_file', [])
    logger.info(f"{original_image_file}")
    if len(original_image_file) > 0:
        try:
            original_img = Image.objects.create(
                local_url=original_image_file[-1])
            serializer.instance.original_image = original_img
            serializer.instance.save(update_fields=['original_image'])
        except:
            pass
