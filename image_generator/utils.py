import requests
from io import BytesIO
from PIL import Image


async def convert_to_png(image_url: str) -> bytes:
    response = requests.get(image_url)
    response.raise_for_status()

    with Image.open(BytesIO(response.content)) as input_image:
        # Resize the image to make it square
        size = max(input_image.size)
        resized_image = input_image.resize((size, size), Image.ANTIALIAS)

        # Convert the image to RGBA format
        rgba_image = resized_image.convert('RGBA')

        # Create a new blank image with white background
        png_image = Image.new('RGBA', rgba_image.size, (255, 255, 255))

        # Composite the RGBA image onto the blank image
        png_image.paste(rgba_image, (0, 0), rgba_image)

        # Compress the image
        png_image.thumbnail((1024, 1024), Image.ANTIALIAS)

        # Create a BytesIO object to hold the compressed image data
        output_buffer = BytesIO()

        # Save the image to the BytesIO object in PNG format
        png_image.save(output_buffer, format='PNG')

        return output_buffer.getvalue()
