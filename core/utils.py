import logging
import random
import string

from django.utils.text import slugify

from .models import Constant


def get_logger(name='django'):
    return logging.getLogger(name)


def get_request_str(request):
    meta = request.META
    return f"{request.method} {request.get_full_path()} {meta.get('SERVER_PROTOCOL')} {meta.get('HTTP_USER_AGENT')}"


def get_debug_str(request, user, errors):
    return (
        f"""
        request: {get_request_str(request)}
        user: {f"{user} ({user.id})" if user else ""}
        data: {request.data}
        errors: {errors}"""
    )


def get_constant(constant):
    key, value = constant
    try:
        constant = Constant.objects.get(key=key)
    except Constant.DoesNotExist:
        constant = Constant.objects.create(key=key, value=value)
    return constant.value


def get_slug(keyword, uid):
    base_slug = slugify(keyword)
    truncated_uuid = str(uid)[:8]  # Truncate to first 8 characters
    return f"{base_slug}-{truncated_uuid}"


def generate_random_string(include_numbers=True, include_alphabets=True, include_punctuations=True,
                           length_range=(8, 16)):
    characters = ''
    if include_numbers:
        characters = string.digits
    if include_alphabets:
        characters += string.ascii_letters
    if include_punctuations:
        characters += string.punctuation
    characters.replace(':', '')
    output = "".join(random.choice(characters) for x in range(random.randint(length_range[0], length_range[1])))
    return output
