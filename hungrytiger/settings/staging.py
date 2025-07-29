import dj_database_url

from . import env
# noinspection PyUnresolvedReferences
from .defaults import *

DEBUG = True
ALLOWED_HOSTS = ["*"]

# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': env.str('DB_NAME'),
        'USER': env.str('DB_USER'),
        'PASSWORD': env.str('DB_PASSWORD'),
        'HOST': env.str('DB_HOST'),
        'PORT': env.str('DB_PORT'),
        'DISABLE_SERVER_SIDE_CURSORS': True
    }
}
# db_from_env = dj_database_url.config(conn_max_age=600)
# DATABASES['default'].update(db_from_env)
# DEFAULT_FILE_STORAGE = "storages.backends.s3boto3.S3Boto3Storage"
# STATICFILES_STORAGE = "storages.backends.s3boto3.S3StaticStorage"

# # Static files (CSS, JavaScript, Images)
# STATIC_URL = '/static/'
# STATIC_ROOT = BASE_DIR / 'static'

# MEDIA_URL = '/media/'
# MEDIA_ROOT = BASE_DIR / 'media'

# STATICFILES_DIRS = [BASE_DIR / 'static_local']

# DEBUG TOOLBAR
INTERNAL_IPS = []

# AWS_ACCESS_KEY_ID = env.str("S3_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = env.str("S3_SECRET_ACCESS_KEY")
# AWS_STORAGE_BUCKET_NAME = env.str("S3_STORAGE_BUCKET_NAME")
# AWS_S3_ENDPOINT_URL = env.str("S3_ENDPOINT")
# AWS_S3_USE_SSL = True
# AWS_DEFAULT_ACL = "public-read"
# AWS_QUERYSTRING_AUTH = False

# STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'
