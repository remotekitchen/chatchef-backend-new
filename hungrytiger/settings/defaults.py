import os.path
from pathlib import Path
from firebase_admin import initialize_app, credentials, get_app

import environ

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent.parent

LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

env = environ.Env()
environ.Env.read_env((BASE_DIR / ".env").as_posix())  # reading .env file

ENV_TYPE = env.str("ENV_TYPE")
SECRET_KEY = env.str("SECRET_KEY")



# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.0/howto/deployment/checklist/

ALLOWED_HOSTS = []

# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.sites",
    'storages',
    # installed app
    "rest_framework",
    "django_rest_passwordreset",
    "rest_framework.authtoken",
    "django_filters",
    "allauth",
    "allauth.account",
    "allauth.socialaccount",

    "allauth.socialaccount.providers.google",
    "allauth.socialaccount.providers.facebook",
    "allauth.socialaccount.providers.apple",

    "corsheaders",
    "dj_rest_auth",
    "debug_toolbar",
    "django_celery_results",
    "django_celery_beat",
    "request_tracker",
    "Webhook",
    # custom app
    "accounts",
    "billing",
    "core",
    "food",
    "chat",
    "integration",
    "hotel",
    # 'image_generator',
    "marketing",
    "pos",
    "communication",
    "reward",
    "Event_logger",
    "templates",
    "firebase",
    "QR_Code",
    "dynamic_theme",
    "referral",
    'analytics',
    'remotekitchen',
    'django_ratelimit',
    'fcm_django',
     "django_extensions"

]

SERVICE_ACCOUNT_KEY_PATH = os.path.join(BASE_DIR, 'hungrytiger', 'settings', 'serviceAccountKey.json')
cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
try:
    FIREBASE_APP = get_app() 
except ValueError:
    FIREBASE_APP = initialize_app(cred) 

FCM_DJANGO_SETTINGS = {
    "DEFAULT_FIREBASE_APP": FIREBASE_APP,
    "ONE_DEVICE_PER_USER": False,  # Allow multiple devices per user
    "DELETE_INACTIVE_DEVICES": True,  # Auto-delete devices that fail notifications
    # Other settings...
}

MIDDLEWARE = [
    "debug_toolbar.middleware.DebugToolbarMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    # 'whitenoise.middleware.WhiteNoiseMiddleware',
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    # 'allauth.account.middleware.AccountMiddleware',
]

ROOT_URLCONF = "hungrytiger.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        # BASE_DIR / 'templates'
        "DIRS": [os.path.join(BASE_DIR, "templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "hungrytiger.wsgi.application"

# Database
# https://docs.djangoproject.com/en/4.0/ref/settings/#databases

# Password validation
# https://docs.djangoproject.com/en/4.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]
DJANGO_REST_RESET_PASSWORD_TOKEN_LIFETIME = 1800  # 30 minutes
# Internationalization
# https://docs.djangoproject.com/en/4.0/topics/i18n/

LANGUAGE_CODE = "en-us"
TIME_ZONE = env.str("TIME_ZONE", default="Canada/Pacific")
# TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True

LOG_DIR = BASE_DIR / "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Logger Config
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[{asctime}] {message}",
            "style": "{",
            "datefmt": "%d/%b/%Y %H:%M:%S",
        }
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "file": {
            "level": "INFO",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOGS_DIR / "debug.log",
            "maxBytes": 1024 * 1024 * 100,  # 100 MB
            "backupCount": 5,
            "formatter": "verbose",
        },
    },
    "loggers": {
        "django": {
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False,
        }
    },
    "hotel": {  # or the actual module name where your code lives
        "handlers": ["console", "file"],
        "level": "INFO",
        "propagate": False,
    },
}

# user model
AUTH_USER_MODEL = "accounts.User"

# Default primary key field type
# https://docs.djangoproject.com/en/4.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# DRF configs
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.BasicAuthentication",
        # "rest_framework.authentication.SessionAuthentication",
        "rest_framework.authentication.TokenAuthentication",
    ],
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
        "rest_framework.renderers.BrowsableAPIRenderer",
    ],
    "DEFAULT_VERSIONING_CLASS": "rest_framework.versioning.NamespaceVersioning",

    #   "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    # "PAGE_SIZE": 10,
    
    "DEFAULT_FILTER_BACKENDS": [
        "rest_framework.filters.SearchFilter",
        "core.api.filters.OrderingFilter",
        "django_filters.rest_framework.DjangoFilterBackend",
    ],

  
}

AUTHENTICATION_BACKENDS = [
    # Needed to log-in by username in Django admin, regardless of `allauth`
    "django.contrib.auth.backends.ModelBackend",
    # `allauth` specific authentication methods, such as login by e-mail
    "allauth.account.auth_backends.AuthenticationBackend",
]

# Allauth settings
SOCIALACCOUNT_PROVIDERS = {
    "google": {
        "SCOPE": ["profile", "email", "openid"],
        "METHOD": "oauth2",
        "AUTH_PARAMS": {"access_type": "offline", "auth_type": "reauthenticate"},
    },
    'facebook': {
        'METHOD': 'oauth2',
        'SCOPE': ['email', 'public_profile', 'user_friends'],
        'AUTH_PARAMS': {'auth_type': 'reauthenticate'},
        'FIELDS': [
            'id',
            'email',
            'name',
            'first_name',
            'last_name',
            'verified',
            'locale',
            'timezone',
            'link',
            'gender',
            'updated_time'],
        'EXCHANGE_TOKEN': True,
        'LOCALE_FUNC': lambda request: 'kr_KR',
        'VERIFIED_EMAIL': True,
        'VERSION': 'v2.4'
    },
    "apple": {
        "APP": {
            # Your service identifier.
            "client_id": env.str("APPLE_CLIENT_ID"),
            # The Key ID (visible in the "View Key Details" page).
            "secret": env.str("APPLE_KEYID"),
            # Member ID/App ID Prefix -- you can find it below your name
            # at the top right corner of the page, or it’s your App ID
            # Prefix in your App ID.
            "key": env.str("MEMAPPIDPREFIX"),
            "settings": {
                # The certificate you downloaded when generating the key.
                "certificate_key": env.str("CERTIFICATE_KEY")
            },
        }
    },
}

ACCOUNT_EMAIL_REQUIRED = True
ACCOUNT_UNIQUE_EMAIL = True
ACCOUNT_AUTHENTICATION_METHOD = "email"

ACCOUNT_USER_MODEL_USERNAME_FIELD = None
ACCOUNT_USERNAME_REQUIRED = False
SITE_ID = 1

# CORS CONFIGURATIONS
CORS_ALLOW_ALL_ORIGINS = True
# CORS_ALLOWED_ORIGINS = [
# ]


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.0/howto/static-files/
STATIC_URL = "/static/"
STATIC_ROOT = os.path.join(BASE_DIR, "static")

MEDIA_URL = "/media/"
MEDIA_ROOT = os.path.join(BASE_DIR, "media")
DATA_UPLOAD_MAX_MEMORY_SIZE_MB = 50

STATICFILES_DIRS = [
    os.path.join(BASE_DIR, "static_local"),
]

INTERNAL_IPS = ["127.0.0.1"]
ALLOWED_HOSTS = ["*"]

REDIS_HOST = env.str("REDIS_HOST", default="redis://localhost:6379/")
# CELERY STUFF
CELERY_BROKER_URL = REDIS_HOST
CELERY_RESULT_BACKEND = "django-db"
CELERY_CACHE_BACKEND = "django-cache"
CELERY_ACCEPT_CONTENT = ["application/json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = "UTC"
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"

GOOGLE_CLIENT_SECRET_FILE = env.str("GOOGLE_CLIENT_SECRET_FILE")
OPEN_AI_API_KEY = env.str("OPEN_AI_API_KEY")
GOOGLE_SEARCH_API_KEY = env.str("GOOGLE_SEARCH_API_KEY")
GOOGLE_SEARCH_ENGINE_ID = env.str("GOOGLE_SEARCH_ENGINE_ID")
REMOVE_BG_API_KEY = env.str("REMOVE_BG_API_KEY")


EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"

EMAIL_PORT = 587  # or the appropriate port for your SMTP server
EMAIL_USE_TLS = True  # Use TLS/SSL if required by your SMTP server


EMAIL_HOST = env.str("EMAIL_HOST")
EMAIL_HOST_DEFAULT_USER = env.str("EMAIL_HOST_DEFAULT_USER")
EMAIL_HOST_NO_REPLAY_USER = env.str("EMAIL_HOST_NO_REPLAY_USER")
EMAIL_HOST_PASSWORD = env.str("EMAIL_HOST_PASSWORD")
EMAIL_HOST_USER = env.str("EMAIL_HOST_DEFAULT_USER")
FIREBASE_SERVICE_ACCOUNT_FILE = env.str(
    "FIREBASE_SERVICE_ACCOUNT_FILE", default="")

SENDGRID_API_KEY = env.str("SENDGRID_API_KEY")
DEFAULT_FROM_EMAIL = env.str("DEFAULT_FROM_EMAIL")
DEFAULT_HUNGRY_TIGER_EMAIL = env.str("DEFAULT_HUNGRY_TIGER_EMAIL")


# twilio
TWILIO_ACCOUNT_SID = env.str("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = env.str("TWILIO_AUTH_TOKEN")
TWILIO_FROM_NUMBER = env.str("TWILIO_FROM_NUMBER")
LOGO_PATH = env.str("LOGO_PATH")
LOGO_PATH_HUNGRY = env.str("LOGO_PATH_HUNGRY")
LOGO_PATH_TECHCHEF = env.str("LOGO_PATH_TECHCHEF")

# Caching
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": REDIS_HOST,
        "KEY_PREFIX": "chatchef",
        "TIMEOUT": 60 * 15,  # in seconds: 60 * 15 (15 minutes)
    }
}


DEFAULT_FILE_STORAGE = "storages.backends.s3boto3.S3Boto3Storage"
STATICFILES_STORAGE = "storages.backends.s3boto3.S3StaticStorage"

# # Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'static'

MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'

# STATICFILES_DIRS = [BASE_DIR / 'static_local']

# # DEBUG TOOLBAR
# INTERNAL_IPS = []

AWS_ACCESS_KEY_ID = env.str("S3_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = env.str("S3_SECRET_ACCESS_KEY")
AWS_STORAGE_BUCKET_NAME = env.str("S3_STORAGE_BUCKET_NAME")
AWS_S3_ENDPOINT_URL = env.str("S3_ENDPOINT")
AWS_S3_USE_SSL = True
AWS_DEFAULT_ACL = "public-read"
AWS_QUERYSTRING_AUTH = False



mapbox_api_key = env.str("MAPBOX_KEY")


ASGI_APPLICATION = 'hungrytiger.asgi.application'
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            'hosts': [('localhost', 6379)],  # Make sure Redis is accessible on this host and port
        },
    },
}


FRONTEND_URL = "https://www.hungry-tiger.com/"

# settings.py
# USE_TZ = True
# TIME_ZONE = 'Asia/Dhaka'



# Lark App credentials
LARK_APP_ID = "cli_a8030393dd799010"
LARK_APP_SECRET = "8ZuSlhJWZrXCcyHHOkU3kfHF2BlPGKrY"

# Your base and tables
LARK_BITABLE_BASE_ID = "OGP4b0T04a2QmesErNsuSkRTs4P"
LARK_TABLE_ID_VR_DATA = "tblgkJObnv96uaYr"       # VR data table (source rows)
LARK_TABLE_ID_VR_CONTROL = "tbl1Tlb5xxlrlV4h" 