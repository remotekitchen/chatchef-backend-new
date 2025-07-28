import sys
from .defaults import *

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.sqlite3',
#         'NAME': BASE_DIR / 'db.sqlite3',
#     }
# }
DATABASES = (
    {
        "default": {
            "ENGINE": "django.db.backends.postgresql_psycopg2",
            "NAME": env.str("DB_NAME"),
            "USER": env.str("DB_USER"),
            "PASSWORD": env.str("DB_PASSWORD"),
            "HOST": env.str("DB_HOST"),
            "PORT": env.str("DB_PORT"),
            "TEST": {
                # "ENGINE": "django.db.backends.sqlite3",
                "NAME": f"test_{env.str('DB_NAME')}",
            },
        }
    }
    # if sys.argv[1:2] != ["test"]
    # else {
    #     "default": {
    #         "ENGINE": "django.db.backends.sqlite3",
    #         "NAME": BASE_DIR / "test.sqlite3",
    #     },
    # }
)
DEBUG = True
