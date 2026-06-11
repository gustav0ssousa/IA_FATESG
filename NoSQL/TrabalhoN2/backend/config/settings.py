from pathlib import Path

import environ

BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent

env = environ.Env(
    DJANGO_DEBUG=(bool, False),
    DJANGO_ALLOWED_HOSTS=(list, ["localhost", "127.0.0.1"]),
    DJANGO_ENVIRONMENT=(str, "local"),
    DJANGO_SECURE_SSL_REDIRECT=(bool, False),
    DJANGO_SECURE_COOKIES=(bool, False),
    DATABASE_URL=(str, f"sqlite:///{BASE_DIR / 'db.sqlite3'}"),
    API_ACCESS_KEY=(str, ""),
    API_REQUIRE_AUTHENTICATION=(bool, False),
    API_ANON_THROTTLE_RATE=(str, "120/min"),
    API_USER_THROTTLE_RATE=(str, "600/min"),
    DOCUMENT_MAX_UPLOAD_SIZE=(int, 75 * 1024 * 1024),
    DOCUMENT_STORAGE_DIR=(str, str(BASE_DIR / "media")),
    RAG_CHUNK_SIZE=(int, 1200),
    RAG_CHUNK_OVERLAP=(int, 200),
    EMBEDDING_MODEL=(str, "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"),
    EMBEDDING_DIMENSION=(int, 384),
    EMBEDDING_CACHE_DIR=(str, "/tmp/fastembed_cache"),
    EMBEDDING_THREADS=(int, 1),
    EMBEDDING_ENABLE_CPU_MEM_ARENA=(bool, False),
    EMBEDDING_SERVICE_URL=(str, ""),
    EMBEDDING_SERVICE_ENABLED=(bool, False),
    EMBEDDING_SERVICE_TIMEOUT_SECONDS=(float, 180),
    QDRANT_URL=(str, "http://localhost:6333"),
    QDRANT_COLLECTION=(str, "documents"),
    RAG_TOP_K=(int, 5),
    RAG_MIN_RELEVANCE_SCORE=(float, 0.35),
    RAG_MAX_CONTEXT_CHARS=(int, 12000),
    MARITACA_API_KEY=(str, ""),
    MARITACA_BASE_URL=(str, "https://chat.maritaca.ai/api"),
    MARITACA_MODEL=(str, "sabia-4"),
    MARITACA_TEMPERATURE=(float, 0.1),
    MARITACA_MAX_OUTPUT_TOKENS=(int, 1024),
    MARITACA_TIMEOUT_SECONDS=(float, 60),
    MARITACA_MAX_RETRIES=(int, 2),
    CELERY_BROKER_URL=(str, "amqp://rag_user:rag_password@localhost:5672//"),
    CELERY_INDEXING_MAX_RETRIES=(int, 3),
    AUDIT_STORE_QUESTION_TEXT=(bool, False),
    AUDIT_RETENTION_DAYS=(int, 90),
    OBSERVABILITY_EXPOSE_QUESTION_TEXT=(bool, False),
)
environ.Env.read_env(PROJECT_ROOT / ".env")

SECRET_KEY = env("DJANGO_SECRET_KEY", default="unsafe-local-development-key")
DEBUG = env("DJANGO_DEBUG")
ALLOWED_HOSTS = env("DJANGO_ALLOWED_HOSTS")
ENVIRONMENT = env("DJANGO_ENVIRONMENT")
API_ACCESS_KEY = env("API_ACCESS_KEY")
API_REQUIRE_AUTHENTICATION = env("API_REQUIRE_AUTHENTICATION")
API_ANON_THROTTLE_RATE = env("API_ANON_THROTTLE_RATE")
API_USER_THROTTLE_RATE = env("API_USER_THROTTLE_RATE")
DOCUMENT_MAX_UPLOAD_SIZE = env("DOCUMENT_MAX_UPLOAD_SIZE")
DOCUMENT_STORAGE_DIR = env("DOCUMENT_STORAGE_DIR")
RAG_CHUNK_SIZE = env("RAG_CHUNK_SIZE")
RAG_CHUNK_OVERLAP = env("RAG_CHUNK_OVERLAP")
EMBEDDING_MODEL = env("EMBEDDING_MODEL")
EMBEDDING_DIMENSION = env("EMBEDDING_DIMENSION")
EMBEDDING_CACHE_DIR = env("EMBEDDING_CACHE_DIR")
EMBEDDING_THREADS = env("EMBEDDING_THREADS")
EMBEDDING_ENABLE_CPU_MEM_ARENA = env("EMBEDDING_ENABLE_CPU_MEM_ARENA")
EMBEDDING_SERVICE_URL = env("EMBEDDING_SERVICE_URL")
EMBEDDING_SERVICE_ENABLED = env("EMBEDDING_SERVICE_ENABLED")
EMBEDDING_SERVICE_TIMEOUT_SECONDS = env("EMBEDDING_SERVICE_TIMEOUT_SECONDS")
QDRANT_URL = env("QDRANT_URL")
QDRANT_COLLECTION = env("QDRANT_COLLECTION")
RAG_TOP_K = env("RAG_TOP_K")
RAG_MIN_RELEVANCE_SCORE = env("RAG_MIN_RELEVANCE_SCORE")
RAG_MAX_CONTEXT_CHARS = env("RAG_MAX_CONTEXT_CHARS")
MARITACA_API_KEY = env("MARITACA_API_KEY")
MARITACA_BASE_URL = env("MARITACA_BASE_URL")
MARITACA_MODEL = env("MARITACA_MODEL")
MARITACA_TEMPERATURE = env("MARITACA_TEMPERATURE")
MARITACA_MAX_OUTPUT_TOKENS = env("MARITACA_MAX_OUTPUT_TOKENS")
MARITACA_TIMEOUT_SECONDS = env("MARITACA_TIMEOUT_SECONDS")
MARITACA_MAX_RETRIES = env("MARITACA_MAX_RETRIES")
CELERY_BROKER_URL = env("CELERY_BROKER_URL")
CELERY_INDEXING_MAX_RETRIES = env("CELERY_INDEXING_MAX_RETRIES")
AUDIT_STORE_QUESTION_TEXT = env("AUDIT_STORE_QUESTION_TEXT")
AUDIT_RETENTION_DAYS = env("AUDIT_RETENTION_DAYS")
OBSERVABILITY_EXPOSE_QUESTION_TEXT = env("OBSERVABILITY_EXPOSE_QUESTION_TEXT")

SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_REFERRER_POLICY = "same-origin"
X_FRAME_OPTIONS = "DENY"
SESSION_COOKIE_HTTPONLY = True
CSRF_COOKIE_HTTPONLY = True
SECURE_SSL_REDIRECT = env("DJANGO_SECURE_SSL_REDIRECT")
SESSION_COOKIE_SECURE = env("DJANGO_SECURE_COOKIES")
CSRF_COOKIE_SECURE = env("DJANGO_SECURE_COOKIES")

CELERY_TASK_SERIALIZER = "json"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_RESULT_SERIALIZER = "json"
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_ACKS_LATE = True
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "rest_framework.authtoken",
    "apps.common",
    "apps.documents",
    "apps.rag",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "apps.common.middleware.RequestLoggingMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

DATABASES = {"default": env.db("DATABASE_URL")}

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

LANGUAGE_CODE = "pt-br"
TIME_ZONE = "America/Sao_Paulo"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
MEDIA_ROOT = DOCUMENT_STORAGE_DIR
MEDIA_URL = "media/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

REST_FRAMEWORK = {
    "EXCEPTION_HANDLER": "apps.common.exceptions.api_exception_handler",
    "DEFAULT_RENDERER_CLASSES": ["rest_framework.renderers.JSONRenderer"],
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.TokenAuthentication",
    ],
    "DEFAULT_PARSER_CLASSES": [
        "rest_framework.parsers.JSONParser",
        "rest_framework.parsers.MultiPartParser",
    ],
    "DEFAULT_THROTTLE_CLASSES": [
        "rest_framework.throttling.AnonRateThrottle",
        "rest_framework.throttling.UserRateThrottle",
    ],
    "DEFAULT_THROTTLE_RATES": {
        "anon": API_ANON_THROTTLE_RATE,
        "user": API_USER_THROTTLE_RATE,
    },
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {"()": "apps.common.logging.JsonFormatter"},
    },
    "handlers": {
        "console_json": {
            "class": "logging.StreamHandler",
            "formatter": "json",
        },
    },
    "loggers": {
        "adaptive_rag": {
            "handlers": ["console_json"],
            "level": "INFO",
            "propagate": False,
        },
    },
}
