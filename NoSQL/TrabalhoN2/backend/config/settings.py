from pathlib import Path

import environ

BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent

env = environ.Env(
    DJANGO_DEBUG=(bool, False),
    DJANGO_ALLOWED_HOSTS=(list, ["localhost", "127.0.0.1"]),
    DJANGO_ENVIRONMENT=(str, "local"),
    DATABASE_URL=(str, f"sqlite:///{BASE_DIR / 'db.sqlite3'}"),
    DOCUMENT_MAX_UPLOAD_SIZE=(int, 10 * 1024 * 1024),
    RAG_CHUNK_SIZE=(int, 1200),
    RAG_CHUNK_OVERLAP=(int, 200),
    EMBEDDING_MODEL=(str, "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"),
    EMBEDDING_DIMENSION=(int, 384),
    QDRANT_URL=(str, "http://localhost:6333"),
    QDRANT_COLLECTION=(str, "documents"),
    RAG_TOP_K=(int, 5),
    RAG_MAX_CONTEXT_CHARS=(int, 12000),
    MARITACA_API_KEY=(str, ""),
    MARITACA_BASE_URL=(str, "https://chat.maritaca.ai/api"),
    MARITACA_MODEL=(str, "sabia-4"),
    MARITACA_TEMPERATURE=(float, 0.1),
    MARITACA_MAX_OUTPUT_TOKENS=(int, 1024),
    MARITACA_TIMEOUT_SECONDS=(float, 60),
    MARITACA_MAX_RETRIES=(int, 2),
)
environ.Env.read_env(PROJECT_ROOT / ".env")

SECRET_KEY = env("DJANGO_SECRET_KEY", default="unsafe-local-development-key")
DEBUG = env("DJANGO_DEBUG")
ALLOWED_HOSTS = env("DJANGO_ALLOWED_HOSTS")
ENVIRONMENT = env("DJANGO_ENVIRONMENT")
DOCUMENT_MAX_UPLOAD_SIZE = env("DOCUMENT_MAX_UPLOAD_SIZE")
RAG_CHUNK_SIZE = env("RAG_CHUNK_SIZE")
RAG_CHUNK_OVERLAP = env("RAG_CHUNK_OVERLAP")
EMBEDDING_MODEL = env("EMBEDDING_MODEL")
EMBEDDING_DIMENSION = env("EMBEDDING_DIMENSION")
QDRANT_URL = env("QDRANT_URL")
QDRANT_COLLECTION = env("QDRANT_COLLECTION")
RAG_TOP_K = env("RAG_TOP_K")
RAG_MAX_CONTEXT_CHARS = env("RAG_MAX_CONTEXT_CHARS")
MARITACA_API_KEY = env("MARITACA_API_KEY")
MARITACA_BASE_URL = env("MARITACA_BASE_URL")
MARITACA_MODEL = env("MARITACA_MODEL")
MARITACA_TEMPERATURE = env("MARITACA_TEMPERATURE")
MARITACA_MAX_OUTPUT_TOKENS = env("MARITACA_MAX_OUTPUT_TOKENS")
MARITACA_TIMEOUT_SECONDS = env("MARITACA_TIMEOUT_SECONDS")
MARITACA_MAX_RETRIES = env("MARITACA_MAX_RETRIES")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "apps.common",
    "apps.documents",
    "apps.rag",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
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
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": ["rest_framework.renderers.JSONRenderer"],
    "DEFAULT_PARSER_CLASSES": [
        "rest_framework.parsers.JSONParser",
        "rest_framework.parsers.MultiPartParser",
    ],
}
