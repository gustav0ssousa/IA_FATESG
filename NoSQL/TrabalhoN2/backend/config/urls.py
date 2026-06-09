from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/", include("apps.common.urls")),
    path("api/documents/", include("apps.documents.urls")),
    path("api/rag/", include("apps.rag.urls")),
]
