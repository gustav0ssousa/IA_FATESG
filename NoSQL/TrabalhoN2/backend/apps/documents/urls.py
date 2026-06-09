from django.urls import path

from apps.documents.views import DocumentIngestionView

urlpatterns = [
    path("ingest", DocumentIngestionView.as_view(), name="document-ingest"),
]
