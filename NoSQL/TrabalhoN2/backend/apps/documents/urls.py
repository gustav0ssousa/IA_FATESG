from django.urls import path

from apps.documents.views import DocumentIngestionView, DocumentListView

urlpatterns = [
    path("", DocumentListView.as_view(), name="document-list"),
    path("ingest", DocumentIngestionView.as_view(), name="document-ingest"),
]
