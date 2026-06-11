from django.urls import path

from apps.documents.views import (
    DocumentDetailView,
    DocumentIngestionView,
    DocumentListView,
    DocumentReprocessView,
)

urlpatterns = [
    path("", DocumentListView.as_view(), name="document-list"),
    path("ingest", DocumentIngestionView.as_view(), name="document-ingest"),
    path("<uuid:document_id>", DocumentDetailView.as_view(), name="document-detail"),
    path(
        "<uuid:document_id>/reprocess",
        DocumentReprocessView.as_view(),
        name="document-reprocess",
    ),
]
