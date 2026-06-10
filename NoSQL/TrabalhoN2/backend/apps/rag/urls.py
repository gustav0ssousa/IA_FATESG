from django.urls import path

from apps.rag.views import (
    AsyncDocumentIndexView,
    DocumentIndexView,
    IndexingJobDetailView,
    InternalEmbeddingView,
    RAGQueryView,
    SemanticSearchView,
)

urlpatterns = [
    path("internal/embeddings", InternalEmbeddingView.as_view()),
    path("documents/<uuid:document_id>/index", DocumentIndexView.as_view()),
    path("documents/<uuid:document_id>/index-async", AsyncDocumentIndexView.as_view()),
    path("jobs/<uuid:job_id>", IndexingJobDetailView.as_view()),
    path("search", SemanticSearchView.as_view()),
    path("query", RAGQueryView.as_view()),
]
