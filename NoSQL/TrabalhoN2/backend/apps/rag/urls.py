from django.urls import path

from apps.rag.views import DocumentIndexView, RAGQueryView, SemanticSearchView

urlpatterns = [
    path("documents/<uuid:document_id>/index", DocumentIndexView.as_view()),
    path("search", SemanticSearchView.as_view()),
    path("query", RAGQueryView.as_view()),
]
