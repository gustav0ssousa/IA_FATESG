from django.contrib import admin

from apps.rag.models import IndexingJob, RAGQueryRecord, RAGQuerySource


@admin.register(IndexingJob)
class IndexingJobAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "document",
        "status",
        "attempts",
        "indexed_chunks",
        "created_at",
        "finished_at",
    )
    list_filter = ("status", "created_at")
    search_fields = ("document__title", "celery_task_id", "error_message")
    readonly_fields = (
        "id",
        "celery_task_id",
        "created_at",
        "started_at",
        "finished_at",
        "updated_at",
    )


class RAGQuerySourceInline(admin.TabularInline):
    model = RAGQuerySource
    extra = 0
    readonly_fields = ("document_id", "source_name", "rank", "score")


@admin.register(RAGQueryRecord)
class RAGQueryRecordAdmin(admin.ModelAdmin):
    list_display = (
        "request_id",
        "status",
        "model",
        "source_count",
        "duration_ms",
        "created_at",
    )
    list_filter = ("status", "model", "created_at")
    search_fields = ("request_id", "question", "error_message")
    readonly_fields = ("id", "request_id", "created_at")
    inlines = (RAGQuerySourceInline,)
