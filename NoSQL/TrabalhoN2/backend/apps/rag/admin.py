from django.contrib import admin

from apps.rag.models import IndexingJob


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
