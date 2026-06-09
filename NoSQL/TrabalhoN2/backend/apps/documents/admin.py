from django.contrib import admin

from apps.documents.models import Document, DocumentChunk


class DocumentChunkInline(admin.TabularInline):
    model = DocumentChunk
    extra = 0
    fields = ("position", "content_hash", "token_count", "page_number")
    readonly_fields = fields
    show_change_link = True


@admin.register(Document)
class DocumentAdmin(admin.ModelAdmin):
    list_display = ("title", "source_type", "status", "created_at", "updated_at")
    list_filter = ("status", "source_type", "created_at")
    search_fields = ("title", "source_name", "content_hash")
    readonly_fields = ("id", "created_at", "updated_at")
    inlines = (DocumentChunkInline,)


@admin.register(DocumentChunk)
class DocumentChunkAdmin(admin.ModelAdmin):
    list_display = ("document", "position", "token_count", "page_number")
    search_fields = ("document__title", "content", "content_hash")
    readonly_fields = ("id", "created_at")
