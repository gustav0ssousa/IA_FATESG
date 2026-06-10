import math
from datetime import timedelta

from django.conf import settings
from django.db.models import Avg, Count, Q
from django.db.models.functions import TruncDate
from django.utils import timezone

from apps.documents.models import Document, DocumentStatus
from apps.rag.models import (
    IndexingJob,
    IndexingJobStatus,
    QueryStatus,
    RAGQueryRecord,
    RAGQuerySource,
)


def percentile(values: list[int], percentile_value: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, math.ceil(percentile_value * len(ordered)) - 1)
    return ordered[index]


def build_kpi_summary() -> dict:
    queries = RAGQueryRecord.objects.all()
    total_queries = queries.count()
    successful_queries = queries.filter(status=QueryStatus.SUCCESS).count()
    failed_queries = total_queries - successful_queries
    durations = list(queries.values_list("duration_ms", flat=True))
    average = queries.aggregate(value=Avg("duration_ms"))["value"] or 0
    since_24h = timezone.now() - timedelta(hours=24)
    since_7d = timezone.now() - timedelta(days=6)

    top_documents = list(
        RAGQuerySource.objects.values("document_id", "source_name")
        .annotate(
            retrieval_count=Count("id"),
            query_count=Count("query_id", distinct=True),
            average_score=Avg("score"),
        )
        .order_by("-query_count", "-retrieval_count")[:5]
    )
    daily_queries = {
        item["day"]: item
        for item in queries.filter(created_at__date__gte=since_7d.date())
        .annotate(day=TruncDate("created_at"))
        .values("day")
        .annotate(
            total=Count("id"),
            errors=Count("id", filter=Q(status=QueryStatus.ERROR)),
        )
        .order_by("day")
    }
    timeline = []
    for offset in range(7):
        day = since_7d.date() + timedelta(days=offset)
        item = daily_queries.get(day, {})
        timeline.append(
            {
                "date": day.isoformat(),
                "total": item.get("total", 0),
                "errors": item.get("errors", 0),
            }
        )

    job_counts = {
        item["status"]: item["total"]
        for item in IndexingJob.objects.values("status").annotate(total=Count("id"))
    }
    return {
        "queries": {
            "total": total_queries,
            "last_24h": queries.filter(created_at__gte=since_24h).count(),
            "successful": successful_queries,
            "failed": failed_queries,
            "error_rate": round(failed_queries / total_queries, 4) if total_queries else 0,
            "average_response_ms": round(average),
            "p95_response_ms": percentile(durations, 0.95),
            "average_sources": round(
                queries.aggregate(value=Avg("source_count"))["value"] or 0,
                2,
            ),
        },
        "documents": {
            "total": Document.objects.count(),
            "indexed": Document.objects.filter(status=DocumentStatus.INDEXED).count(),
            "top_retrieved": top_documents,
        },
        "indexing_jobs": {
            "queued": job_counts.get(IndexingJobStatus.QUEUED, 0),
            "processing": job_counts.get(IndexingJobStatus.PROCESSING, 0),
            "retrying": job_counts.get(IndexingJobStatus.RETRYING, 0),
            "completed": job_counts.get(IndexingJobStatus.COMPLETED, 0),
            "failed": job_counts.get(IndexingJobStatus.FAILED, 0),
        },
        "timeline": timeline,
        "recent_queries": [
            {
                **item,
                "question": (
                    item["question"]
                    if settings.OBSERVABILITY_EXPOSE_QUESTION_TEXT
                    else "[conteudo oculto]"
                ),
            }
            for item in queries.values(
                "request_id",
                "question",
                "status",
                "model",
                "source_count",
                "duration_ms",
                "created_at",
            )[:10]
        ],
    }
