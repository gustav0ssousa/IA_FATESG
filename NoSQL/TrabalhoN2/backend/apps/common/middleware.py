import logging
import time
import uuid

from django.http import HttpRequest, HttpResponse

logger = logging.getLogger("adaptive_rag.requests")


class RequestLoggingMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        started_at = time.perf_counter()
        request_id = self._request_id(request.headers.get("X-Request-ID"))
        request.request_id = request_id
        response = self.get_response(request)
        duration_ms = round((time.perf_counter() - started_at) * 1000)
        request_id = request.request_id
        response["X-Request-ID"] = request_id
        logger.info(
            "http_request_completed",
            extra={
                "event": "http_request_completed",
                "request_id": request_id,
                "method": request.method,
                "path": request.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            },
        )
        return response

    @staticmethod
    def _request_id(value: str | None) -> str:
        try:
            return str(uuid.UUID(value)) if value else str(uuid.uuid4())
        except (ValueError, AttributeError):
            return str(uuid.uuid4())
