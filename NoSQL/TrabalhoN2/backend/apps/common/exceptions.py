from rest_framework.response import Response
from rest_framework.views import exception_handler


def api_exception_handler(exc: Exception, context: dict) -> Response | None:
    response = exception_handler(exc, context)
    if response is None:
        return None

    original_data = response.data
    detail = (
        original_data.get("detail")
        if isinstance(original_data, dict) and "detail" in original_data
        else "A requisicao nao pode ser processada."
    )
    request = context.get("request")
    request_id = getattr(request, "request_id", None)
    if request_id is None and request is not None:
        request_id = getattr(getattr(request, "_request", None), "request_id", None)

    response.data = {
        "detail": str(detail),
        "code": response.status_code,
        "request_id": request_id,
    }
    if detail == "A requisicao nao pode ser processada.":
        response.data["errors"] = original_data
    return response
