from django.conf import settings
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView


class HealthCheckView(APIView):
    authentication_classes = []
    permission_classes = []

    def get(self, request: Request) -> Response:
        return Response(
            {
                "status": "ok",
                "service": "adaptive-rag-api",
                "environment": settings.ENVIRONMENT,
            }
        )
