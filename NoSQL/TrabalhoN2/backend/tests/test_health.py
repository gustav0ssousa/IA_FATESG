from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient


def test_health_check_returns_service_status() -> None:
    response = APIClient().get(reverse("health-check"))

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "status": "ok",
        "service": "adaptive-rag-api",
        "environment": "local",
    }


def test_health_check_does_not_require_authentication() -> None:
    response = APIClient().get("/api/health")

    assert response.status_code == status.HTTP_200_OK
