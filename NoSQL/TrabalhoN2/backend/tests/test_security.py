import uuid

import pytest
from django.contrib.auth import get_user_model
from django.test import override_settings
from rest_framework import status
from rest_framework.test import APIClient

pytestmark = pytest.mark.django_db


@override_settings(API_ACCESS_KEY="segredo-local", API_REQUIRE_AUTHENTICATION=True)
def test_public_api_requires_configured_access_key() -> None:
    denied = APIClient().get("/api/documents/")
    allowed = APIClient().get("/api/documents/", HTTP_X_API_KEY="segredo-local")

    assert denied.status_code == status.HTTP_401_UNAUTHORIZED
    assert "credenciais de autenticação" in denied.json()["detail"]
    assert allowed.status_code == status.HTTP_200_OK


@override_settings(API_ACCESS_KEY="segredo-local", API_REQUIRE_AUTHENTICATION=False)
def test_api_key_still_protects_api_when_individual_login_is_disabled() -> None:
    denied = APIClient().get("/api/documents/")
    allowed = APIClient().get("/api/documents/", HTTP_X_API_KEY="segredo-local")

    assert denied.status_code == status.HTTP_401_UNAUTHORIZED
    assert allowed.status_code == status.HTTP_200_OK


@override_settings(API_ACCESS_KEY="segredo-local", API_REQUIRE_AUTHENTICATION=True)
def test_health_check_remains_public() -> None:
    response = APIClient().get("/api/health")

    assert response.status_code == status.HTTP_200_OK


@override_settings(API_REQUIRE_AUTHENTICATION=True)
def test_login_token_allows_authenticated_read_access() -> None:
    get_user_model().objects.create_user(username="leitor", password="senha-forte-123")
    client = APIClient()

    login = client.post(
        "/api/auth/login",
        {"username": "leitor", "password": "senha-forte-123"},
        format="json",
    )
    token = login.json()["token"]
    client.credentials(HTTP_AUTHORIZATION=f"Token {token}")

    assert login.status_code == status.HTTP_200_OK
    assert login.json()["user"]["is_staff"] is False
    assert client.get("/api/auth/me").json()["username"] == "leitor"
    assert client.get("/api/documents/").status_code == status.HTTP_200_OK
    assert client.get("/api/rag/kpis/overview").status_code == status.HTTP_403_FORBIDDEN


@override_settings(API_REQUIRE_AUTHENTICATION=True)
def test_staff_token_allows_administrative_access_and_logout() -> None:
    get_user_model().objects.create_user(
        username="gestor",
        password="senha-forte-123",
        is_staff=True,
    )
    client = APIClient()
    login = client.post(
        "/api/auth/login",
        {"username": "gestor", "password": "senha-forte-123"},
        format="json",
    )
    client.credentials(HTTP_AUTHORIZATION=f"Token {login.json()['token']}")

    assert client.get("/api/rag/kpis/overview").status_code == status.HTTP_200_OK
    assert client.post("/api/auth/logout").status_code == status.HTTP_204_NO_CONTENT
    assert client.get("/api/auth/me").status_code == status.HTTP_401_UNAUTHORIZED


def test_auth_config_reports_if_login_is_required() -> None:
    with override_settings(API_REQUIRE_AUTHENTICATION=True):
        response = APIClient().get("/api/auth/config")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"required": True}


def test_invalid_request_id_is_replaced_with_uuid() -> None:
    response = APIClient().get("/api/health", HTTP_X_REQUEST_ID="quebra-de-log")

    assert uuid.UUID(response.headers["X-Request-ID"])
    assert response.headers["X-Request-ID"] != "quebra-de-log"


def test_validation_error_has_standard_metadata() -> None:
    response = APIClient().post("/api/rag/query", {"question": ""}, format="json")

    body = response.json()
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert body["detail"] == "A requisicao nao pode ser processada."
    assert body["code"] == 400
    assert body["request_id"]
    assert "question" in body["errors"]
