import secrets

from django.conf import settings
from rest_framework.permissions import BasePermission
from rest_framework.request import Request
from rest_framework.views import APIView


def has_valid_api_key(request: Request) -> bool:
    configured_key = settings.API_ACCESS_KEY
    provided_key = request.headers.get("X-API-Key", "")
    return bool(configured_key) and secrets.compare_digest(provided_key, configured_key)


def is_open_local_profile() -> bool:
    return not settings.API_REQUIRE_AUTHENTICATION and not settings.API_ACCESS_KEY


class AuthenticatedOrAPIKey(BasePermission):
    message = "Autenticacao obrigatoria."

    def has_permission(self, request: Request, view: APIView) -> bool:
        return (
            is_open_local_profile()
            or request.user.is_authenticated
            or has_valid_api_key(request)
        )


class StaffOrAPIKey(BasePermission):
    message = "Acesso administrativo obrigatorio."

    def has_permission(self, request: Request, view: APIView) -> bool:
        return (
            is_open_local_profile()
            or request.user.is_staff
            or has_valid_api_key(request)
        )
