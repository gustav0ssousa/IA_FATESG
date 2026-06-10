from django.urls import path

from apps.common.views import (
    AuthConfigView,
    CurrentUserView,
    HealthCheckView,
    LoginView,
    LogoutView,
)

urlpatterns = [
    path("health", HealthCheckView.as_view(), name="health-check"),
    path("auth/config", AuthConfigView.as_view(), name="auth-config"),
    path("auth/login", LoginView.as_view(), name="auth-login"),
    path("auth/me", CurrentUserView.as_view(), name="auth-me"),
    path("auth/logout", LogoutView.as_view(), name="auth-logout"),
]
