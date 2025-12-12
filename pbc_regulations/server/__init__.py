"""Server application factory for the PBC portal."""

from .app import create_dashboard_app, serve_dashboard

__all__ = ["create_dashboard_app", "serve_dashboard"]
