# === BEGIN: auto-register models for admin read-only === # django-extensions的admin_generator 或 django-admin-autoregister 第三方包
from django.contrib import admin as _admin
from django.apps import apps as _apps
_SKIP_APPS = { "admin", "auth", "contenttypes", "sessions", "authtoken", "django_celery_beat", "django_celery_results", }
for _model in _apps.get_models():
    if _model._meta.app_label in _SKIP_APPS: continue
    if _model in _admin.site._registry: continue
    _concrete = list(_model._meta.concrete_fields)
    _attrs = {
        "list_display": [f.name for f in _concrete][:8], "list_per_page": 50,
        "search_fields": [f.name for f in _concrete if f.get_internal_type() in ("CharField", "TextField")][:5],
        "raw_id_fields": [f.name for f in _concrete if f.get_internal_type() == "ForeignKey"],
    }
    _AutoAdmin = type(f"_Auto{_model.__name__}Admin", (_admin.ModelAdmin,), _attrs)
    _admin.site.register(_model, _AutoAdmin)
# === END auto-register ===
