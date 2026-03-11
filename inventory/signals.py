from django.db.models.signals import post_save, pre_delete, pre_save
from django.dispatch import receiver

from .middleware import get_current_user

AUDITED_MODELS = []
EXCLUDED_FIELDS = {"id", "created_at", "updated_at", "last_synced", "last_metrics_at"}


def _model_fields(instance):
    data = {}
    for field in instance._meta.concrete_fields:
        if field.name in EXCLUDED_FIELDS:
            continue
        try:
            val = field.value_from_object(instance)
            data[field.name] = str(val) if val is not None else None
        except Exception:
            pass
    return data


def _on_pre_save(sender, instance, **kwargs):
    if not instance.pk:
        instance._audit_is_new = True
        instance._audit_old_values = {}
        return
    try:
        old = sender.objects.get(pk=instance.pk)
        instance._audit_is_new = False
        instance._audit_old_values = _model_fields(old)
    except sender.DoesNotExist:
        instance._audit_is_new = True
        instance._audit_old_values = {}


def _on_post_save(sender, instance, created, **kwargs):
    from .models import AuditLog

    is_new = getattr(instance, "_audit_is_new", created)
    old_values = getattr(instance, "_audit_old_values", {})
    changes = None

    if not is_new:
        new_values = _model_fields(instance)
        diff = {
            k: {"antes": old_values.get(k), "despues": new_val}
            for k, new_val in new_values.items()
            if old_values.get(k) != new_val
        }
        if not diff:
            return
        changes = diff

    AuditLog.objects.create(
        action=AuditLog.Action.CREATE if is_new else AuditLog.Action.UPDATE,
        model_name=sender.__name__,
        object_id=instance.pk,
        object_repr=str(instance),
        changes=changes,
        user=get_current_user(),
    )


def _on_pre_delete(sender, instance, **kwargs):
    from .models import AuditLog

    AuditLog.objects.create(
        action=AuditLog.Action.DELETE,
        model_name=sender.__name__,
        object_id=instance.pk,
        object_repr=str(instance),
        changes=None,
        user=get_current_user(),
    )


def connect_audit_signals():
    from .models import (
        Customer,
        CustomerPayment,
        Product,
        Purchase,
        Sale,
        Supplier,
        SupplierPayment,
    )

    models = [Sale, Purchase, Product, Customer, Supplier, CustomerPayment, SupplierPayment]
    for model in models:
        pre_save.connect(_on_pre_save, sender=model, weak=False)
        post_save.connect(_on_post_save, sender=model, weak=False)
        pre_delete.connect(_on_pre_delete, sender=model, weak=False)
