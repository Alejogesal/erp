"""
Data migration: update avg_cost to include VAT.

Previously avg_cost stored the base price without VAT, and cost_with_vat() multiplied
by vat_percent on the fly. Now avg_cost stores the all-in purchase cost (with VAT).
"""
from django.db import migrations


def include_vat_in_avg_cost(apps, schema_editor):
    Product = apps.get_model("inventory", "Product")
    db_alias = schema_editor.connection.alias
    products = Product.objects.using(db_alias).filter(
        vat_percent__gt=0, avg_cost__gt=0
    )
    for product in products:
        multiplier = 1 + float(product.vat_percent) / 100
        product.avg_cost = round(float(product.avg_cost) * multiplier, 2)
        product.save(update_fields=["avg_cost"])


def reverse_include_vat(apps, schema_editor):
    Product = apps.get_model("inventory", "Product")
    db_alias = schema_editor.connection.alias
    products = Product.objects.using(db_alias).filter(
        vat_percent__gt=0, avg_cost__gt=0
    )
    for product in products:
        multiplier = 1 + float(product.vat_percent) / 100
        product.avg_cost = round(float(product.avg_cost) / multiplier, 2)
        product.save(update_fields=["avg_cost"])


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0042_audit_log_and_updated_at"),
    ]

    operations = [
        migrations.RunPython(include_vat_in_avg_cost, reverse_code=reverse_include_vat),
    ]
