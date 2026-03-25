"""
Data migration: avg_cost was historically stored WITH VAT included (via register_entry).
Going forward avg_cost is stored WITHOUT VAT and cost_with_vat() adds vat_percent at
read time. This migration divides avg_cost by (1 + vat_percent/100) for all products
that have vat_percent > 0, so the base cost is normalised to the pre-VAT value.
"""
from decimal import Decimal, ROUND_HALF_UP

from django.db import migrations


def strip_vat_from_avg_cost(apps, schema_editor):
    Product = apps.get_model("inventory", "Product")
    to_update = []
    for p in Product.objects.filter(vat_percent__gt=0):
        divisor = Decimal("1.00") + (p.vat_percent / Decimal("100.00"))
        p.avg_cost = (p.avg_cost / divisor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        to_update.append(p)
    if to_update:
        Product.objects.bulk_update(to_update, ["avg_cost"])


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0044_credit_note"),
    ]

    operations = [
        migrations.RunPython(strip_vat_from_avg_cost, noop),
    ]
