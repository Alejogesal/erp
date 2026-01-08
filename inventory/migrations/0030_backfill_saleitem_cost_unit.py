from decimal import Decimal

from django.db import migrations


def backfill_saleitem_cost_unit(apps, schema_editor):
    SaleItem = apps.get_model("inventory", "SaleItem")
    Product = apps.get_model("inventory", "Product")
    for item in SaleItem.objects.select_related("product").all():
        if item.cost_unit and item.cost_unit > 0:
            continue
        product = item.product
        if not product:
            continue
        vat = product.vat_percent or Decimal("0.00")
        multiplier = Decimal("1.00") + vat / Decimal("100.00")
        cost_unit = (product.avg_cost or Decimal("0.00")) * multiplier
        SaleItem.objects.filter(pk=item.pk).update(cost_unit=cost_unit)


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0029_saleitem_cost_unit"),
    ]

    operations = [
        migrations.RunPython(backfill_saleitem_cost_unit, migrations.RunPython.noop),
    ]
