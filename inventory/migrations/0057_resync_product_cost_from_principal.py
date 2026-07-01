from decimal import Decimal, ROUND_HALF_UP

from django.db import migrations


def resync(apps, schema_editor):
    """Re-sincroniza el costo (avg_cost sin IVA + vat_percent) de cada producto
    desde su proveedor principal. Corre de nuevo ahora que los costos en
    Proveedores ya están correctos (la 0056 corrió antes de que lo estuvieran)."""
    Product = apps.get_model("inventory", "Product")
    SupplierProduct = apps.get_model("inventory", "SupplierProduct")
    for product in Product.objects.filter(default_supplier__isnull=False).iterator():
        sp = SupplierProduct.objects.filter(
            supplier_id=product.default_supplier_id, product_id=product.id
        ).first()
        if not sp or not sp.last_cost or sp.last_cost <= 0:
            continue
        vat = sp.vat_percent or Decimal("0.00")
        factor = Decimal("1.00") + vat / Decimal("100.00")
        net = (
            (sp.last_cost / factor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            if factor > 0
            else sp.last_cost
        )
        if product.avg_cost != net or (product.vat_percent or Decimal("0.00")) != vat:
            product.avg_cost = net
            product.vat_percent = vat
            product.save(update_fields=["avg_cost", "vat_percent"])


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0056_backfill_product_cost_from_principal"),
    ]

    operations = [
        migrations.RunPython(resync, noop),
    ]
