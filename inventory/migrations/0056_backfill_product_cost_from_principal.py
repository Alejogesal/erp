from decimal import Decimal, ROUND_HALF_UP

from django.db import migrations


def backfill(apps, schema_editor):
    """Deja el costo (avg_cost sin IVA + vat_percent) de cada producto igual al
    de su proveedor principal. Corrige productos cuyo costo quedó viejo/corrupto
    (0 o mal) mientras el costo correcto ya vive en la lista del proveedor."""
    Product = apps.get_model("inventory", "Product")
    SupplierProduct = apps.get_model("inventory", "SupplierProduct")
    for product in Product.objects.filter(default_supplier__isnull=False).iterator():
        sp = (
            SupplierProduct.objects.filter(
                supplier_id=product.default_supplier_id, product_id=product.id
            )
            .first()
        )
        if not sp or not sp.last_cost or sp.last_cost <= 0:
            continue  # sin precio del proveedor: no se toca el costo actual
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
        ("inventory", "0055_supplierproduct_vat"),
    ]

    operations = [
        migrations.RunPython(backfill, noop),
    ]
