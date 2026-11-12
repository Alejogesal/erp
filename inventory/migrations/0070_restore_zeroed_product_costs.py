from decimal import Decimal, ROUND_HALF_UP

from django.db import migrations


def restore_zeroed_costs(apps, schema_editor):
    """Devuelve el costo a los productos que quedaron en $0 teniendo precio
    cargado en algun proveedor.

    Los ajustes de stock (manuales y los automaticos del sync de MercadoLibre)
    recalculaban el costo como promedio ponderado y lo dejaban en $0 cuando el
    stock total del producto quedaba en cero o negativo. El costo se vuelve a
    tomar del proveedor principal: el elegido para la marca si tiene precio y,
    si no, el mas barato entre los vinculados con precio."""
    Product = apps.get_model("inventory", "Product")
    SupplierProduct = apps.get_model("inventory", "SupplierProduct")
    BrandSupplier = apps.get_model("inventory", "BrandSupplier")

    def cost_net(sp):
        factor = Decimal("1.00") + (sp.vat_percent or Decimal("0.00")) / Decimal("100.00")
        if factor <= 0:
            return sp.last_cost or Decimal("0.00")
        return ((sp.last_cost or Decimal("0.00")) / factor).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    brand_supplier = {
        (bs.group or "").strip().casefold(): bs.supplier_id for bs in BrandSupplier.objects.all()
    }

    fixed = 0
    zeroed = Product.objects.filter(avg_cost__lte=0, is_kit=False, supplier_products__last_cost__gt=0)
    for product in zeroed.distinct().iterator():
        priced = [
            link
            for link in SupplierProduct.objects.filter(product_id=product.id)
            if cost_net(link) > 0
        ]
        if not priced:
            continue
        pinned_id = brand_supplier.get((product.group or "").strip().casefold())
        chosen = next((link for link in priced if link.supplier_id == pinned_id), None)
        if chosen is None:
            chosen = min(priced, key=cost_net)
        product.default_supplier_id = chosen.supplier_id
        product.avg_cost = cost_net(chosen)
        product.vat_percent = chosen.vat_percent or Decimal("0.00")
        product.save(update_fields=["default_supplier", "avg_cost", "vat_percent"])
        fixed += 1
    if fixed:
        print(f"  Costo restaurado en {fixed} producto(s) que estaban en $0.")


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0069_excludedbrand"),
    ]

    operations = [
        migrations.RunPython(restore_zeroed_costs, noop),
    ]
