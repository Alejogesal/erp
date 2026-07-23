from decimal import Decimal, ROUND_HALF_UP

from django.db import migrations


def reassign_to_cheapest(apps, schema_editor):
    """Reasigna el proveedor principal de cada producto al que tenga el costo
    neto (sin IVA) mas bajo entre los proveedores vinculados, y sincroniza el
    costo del producto desde ahi. Antes, el principal quedaba fijo en el primer
    proveedor vinculado sin comparar precios entre proveedores de la misma
    marca, lo que hacia que la Lista de Precios saliera con el costo de un
    proveedor mas caro en vez del mas barato disponible."""
    Product = apps.get_model("inventory", "Product")
    SupplierProduct = apps.get_model("inventory", "SupplierProduct")

    def cost_net(sp):
        factor = Decimal("1.00") + (sp.vat_percent or Decimal("0.00")) / Decimal("100.00")
        if factor <= 0:
            return sp.last_cost or Decimal("0.00")
        return ((sp.last_cost or Decimal("0.00")) / factor).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    for product in Product.objects.iterator():
        links = list(SupplierProduct.objects.filter(product_id=product.id))
        if not links:
            continue
        priced = [link for link in links if cost_net(link) > 0]
        candidates = priced or links
        cheapest = min(candidates, key=cost_net)

        if product.default_supplier_id != cheapest.supplier_id:
            product.default_supplier_id = cheapest.supplier_id
            product.save(update_fields=["default_supplier"])

        net = cost_net(cheapest)
        vat = cheapest.vat_percent or Decimal("0.00")
        if product.avg_cost != net or (product.vat_percent or Decimal("0.00")) != vat:
            product.avg_cost = net
            product.vat_percent = vat
            product.save(update_fields=["avg_cost", "vat_percent"])


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0057_resync_product_cost_from_principal"),
    ]

    operations = [
        migrations.RunPython(reassign_to_cheapest, noop),
    ]
