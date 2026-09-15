"""Diagnóstico de sólo lectura: salud del número de stock COMUN en sí mismo.

El diagnóstico anterior (diagnose_ml_stock_lag) confirmó que el descuento por
ventas ML no-Full SÍ se aplica, y casi siempre en segundos. Este comando revisa
la otra punta: si el número de COMUN del que parte ese descuento es correcto,
y si lo publicado en MercadoLibre coincide con él. Cubre los dos pendientes
que ya estaban anotados de la migración de agosto:

1. Productos con variedades cuyo Stock COMUN no coincide con la suma de sus
   variedades (arrastre del bug de doble descuento corregido en f2aa012, pero
   nunca recalculado en los datos viejos).
2. Publicaciones Flex/convivencia cuyo último stock empujado a ML
   (MercadoLibreItem.flex_quantity) no coincide con el COMUN actual: si ML
   quedó mostrando MÁS de lo real, ML sigue aceptando pedidos de algo que ya
   no está.

También lista productos con stock COMUN negativo (evidencia directa de
sobreventa ya ocurrida) y si la reconciliación automática (ML_STOCK_RECONCILE)
está prendida.

No modifica nada.

Uso:
  python manage.py diagnose_comun_stock_health
  python manage.py diagnose_comun_stock_health --product "oil"
"""
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db.models import Q, Sum

from inventory.mercadolibre import reconcile_enabled
from inventory.models import MercadoLibreItem, Product, ProductVariant, Stock, Warehouse


class Command(BaseCommand):
    help = "Audita si el Stock COMUN es internamente consistente y coincide con lo publicado en ML."

    def add_arguments(self, parser):
        parser.add_argument(
            "--product",
            default="",
            help="Filtrar por nombre o SKU de producto (case-insensitive, substring).",
        )

    def handle(self, *args, **options):
        product_filter = options["product"].strip()

        comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
        if not comun_wh:
            self.stderr.write("No existe el depósito COMUN.")
            return

        self.stdout.write(
            f"Reconciliación automática COMUN -> ML (ML_STOCK_RECONCILE): "
            f"{'ENCENDIDA' if reconcile_enabled() else 'APAGADA'}"
        )

        # --- 1. Productos con variedades: COMUN vs suma de variedades ---
        products_with_variants = Product.objects.filter(variants__isnull=False).distinct()
        if product_filter:
            products_with_variants = products_with_variants.filter(
                Q(name__icontains=product_filter) | Q(sku__icontains=product_filter)
            )

        mismatches = []
        for product in products_with_variants.prefetch_related("variants"):
            variant_sum = (
                ProductVariant.objects.filter(product=product).aggregate(total=Sum("quantity")).get("total")
            ) or Decimal("0.00")
            stock = Stock.objects.filter(product=product, warehouse=comun_wh).first()
            comun_qty = stock.quantity if stock else Decimal("0.00")
            if comun_qty != variant_sum:
                mismatches.append((product, comun_qty, variant_sum))

        self.stdout.write(self.style.WARNING("\n=== Productos con variedades: COMUN vs. suma de variedades ==="))
        if not mismatches:
            self.stdout.write("  (ninguno desalineado)")
        for product, comun_qty, variant_sum in sorted(mismatches, key=lambda t: -(t[1] - t[2])):
            delta = comun_qty - variant_sum
            signo = "COMUN de más" if delta > 0 else "COMUN de menos"
            self.stdout.write(
                f"  {product.sku or product.name:<20} COMUN={comun_qty:>8} suma variedades={variant_sum:>8}  "
                f"({signo}: {abs(delta)})"
            )

        # --- 2. Publicaciones Flex/convivencia: último push vs. COMUN actual ---
        flex_items = MercadoLibreItem.objects.filter(has_flex=True, product__isnull=False).select_related(
            "product", "variant"
        )
        if product_filter:
            flex_items = flex_items.filter(
                Q(product__name__icontains=product_filter) | Q(product__sku__icontains=product_filter)
            )

        # Publicaciones sin variedad elegida: la reconciliación las deja
        # explícitamente afuera ("las que no tienen variedad elegida no se
        # tocan"), así que comparar su flex_quantity contra el total del
        # producto no dice nada — se separan aparte, agrupadas por producto,
        # porque varias publicaciones sin variedad apuntando al MISMO producto
        # es en sí mismo la señal de un mapeo SKU/variante mal hecho.
        no_variant_by_product: dict[int, list] = {}
        push_mismatches = []
        for item in flex_items:
            if item.variant_id:
                variant = ProductVariant.objects.filter(id=item.variant_id).first()
                real_qty = int(variant.quantity) if variant else None
                if real_qty is None:
                    continue
                if item.flex_quantity != real_qty:
                    push_mismatches.append((item, real_qty))
            else:
                no_variant_by_product.setdefault(item.product_id, []).append(item)

        self.stdout.write(self.style.WARNING("\n=== Publicaciones Flex CON variedad asignada: último stock empujado a ML vs. COMUN real ==="))
        if not push_mismatches:
            self.stdout.write("  (ninguna desalineada)")
        for item, real_qty in sorted(push_mismatches, key=lambda t: -(t[0].flex_quantity - t[1])):
            delta = item.flex_quantity - real_qty
            riesgo = " *** ML muestra MÁS de lo real: riesgo de sobreventa ***" if delta > 0 else ""
            self.stdout.write(
                f"  {item.item_id:<16} {item.title[:40]:<40} publicado={item.flex_quantity:>6} real={real_qty:>6}{riesgo}"
            )

        self.stdout.write(self.style.WARNING("\n=== Publicaciones Flex SIN variedad asignada (la reconciliación no las toca) ==="))
        if not no_variant_by_product:
            self.stdout.write("  (ninguna)")
        for product_id, items in no_variant_by_product.items():
            product = items[0].product
            stock = Stock.objects.filter(product=product, warehouse=comun_wh).first()
            comun_total = stock.quantity if stock else Decimal("0.00")
            compartido = " *** varias publicaciones distintas comparten el mismo producto del ERP ***" if len(items) > 1 else ""
            self.stdout.write(
                f"  Producto #{product_id} {product.sku or product.name:<20} COMUN total={comun_total}{compartido}"
            )
            for item in items:
                self.stdout.write(f"      {item.item_id:<16} {item.title[:50]:<50} publicado={item.flex_quantity}")

        # --- 3. Stock COMUN negativo: sobreventa ya concretada ---
        negativos = Stock.objects.filter(warehouse=comun_wh, quantity__lt=0).select_related("product")
        if product_filter:
            negativos = negativos.filter(
                Q(product__name__icontains=product_filter) | Q(product__sku__icontains=product_filter)
            )
        self.stdout.write(self.style.ERROR("\n=== Stock COMUN negativo (ya se vendió más de lo que había) ==="))
        if not negativos.exists():
            self.stdout.write("  (ninguno)")
        for stock in negativos.order_by("quantity"):
            self.stdout.write(f"  {stock.product.sku or stock.product.name:<20} COMUN={stock.quantity}")

        self.stdout.write(
            f"\nResumen: {len(mismatches)} productos con variedades desalineadas | "
            f"{len(push_mismatches)} publicaciones Flex desalineadas con ML | "
            f"{negativos.count()} productos en negativo"
        )
