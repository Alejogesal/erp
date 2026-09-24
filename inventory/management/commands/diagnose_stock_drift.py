"""Diagnóstico de sólo lectura: productos cuyo stock no cuadra con su historial.

Por cada producto compara, en el depósito COMUN:

1. El stock guardado.
2. Lo que dan los movimientos (entradas + ajustes positivos + transferencias
   recibidas - salidas - ajustes negativos - transferencias enviadas).
3. La suma de sus variedades, si tiene.

Una diferencia entre 1 y 2 indica movimientos que no tocaron el stock o stock
tocado sin movimiento (ajustes por fuera, recálculos desde variedades). Una
diferencia entre 1 y 3 es el arrastre del recálculo de variedades.
Un stock positivo mayor al del historial es el síntoma de "el sistema tiene más
de lo que hay".

No modifica nada.

Uso:
  python manage.py diagnose_stock_drift
  python manage.py diagnose_stock_drift --product "aceite" --min-diff 1
"""
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db.models import Q, Sum

from inventory.models import Product, ProductVariant, Stock, StockMovement, Warehouse

ZERO = Decimal("0.00")
MT = StockMovement.MovementType


class Command(BaseCommand):
    help = "Compara el stock COMUN contra el historial de movimientos y contra la suma de variedades."

    def add_arguments(self, parser):
        parser.add_argument("--product", default="", help="Filtra por nombre o SKU (contiene).")
        parser.add_argument("--min-diff", type=Decimal, default=Decimal("0.01"), help="Diferencia mínima a listar.")

    def handle(self, *args, **opts):
        comun = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
        if not comun:
            self.stderr.write("No hay depósito COMUN.")
            return
        products = Product.objects.all()
        if opts["product"]:
            products = products.filter(Q(name__icontains=opts["product"]) | Q(sku__icontains=opts["product"]))

        def total(**flt):
            return StockMovement.objects.filter(**flt).aggregate(t=Sum("quantity"))["t"] or ZERO

        rows = []
        for product in products.order_by("name"):
            stock = Stock.objects.filter(product=product, warehouse=comun).first()
            stored = stock.quantity if stock else ZERO
            base = StockMovement.objects.filter(product=product)
            ledger = (
                (base.filter(to_warehouse=comun, movement_type__in=[MT.ENTRY, MT.ADJUSTMENT, MT.TRANSFER])
                 .aggregate(t=Sum("quantity"))["t"] or ZERO)
                - (base.filter(from_warehouse=comun, movement_type__in=[MT.EXIT, MT.ADJUSTMENT, MT.TRANSFER])
                   .aggregate(t=Sum("quantity"))["t"] or ZERO)
            )
            variants = ProductVariant.objects.filter(product=product)
            var_sum = variants.aggregate(t=Sum("quantity"))["t"] if variants.exists() else None
            d_ledger = stored - ledger
            d_var = (stored - var_sum) if var_sum is not None else ZERO
            if abs(d_ledger) >= opts["min_diff"] or abs(d_var) >= opts["min_diff"]:
                rows.append((product, stored, ledger, var_sum, d_ledger, d_var))

        self.stdout.write(f"Productos con diferencias: {len(rows)} de {products.count()}")
        self.stdout.write(f"{'Producto':45} {'Guardado':>10} {'Historial':>10} {'Variedades':>11} {'G-H':>9} {'G-V':>9}")
        for product, stored, ledger, var_sum, d_ledger, d_var in sorted(rows, key=lambda r: -abs(r[4])):
            vs = "-" if var_sum is None else str(var_sum)
            self.stdout.write(
                f"{(product.name or '')[:45]:45} {stored:>10} {ledger:>10} {vs:>11} {d_ledger:>9} {d_var:>9}"
            )
        over = [r for r in rows if r[4] > 0]
        self.stdout.write(f"\nGuardado MAYOR al historial: {len(over)} productos, {sum((r[4] for r in over), ZERO)} unidades.")
        self.stdout.write("Nota: si cargaron stock inicial sin movimiento, esa base aparece como diferencia constante.")
