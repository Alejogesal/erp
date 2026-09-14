"""Diagnóstico de sólo lectura: ventas ML no-Full y su descuento de stock COMUN.

No modifica nada. Sirve para confirmar, con datos reales, dos cosas antes de
tocar el código de sync:

1. Ventas Flex/Colecta/Places/Correo que NUNCA generaron el movimiento de
   salida en COMUN (el leak de _apply_ml_stock_exit descripto en el chat).
   Se separan en "todavía a tiempo" (dentro de la ventana de reintento,
   ML_STOCK_EXIT_MAX_AGE_DAYS) y "ya perdidas" (el sync dejó de intentarlas).

2. Para las que SÍ tienen movimiento: cuánto tardó en aparecer respecto de la
   hora de la venta, para confirmar si el descuento no es instantáneo sino que
   llega recién en un sync posterior (5-15 min, o más si hubo catch-up).

Uso:
  python manage.py diagnose_ml_stock_lag
  python manage.py diagnose_ml_stock_lag --product "oil"      # filtra por nombre/SKU
  python manage.py diagnose_ml_stock_lag --days 30            # ventana a revisar (default 60)
"""
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from inventory.mercadolibre import ML_STOCK_EXIT_MAX_AGE_DAYS
from inventory.models import (
    ML_SELLER_FULFILLED_TYPES,
    MercadoLibreItem,
    Sale,
    StockMovement,
    Warehouse,
)


class Command(BaseCommand):
    help = "Audita el descuento de stock COMUN en ventas ML no-Full: faltantes y demoras."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=60, help="Ventana a revisar hacia atrás (default 60).")
        parser.add_argument(
            "--product",
            default="",
            help="Filtrar por nombre o SKU de producto (case-insensitive, substring).",
        )

    def handle(self, *args, **options):
        days = options["days"]
        product_filter = options["product"].strip()
        since = timezone.now() - timedelta(days=days)
        cutoff = timezone.now() - timedelta(days=ML_STOCK_EXIT_MAX_AGE_DAYS)

        comun_wh = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
        if not comun_wh:
            self.stderr.write("No existe el depósito COMUN.")
            return

        sales = (
            Sale.objects.filter(
                ml_order_id__gt="",
                ml_logistic_type__in=list(ML_SELLER_FULFILLED_TYPES),
                is_cancelled=False,
                created_at__gte=since,
            )
            .select_related()
            .prefetch_related("items__product", "items__variant", "movements")
            .order_by("created_at")
        )

        if product_filter:
            sales = sales.filter(
                Q(items__product__name__icontains=product_filter)
                | Q(items__product__sku__icontains=product_filter)
            ).distinct()

        leaked_active = []  # sin movimiento, todavía dentro de la ventana de reintento
        leaked_lost = []  # sin movimiento, ya fuera de la ventana: no se va a corregir solo
        delays = []  # (sale, delay_seconds) para las que sí tienen movimiento

        for sale in sales:
            exit_movements = [m for m in sale.movements.all() if m.movement_type == StockMovement.MovementType.EXIT]
            if not exit_movements:
                if sale.created_at < cutoff:
                    leaked_lost.append(sale)
                else:
                    leaked_active.append(sale)
                continue
            first_movement = min(exit_movements, key=lambda m: m.created_at)
            delay = (first_movement.created_at - sale.created_at).total_seconds()
            delays.append((sale, delay))

        def describe(sale):
            products = ", ".join(
                sorted({f"{i.product.sku or i.product.name}" for i in sale.items.all()})
            )[:60]
            item_ids = MercadoLibreItem.objects.filter(product__in=[i.product for i in sale.items.all()])
            coexistence = any(mi.has_flex and mi.logistic_type == "fulfillment" for mi in item_ids)
            age_days = (timezone.now() - sale.created_at).total_seconds() / 86400
            flag = " [convivencia Full+Flex]" if coexistence else ""
            return (
                f"  venta #{sale.id:<6} orden ML {sale.ml_order_id:<14} "
                f"canal={sale.ml_logistic_type:<12} hace {age_days:>5.1f}d  {products}{flag}"
            )

        self.stdout.write(self.style.WARNING(f"\n=== Ventas SIN descuento aplicado, todavía a tiempo (< {ML_STOCK_EXIT_MAX_AGE_DAYS}d) ==="))
        if not leaked_active:
            self.stdout.write("  (ninguna)")
        for s in leaked_active:
            self.stdout.write(describe(s))

        self.stdout.write(self.style.ERROR(f"\n=== Ventas SIN descuento aplicado, YA PERDIDAS (> {ML_STOCK_EXIT_MAX_AGE_DAYS}d, el sync ya no las reintenta) ==="))
        if not leaked_lost:
            self.stdout.write("  (ninguna)")
        for s in leaked_lost:
            self.stdout.write(describe(s))

        self.stdout.write("\n=== Demora del descuento en ventas que SÍ tienen movimiento ===")
        if delays:
            secs = sorted(d for _, d in delays)
            n = len(secs)
            avg = sum(secs) / n
            over_15min = sum(1 for d in secs if d > 900)
            over_1h = sum(1 for d in secs if d > 3600)
            over_1d = sum(1 for d in secs if d > 86400)
            self.stdout.write(f"  ventas analizadas: {n}")
            self.stdout.write(f"  demora mínima: {secs[0]/60:.1f} min | mediana: {secs[n//2]/60:.1f} min | máxima: {secs[-1]/3600:.1f} h | promedio: {avg/60:.1f} min")
            self.stdout.write(f"  con más de 15 min de demora: {over_15min}")
            self.stdout.write(f"  con más de 1 hora de demora: {over_1h}")
            self.stdout.write(f"  con más de 1 día de demora: {over_1d}")
            peores = sorted(delays, key=lambda t: -t[1])[:15]
            self.stdout.write("\n  Las 15 con más demora:")
            for sale, delay in peores:
                self.stdout.write(f"  venta #{sale.id:<6} orden ML {sale.ml_order_id:<14} demora {delay/3600:>6.1f} h")
        else:
            self.stdout.write("  (no hay ventas con movimiento en la ventana analizada)")

        self.stdout.write(
            f"\nResumen: {len(sales)} ventas no-Full revisadas | "
            f"{len(leaked_active)} pendientes de reintento | "
            f"{len(leaked_lost)} perdidas definitivamente | "
            f"{len(delays)} con descuento aplicado"
        )
