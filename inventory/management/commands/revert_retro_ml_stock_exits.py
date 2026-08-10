"""Revertir los descuentos retroactivos de stock de ventas ML viejas.

`sync_ml_orders` re-sincroniza hasta 90 días hacia atrás. Al agregarse el
descuento de stock para ventas no-Full, ese re-sync se lo aplicó también a
ventas anteriores a la funcionalidad, cuyo stock ya estaba reflejado en el
depósito: quedaron descontadas dos veces (hubo productos en negativo).

Se identifican por el desfasaje entre cuándo se hizo la venta y cuándo se creó
el movimiento: el movimiento se registró mucho después que la venta. Los
descuentos legítimos se crean junto con la venta, o pocas horas después.

Devuelve el stock y borra esos movimientos, para que la venta quede como estaba
antes del despliegue.
"""

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db import transaction

from inventory import services
from inventory.models import StockMovement, Warehouse


class Command(BaseCommand):
    help = "Revierte los descuentos de stock aplicados retroactivamente a ventas ML viejas."

    def add_arguments(self, parser):
        parser.add_argument(
            "--max-age-days",
            type=int,
            default=3,
            help=(
                "Se revierte el movimiento si la venta es más vieja que esto al momento "
                "en que se registró el movimiento (default: 3, igual que ML_STOCK_EXIT_MAX_AGE_DAYS)."
            ),
        )
        parser.add_argument("--dry-run", action="store_true", help="Muestra qué se revertiría, sin tocar nada.")

    def handle(self, *args, **options):
        max_age = timedelta(days=options["max_age_days"])
        dry_run = options["dry_run"]

        comun = Warehouse.objects.filter(type=Warehouse.WarehouseType.COMUN).first()
        if not comun:
            self.stderr.write("No existe el depósito COMUN.")
            return

        candidatos = (
            StockMovement.objects.filter(
                movement_type=StockMovement.MovementType.EXIT,
                from_warehouse=comun,
                reference__startswith="Venta ML ",
            )
            .exclude(sale=None)
            .select_related("sale", "product")
            .order_by("id")
        )

        retro = [m for m in candidatos if m.sale.created_at and m.created_at - m.sale.created_at > max_age]

        if not retro:
            self.stdout.write(self.style.SUCCESS("No hay descuentos retroactivos para revertir."))
            return

        por_producto: dict[str, int] = {}
        for m in retro:
            sku = m.product.sku or m.product.name
            por_producto[sku] = por_producto.get(sku, 0) + int(m.quantity)

        for sku, qty in sorted(por_producto.items(), key=lambda kv: -kv[1]):
            self.stdout.write(f"  {sku[:34]:34} devuelve {qty:>6}")
        self.stdout.write(f"Movimientos a revertir: {len(retro)} sobre {len(por_producto)} productos.")

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run: no se modificó nada."))
            return

        with transaction.atomic():
            for m in retro:
                services.register_adjustment(
                    product=m.product,
                    warehouse=comun,
                    quantity=m.quantity,
                    user=m.user,
                    reference=f"Reversa descuento retroactivo venta ML {m.sale.ml_order_id or m.sale_id}",
                    allow_negative=True,
                )
            StockMovement.objects.filter(id__in=[m.id for m in retro]).delete()

        self.stdout.write(self.style.SUCCESS(f"Revertidos {len(retro)} movimientos."))
