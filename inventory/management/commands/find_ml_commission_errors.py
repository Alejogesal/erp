"""Cuenta (y lista) las ventas de MercadoLibre con la comisión mal calculada.

Bug: cuando ML no devolvía `fee_details`, se guardaba `sale_fee` (comisión POR
UNIDAD) como si fuera el total. Las ventas con líneas de más de 1 unidad quedaron
con la comisión de menos unidades de las vendidas, y el impuesto se estimó como
3,5% de esa comisión. Esa combinación es la firma de las ventas afectadas.

Uso (en el server / Docker):
    python manage.py find_ml_commission_errors          # resumen + listado
    python manage.py find_ml_commission_errors --full   # muestra todas las filas
"""

from decimal import Decimal, ROUND_HALF_EVEN

from django.core.management.base import BaseCommand
from django.db.models import Max

from inventory.models import Sale, Warehouse

TAX_RATE = Decimal("0.035")
CENT = Decimal("0.01")


def _looks_estimated(commission: Decimal, tax: Decimal) -> bool:
    """True si el impuesto guardado == 3,5% de la comisión (firma del fallback viejo)."""
    if commission <= 0:
        return False
    estimated = (commission * TAX_RATE).quantize(CENT, rounding=ROUND_HALF_EVEN)
    # tolerancia de 1 centavo por diferencias de redondeo
    return abs(tax - estimated) <= CENT


class Command(BaseCommand):
    help = "Cuenta las ventas ML con comisión mal calculada (comisión de 1 unidad en ventas de varias)."

    def add_arguments(self, parser):
        parser.add_argument("--full", action="store_true", help="Lista todas las ventas afectadas (no solo las primeras 30).")

    def handle(self, *args, **options):
        ml_sales = (
            Sale.objects.filter(warehouse__type=Warehouse.WarehouseType.MERCADOLIBRE)
            .prefetch_related("items")
        )
        total_ml = ml_sales.count()

        affected = []
        multi_unit_total = 0
        for sale in ml_sales:
            max_qty = max((it.quantity for it in sale.items.all()), default=Decimal("0"))
            if max_qty <= 1:
                continue
            multi_unit_total += 1
            if _looks_estimated(sale.ml_commission_total, sale.ml_tax_total):
                units = sum((it.quantity for it in sale.items.all()), Decimal("0"))
                affected.append((sale, units, max_qty))

        self.stdout.write("")
        self.stdout.write(f"Ventas ML totales:                         {total_ml}")
        self.stdout.write(f"Ventas ML con alguna línea > 1 unidad:     {multi_unit_total}")
        self.stdout.write(self.style.WARNING(
            f"Ventas AFECTADAS (comisión mal calculada): {len(affected)}"
        ))
        self.stdout.write("")

        if not affected:
            self.stdout.write(self.style.SUCCESS("No se detectaron ventas afectadas. 🎉"))
            return

        self.stdout.write("Detalle (comisión/impuesto que hay que corregir con 'Recalcular TODAS'):")
        self.stdout.write(
            f"  {'Venta':>7}  {'Orden ML':>16}  {'Fecha':>10}  {'Unid':>5}  "
            f"{'Comisión':>11}  {'Impuesto':>10}"
        )
        limit = None if options["full"] else 30
        for sale, units, _max_qty in affected[:limit] if limit else affected:
            fecha = sale.created_at.strftime("%Y-%m-%d") if sale.created_at else "-"
            self.stdout.write(
                f"  {sale.pk:>7}  {sale.ml_order_id:>16}  {fecha:>10}  {units:>5}  "
                f"{sale.ml_commission_total:>11}  {sale.ml_tax_total:>10}"
            )
        if limit and len(affected) > limit:
            self.stdout.write(f"  ... y {len(affected) - limit} más (usá --full para verlas todas).")

        self.stdout.write("")
        self.stdout.write(
            "Para corregirlas: dashboard de MercadoLibre → botón 'Recalcular TODAS' "
            "(re-trae los montos reales de ML)."
        )
