"""Recalcula comisión e impuestos de ventas de MercadoLibre re-trayendo los
montos reales desde ML. Pensado para correr en consola (sin timeout web).

Uso (en el server / Docker / Easypanel console):
    python manage.py resync_ml_commissions            # solo las que quedaron en $0
    python manage.py resync_ml_commissions --all      # TODAS (corrige montos ya guardados)
    python manage.py resync_ml_commissions --all --limit 50   # de a tandas
    python manage.py resync_ml_commissions --only-affected     # solo las detectadas como mal calculadas
"""

from decimal import Decimal, ROUND_HALF_EVEN

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model

from inventory import mercadolibre as ml
from inventory.models import MercadoLibreConnection, Sale, Warehouse

TAX_RATE = Decimal("0.035")
CENT = Decimal("0.01")


def _looks_estimated(commission: Decimal, tax: Decimal) -> bool:
    if commission <= 0:
        return False
    estimated = (commission * TAX_RATE).quantize(CENT, rounding=ROUND_HALF_EVEN)
    return abs(tax - estimated) <= CENT


class Command(BaseCommand):
    help = "Recalcula comisiones/impuestos de ventas ML con datos reales de MercadoLibre."

    def add_arguments(self, parser):
        parser.add_argument("--all", action="store_true", help="Recalcula TODAS las ventas ML (no solo las de comisión $0).")
        parser.add_argument("--only-affected", action="store_true", help="Solo las detectadas como mal calculadas (multi-unidad + impuesto 3,5%%).")
        parser.add_argument("--limit", type=int, default=0, help="Procesar como máximo N ventas (0 = sin límite).")

    def handle(self, *args, **options):
        User = get_user_model()
        user = (
            User.objects.filter(is_superuser=True).order_by("id").first()
            or User.objects.order_by("id").first()
        )
        connection = MercadoLibreConnection.objects.exclude(access_token="").first()
        if not connection:
            self.stderr.write(self.style.ERROR("No hay conexión de MercadoLibre configurada."))
            return
        if not user:
            self.stderr.write(self.style.ERROR("No hay usuarios disponibles."))
            return

        qs = Sale.objects.filter(
            warehouse__type=Warehouse.WarehouseType.MERCADOLIBRE,
            ml_order_id__gt="",
        ).prefetch_related("items")

        if options["only_affected"]:
            ids = []
            for s in qs:
                qtys = [i.quantity for i in s.items.all()]
                if qtys and max(qtys) > 1 and _looks_estimated(s.ml_commission_total, s.ml_tax_total):
                    ids.append(s.pk)
            qs = Sale.objects.filter(pk__in=ids)
        elif not options["all"]:
            qs = qs.filter(ml_commission_total=Decimal("0.00"))

        qs = qs.order_by("id")
        if options["limit"]:
            qs = qs[: options["limit"]]

        sales = list(qs)
        total = len(sales)
        self.stdout.write(f"Ventas a procesar: {total}")
        if total == 0:
            return

        updated = skipped = errors = 0
        for i, sale in enumerate(sales, 1):
            before_c, before_t = sale.ml_commission_total, sale.ml_tax_total
            try:
                ok, reason = ml.sync_order(connection, sale.ml_order_id, user)
            except Exception as exc:  # noqa: BLE001
                errors += 1
                self.stdout.write(self.style.WARNING(f"[{i}/{total}] venta #{sale.pk} error: {exc}"))
                continue
            if ok:
                sale.refresh_from_db()
                if (sale.ml_commission_total, sale.ml_tax_total) != (before_c, before_t):
                    updated += 1
                    self.stdout.write(
                        f"[{i}/{total}] venta #{sale.pk}: comisión {before_c}→{sale.ml_commission_total} | "
                        f"impuesto {before_t}→{sale.ml_tax_total}"
                    )
                else:
                    skipped += 1
            else:
                skipped += 1
                self.stdout.write(f"[{i}/{total}] venta #{sale.pk} sin cambios ({reason}).")

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            f"Listo. Corregidas: {updated} | sin cambios: {skipped} | con error: {errors}"
        ))
