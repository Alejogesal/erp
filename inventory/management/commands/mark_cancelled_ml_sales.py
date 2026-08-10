"""Marca como CANCELADAS las ventas de MercadoLibre cuya orden figura cancelada o
expirada en ML. Las ventas no se borran: quedan en el historial pero dejan de
contar en ventas y ganancias.

Va orden por orden consultando ML, así que puede tardar. Correr en consola.

Uso:
    python manage.py mark_cancelled_ml_sales            # revisa todas las ventas ML activas
    python manage.py mark_cancelled_ml_sales --limit 100
"""

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model

from inventory import mercadolibre as ml
from inventory.models import MercadoLibreConnection, Sale, Warehouse


class Command(BaseCommand):
    help = "Marca como canceladas las ventas ML cuya orden está cancelada/expirada en MercadoLibre."

    def add_arguments(self, parser):
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
            is_cancelled=False,
        ).order_by("id")
        if options["limit"]:
            qs = qs[: options["limit"]]

        sales = list(qs)
        total = len(sales)
        self.stdout.write(f"Ventas ML activas a revisar: {total}")
        if total == 0:
            return

        cancelled = active = errors = 0
        for i, sale in enumerate(sales, 1):
            try:
                ok, reason = ml.sync_order(connection, sale.ml_order_id, user)
            except Exception as exc:  # noqa: BLE001
                errors += 1
                self.stdout.write(self.style.WARNING(f"[{i}/{total}] venta #{sale.pk} error: {exc}"))
                continue
            if reason == "cancelled_marked":
                cancelled += 1
                self.stdout.write(f"[{i}/{total}] venta #{sale.pk} (orden {sale.ml_order_id}): marcada CANCELADA")
            else:
                active += 1

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            f"Listo. Marcadas canceladas: {cancelled} | siguen activas: {active} | con error: {errors}"
        ))
