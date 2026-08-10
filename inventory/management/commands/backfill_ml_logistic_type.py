"""Completar el canal logístico de las ventas de MercadoLibre ya importadas.

Las ventas cargadas antes de que el ERP guardara el canal quedan con
ml_logistic_type vacío y aparecen como "Sin dato" en el filtro. Este comando les
consulta el envío a ML y les completa el tipo logístico, el shipment y el costo
de envío del vendedor.

A propósito NO toca el stock: una venta vieja ya está reflejada en el inventario
tal como quedó, y descontarla ahora la contaría dos veces. Solo completa datos.
"""

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand

from inventory import mercadolibre as ml
from inventory.models import MercadoLibreConnection, Sale


class Command(BaseCommand):
    help = "Completa ml_logistic_type/ml_shipment_id/shipping_cost en ventas ML ya importadas (no toca stock)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=200,
            help="Máximo de ventas a procesar en la corrida (default: 200).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Muestra qué se completaría, sin guardar.",
        )

    def handle(self, *args, **options):
        limit = options["limit"]
        dry_run = options["dry_run"]

        connection = (
            MercadoLibreConnection.objects.exclude(access_token="")
            .order_by("id")
            .first()
        )
        if not connection:
            self.stderr.write("No hay conexión de MercadoLibre configurada.")
            return
        access_token = ml.get_valid_access_token(connection)
        if not access_token:
            self.stderr.write("No se pudo obtener un access token válido.")
            return

        User = get_user_model()
        if not User.objects.exists():
            self.stderr.write("No hay usuarios.")
            return

        pending = (
            Sale.objects.filter(ml_logistic_type="")
            .exclude(ml_order_id="")
            .order_by("-created_at")[:limit]
        )

        updated = 0
        skipped = 0
        for sale in pending:
            try:
                order = ml._call_with_refresh(
                    connection, ml.get_order, sale.ml_order_id, access_token=access_token
                )
            except Exception as exc:
                self.stderr.write(f"Orden {sale.ml_order_id}: no se pudo leer ({exc}).")
                skipped += 1
                continue

            info = ml._resolve_shipment_info(connection, order, access_token)
            if not info["logistic_type"]:
                skipped += 1
                continue

            self.stdout.write(
                f"Venta #{sale.id} orden {sale.ml_order_id} → {info['logistic_type']}"
                + (f" envío ${info['shipping_cost']}" if info["shipping_cost"] is not None else "")
            )
            if dry_run:
                updated += 1
                continue

            sale.ml_logistic_type = info["logistic_type"]
            fields = ["ml_logistic_type"]
            if info["shipment_id"]:
                sale.ml_shipment_id = info["shipment_id"]
                fields.append("ml_shipment_id")
            if info["shipping_cost"] is not None:
                sale.shipping_cost = info["shipping_cost"]
                fields.append("shipping_cost")
            sale.save(update_fields=fields)
            updated += 1

        verb = "se completarían" if dry_run else "completadas"
        self.stdout.write(self.style.SUCCESS(f"Ventas {verb}: {updated}. Sin datos de envío: {skipped}."))
