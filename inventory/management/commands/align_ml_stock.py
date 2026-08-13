"""Alinear el stock publicado en MercadoLibre con el del ERP.

El push automático solo corre después de una venta, al guardar el match en el
panel y —si ML_STOCK_RECONCILE=1— en el barrido de stock. Cuando una
publicación quedó desfasada por fuera de esos caminos (o el push falló y el
error se perdió), este comando la empuja a mano y dice qué pasó con cada una.

Sin --apply no toca nada: muestra qué haría.
"""
from django.core.management.base import BaseCommand

from inventory import mercadolibre as ml
from inventory.models import MercadoLibreConnection, MercadoLibreItem


class Command(BaseCommand):
    help = "Compara el stock del ERP con el publicado en ML y (con --apply) lo corrige."

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Empujar de verdad. Sin esto solo se lista lo que se haría.",
        )
        parser.add_argument(
            "--item",
            action="append",
            default=[],
            help="Limitar a estas publicaciones (MLAxxxx). Se puede repetir.",
        )
        parser.add_argument(
            "--only-diff",
            action="store_true",
            help="Empujar solo las publicaciones cuyo número difiere del del ERP.",
        )

    def handle(self, *args, **options):
        connection = ml._default_connection()
        if not connection or not connection.access_token:
            self.stderr.write("No hay conexión de MercadoLibre configurada.")
            return
        access_token = ml.get_valid_access_token(connection)
        if not access_token:
            self.stderr.write("Token inválido o expirado: reautorizá la cuenta desde el panel.")
            return

        items = MercadoLibreItem.objects.select_related("product", "variant")
        if options["item"]:
            items = items.filter(item_id__in=options["item"])
        items = items.order_by("title")

        rows = ml.stock_alignment_rows(items)
        if not rows:
            self.stdout.write("No hay publicaciones para revisar.")
            return

        apply_changes = options["apply"]
        if apply_changes:
            pushed, failed = ml.apply_stock_alignment(
                rows, access_token, only_diff=options["only_diff"]
            )
        else:
            pushed = failed = 0

        for row in sorted(rows, key=lambda r: (r["action"] == ml.ALIGN_SKIP, r["title"])):
            erp = "—" if row["erp_qty"] is None else str(row["erp_qty"])
            head = f"{row['item_id']:<16} ERP {erp:>4} | ML {row['ml_qty']:>4}  {row['title'][:45]}"
            if row["error"]:
                self.stdout.write(self.style.ERROR(f"{head}  ✗ {row['error']}"))
            elif row["result"] == "ok":
                self.stdout.write(self.style.SUCCESS(f"{head}  ✓ publicado {row['erp_qty']}"))
            elif row["action"] == ml.ALIGN_SKIP:
                self.stdout.write(f"{head}  – {row['reason']}")
            else:
                self.stdout.write(self.style.WARNING(f"{head}  → publicaría {row['erp_qty']}"))

        pendientes = sum(1 for r in rows if r["action"] != ml.ALIGN_SKIP)
        sin_variedad = sum(1 for r in rows if r["reason"] == "falta elegir la variedad")
        self.stdout.write("")
        if apply_changes:
            self.stdout.write(f"Publicaciones actualizadas: {pushed}. Con error: {failed}.")
        else:
            self.stdout.write(
                f"Simulación: {pendientes} publicación(es) se empujarían. "
                "Repetí con --apply para hacerlo."
            )
        if sin_variedad:
            self.stdout.write(
                self.style.WARNING(
                    f"{sin_variedad} publicación(es) de productos con variedades siguen sin "
                    "variedad elegida: enlazalas en el panel para que reciban stock."
                )
            )
