from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from django.utils import timezone

from inventory import mercadolibre as ml
from inventory.models import MercadoLibreConnection


class Command(BaseCommand):
    help = "Sincroniza stock de MercadoLibre para la cuenta conectada."

    def handle(self, *args, **options):
        connection = MercadoLibreConnection.objects.first()
        if not connection:
            self.stdout.write(self.style.ERROR("No hay conexión de MercadoLibre configurada."))
            return

        User = get_user_model()
        sync_user = User.objects.filter(is_superuser=True).order_by("id").first() or User.objects.order_by("id").first()
        if not sync_user:
            self.stdout.write(self.style.ERROR("No hay usuarios para ejecutar la sincronización."))
            return

        ts = timezone.now().strftime("%Y-%m-%d %H:%M:%S")
        # El sync de stock debe recorrer TODAS las publicaciones: si se trunca
        # (ML_SYNC_MAX_ITEMS), las publicaciones fuera de la ventana quedan con
        # stock viejo para siempre. ignore_env_limit fuerza el scan completo.
        result = ml.sync_items_and_stock(connection, sync_user, ignore_env_limit=True)
        if result.metrics.get("error") == "unauthorized" or (result.total_items == 0 and result.matched == 0 and not result.metrics):
            self.stderr.write(
                f"[{ts}] sync_ml_stock FALLÓ: token inválido o expirado — se requiere reautorizar con MercadoLibre."
            )
            return
        self.stdout.write(
            f"[{ts}] Sync OK. Items: {result.total_items}, Matcheados: {result.matched}, "
            f"Sin match: {result.unmatched}, Stock actualizado: {result.updated_stock}."
        )
