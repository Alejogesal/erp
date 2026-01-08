from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model

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

        result = ml.sync_items_and_stock(connection, sync_user)
        self.stdout.write(
            self.style.SUCCESS(
                f"Sync OK. Items: {result.total_items}, Matcheados: {result.matched}, "
                f"Sin match: {result.unmatched}, Stock actualizado: {result.updated_stock}."
            )
        )
