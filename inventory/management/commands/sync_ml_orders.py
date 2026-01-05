from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from django.utils import timezone
import os

from inventory import mercadolibre as ml
from inventory.models import MercadoLibreConnection


class Command(BaseCommand):
    help = "Sync MercadoLibre orders for all connected accounts."

    def handle(self, *args, **options):
        days_env = os.environ.get("ML_ORDERS_DAYS", "")
        days = int(days_env) if days_env.isdigit() else 1
        User = get_user_model()
        sync_user = (
            User.objects.filter(is_superuser=True).order_by("id").first()
            or User.objects.order_by("id").first()
        )
        if not sync_user:
            self.stderr.write("No users available to sync orders.")
            return
        total_created = 0
        total_updated = 0
        total_reviewed = 0
        reasons = {}
        for connection in MercadoLibreConnection.objects.exclude(access_token=""):
            result = ml.sync_recent_orders(connection, sync_user, days=days)
            total_created += result.get("created", 0)
            total_updated += result.get("updated", 0)
            total_reviewed += result.get("total", 0)
            for key, count in result.get("reasons", {}).items():
                reasons[key] = reasons.get(key, 0) + count
        reason_text = ", ".join([f"{k}:{v}" for k, v in reasons.items()]) if reasons else "none"
        self.stdout.write(
            f"[{timezone.now():%Y-%m-%d %H:%M:%S}] "
            f"Orders reviewed:{total_reviewed} created:{total_created} updated:{total_updated} "
            f"reasons:{reason_text}"
        )
