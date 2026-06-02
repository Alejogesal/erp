from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from django.utils import timezone
import os

from inventory import mercadolibre as ml
from inventory.models import MercadoLibreConnection

MAX_CATCHUP_DAYS = 90


class Command(BaseCommand):
    help = "Sync MercadoLibre orders for all connected accounts."

    def handle(self, *args, **options):
        days_env = os.environ.get("ML_ORDERS_DAYS", "")
        base_days = int(days_env) if days_env.isdigit() else 2
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
            # Smart lookback: if sync was broken for longer than base_days, catch up automatically
            if connection.last_sync_at:
                gap_days = (timezone.now() - connection.last_sync_at).total_seconds() / 86400
                effective_days = min(max(base_days, int(gap_days) + 2), MAX_CATCHUP_DAYS)
            else:
                effective_days = 30  # First run: pull last 30 days

            if effective_days > base_days:
                self.stdout.write(
                    f"[{timezone.now():%Y-%m-%d %H:%M:%S}] "
                    f"Catch-up detected: last sync {int(gap_days if connection.last_sync_at else 0)}d ago, "
                    f"extending window to {effective_days}d"
                )

            result = ml.sync_recent_orders(connection, sync_user, days=effective_days)
            total_created += result.get("created", 0)
            total_updated += result.get("updated", 0)
            total_reviewed += result.get("total", 0)
            for key, count in result.get("reasons", {}).items():
                reasons[key] = reasons.get(key, 0) + count
            if not result.get("reasons", {}).get("unauthorized"):
                connection.last_sync_at = timezone.now()
                connection.save(update_fields=["last_sync_at"])
        reason_text = ", ".join([f"{k}:{v}" for k, v in reasons.items()]) if reasons else "none"
        self.stdout.write(
            f"[{timezone.now():%Y-%m-%d %H:%M:%S}] "
            f"Orders reviewed:{total_reviewed} created:{total_created} updated:{total_updated} "
            f"reasons:{reason_text}"
        )
