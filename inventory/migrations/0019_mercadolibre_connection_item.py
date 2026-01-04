from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0018_mercadolibre_notification"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="MercadoLibreConnection",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("access_token", models.TextField(blank=True, default="")),
                ("refresh_token", models.TextField(blank=True, default="")),
                ("expires_at", models.DateTimeField(blank=True, null=True)),
                ("ml_user_id", models.CharField(blank=True, default="", max_length=50)),
                ("nickname", models.CharField(blank=True, default="", max_length=100)),
                ("last_sync_at", models.DateTimeField(blank=True, null=True)),
                ("last_metrics", models.TextField(blank=True, default="")),
                ("last_metrics_at", models.DateTimeField(blank=True, null=True)),
                ("connected_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "user",
                    models.OneToOneField(
                        on_delete=models.CASCADE, related_name="ml_connection", to=settings.AUTH_USER_MODEL
                    ),
                ),
            ],
            options={
                "ordering": ["-connected_at"],
            },
        ),
        migrations.CreateModel(
            name="MercadoLibreItem",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("item_id", models.CharField(max_length=50, unique=True)),
                ("title", models.CharField(blank=True, default="", max_length=255)),
                ("status", models.CharField(blank=True, default="", max_length=50)),
                ("permalink", models.URLField(blank=True, default="")),
                ("available_quantity", models.IntegerField(default=0)),
                ("matched_name", models.CharField(blank=True, default="", max_length=255)),
                ("last_synced", models.DateTimeField(auto_now=True)),
                (
                    "product",
                    models.ForeignKey(
                        blank=True, null=True, on_delete=models.SET_NULL, related_name="ml_items", to="inventory.product"
                    ),
                ),
            ],
            options={
                "ordering": ["-last_synced"],
            },
        ),
    ]
