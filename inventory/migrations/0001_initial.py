from decimal import Decimal

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="Warehouse",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=100)),
                ("type", models.CharField(choices=[("MERCADOLIBRE", "MercadoLibre"), ("COMUN", "Comun")], max_length=20, unique=True)),
            ],
            options={"ordering": ["name"]},
        ),
        migrations.CreateModel(
            name="Product",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("sku", models.CharField(max_length=64, unique=True)),
                ("name", models.CharField(max_length=255)),
                ("avg_cost", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("target_margin", models.DecimalField(decimal_places=2, default=Decimal("0.00"), help_text="Desired margin percentage (e.g. 25.00 for 25%)", max_digits=5)),
            ],
            options={"ordering": ["sku"]},
        ),
        migrations.CreateModel(
            name="Stock",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("quantity", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("product", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="stocks", to="inventory.product")),
                ("warehouse", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="stocks", to="inventory.warehouse")),
            ],
            options={
                "ordering": ["product__sku", "warehouse__name"],
                "unique_together": {("product", "warehouse")},
            },
        ),
        migrations.CreateModel(
            name="StockMovement",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("movement_type", models.CharField(choices=[("ENTRY", "Entrada"), ("EXIT", "Salida"), ("TRANSFER", "Transferencia"), ("ADJUSTMENT", "Ajuste")], max_length=20)),
                ("quantity", models.DecimalField(decimal_places=2, max_digits=12)),
                ("unit_cost", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("reference", models.CharField(blank=True, default="", max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("from_warehouse", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="outgoing_movements", to="inventory.warehouse")),
                ("product", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="movements", to="inventory.product")),
                ("to_warehouse", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="incoming_movements", to="inventory.warehouse")),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="stock_movements", to=settings.AUTH_USER_MODEL)),
            ],
            options={"ordering": ["-created_at", "-id"]},
        ),
    ]
