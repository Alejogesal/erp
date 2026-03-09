from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0040_add_purchase_shipping_cost"),
    ]

    operations = [
        migrations.CreateModel(
            name="SupplierPayment",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("amount", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                (
                    "method",
                    models.CharField(
                        choices=[
                            ("CASH", "Efectivo"),
                            ("TRANSFER", "Transferencia"),
                            ("CARD", "Tarjeta"),
                            ("MERCADOPAGO", "MercadoPago"),
                            ("OTHER", "Otro"),
                        ],
                        default="CASH",
                        max_length=20,
                    ),
                ),
                (
                    "kind",
                    models.CharField(
                        choices=[("PAYMENT", "Pago"), ("ADJUSTMENT", "Ajuste/Devolución")],
                        default="PAYMENT",
                        max_length=20,
                    ),
                ),
                ("paid_at", models.DateField(default=django.utils.timezone.localdate)),
                ("notes", models.CharField(blank=True, default="", max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "purchase",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="payments",
                        to="inventory.purchase",
                    ),
                ),
                (
                    "supplier",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="payments",
                        to="inventory.supplier",
                    ),
                ),
            ],
            options={
                "ordering": ["-paid_at", "-id"],
            },
        ),
    ]
