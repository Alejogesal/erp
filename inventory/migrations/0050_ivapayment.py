import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0049_product_min_stock"),
    ]

    operations = [
        migrations.CreateModel(
            name="IVAPayment",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tipo", models.CharField(
                    max_length=10,
                    choices=[("DEBITO", "Débito fiscal"), ("CREDITO", "Crédito fiscal"), ("SALDO", "Saldo a pagar")],
                    default="SALDO",
                )),
                ("amount", models.DecimalField(decimal_places=2, max_digits=12)),
                ("period", models.DateField(help_text="Período que cubre el pago (primer día del mes)")),
                ("paid_at", models.DateField(default=django.utils.timezone.localdate)),
                ("notes", models.CharField(blank=True, default="", max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["-paid_at", "-id"],
            },
        ),
    ]
