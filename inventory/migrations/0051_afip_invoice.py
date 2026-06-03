from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0050_ivapayment"),
    ]

    operations = [
        migrations.CreateModel(
            name="AFIPInvoice",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("date", models.DateField()),
                ("tipo_codigo", models.IntegerField()),
                ("tipo_descripcion", models.CharField(blank=True, max_length=100)),
                ("punto_venta", models.IntegerField()),
                ("numero", models.IntegerField()),
                ("cae", models.CharField(blank=True, max_length=30)),
                ("cuit_emisor", models.CharField(max_length=20)),
                ("razon_social", models.CharField(blank=True, max_length=255)),
                ("iva_105", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("neto_105", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("iva_21", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("neto_21", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("iva_27", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("neto_27", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("total_iva", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("imp_total", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                ("imported_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["-date", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="afipinvoice",
            constraint=models.UniqueConstraint(
                fields=("cuit_emisor", "punto_venta", "numero", "tipo_codigo"),
                name="unique_afip_invoice",
            ),
        ),
    ]
