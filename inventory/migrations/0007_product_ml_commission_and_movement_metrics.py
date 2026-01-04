from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0006_stockmovement_sale_price"),
    ]

    operations = [
        migrations.AddField(
            model_name="product",
            name="ml_commission_percent",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="Comisión MercadoLibre %",
                max_digits=5,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="stockmovement",
            name="ml_commission_percent",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=5),
        ),
        migrations.AddField(
            model_name="stockmovement",
            name="profit",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12),
        ),
        migrations.AddField(
            model_name="stockmovement",
            name="retention_percent",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=5),
        ),
        migrations.AddField(
            model_name="stockmovement",
            name="sale_net",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12),
        ),
    ]
