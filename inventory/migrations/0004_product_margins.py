from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0003_product_price_tiers"),
    ]

    operations = [
        migrations.AddField(
            model_name="product",
            name="margin_barber",
            field=models.DecimalField(
                decimal_places=2,
                default=Decimal("0.00"),
                help_text="Margen % peluquerías/barberías",
                max_digits=5,
            ),
        ),
        migrations.AddField(
            model_name="product",
            name="margin_consumer",
            field=models.DecimalField(
                decimal_places=2, default=Decimal("0.00"), help_text="Margen % consumidor final", max_digits=5
            ),
        ),
        migrations.AddField(
            model_name="product",
            name="margin_distributor",
            field=models.DecimalField(
                decimal_places=2,
                default=Decimal("0.00"),
                help_text="Margen % distribuidores",
                max_digits=5,
            ),
        ),
    ]
