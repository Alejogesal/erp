from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0002_default_warehouses"),
    ]

    operations = [
        migrations.AddField(
            model_name="product",
            name="price_barber",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12),
        ),
        migrations.AddField(
            model_name="product",
            name="price_consumer",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12),
        ),
        migrations.AddField(
            model_name="product",
            name="price_distributor",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12),
        ),
    ]
