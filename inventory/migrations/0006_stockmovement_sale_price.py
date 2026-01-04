from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0005_customer_and_discounts"),
    ]

    operations = [
        migrations.AddField(
            model_name="stockmovement",
            name="sale_price",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12),
        ),
    ]
