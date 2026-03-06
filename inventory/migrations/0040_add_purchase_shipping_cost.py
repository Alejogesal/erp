from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0039_customerproductprice"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchase",
            name="shipping_cost",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12),
        ),
    ]
