from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0033_add_purchase_discount_percent"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseitem",
            name="discount_percent",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=5),
        ),
    ]
