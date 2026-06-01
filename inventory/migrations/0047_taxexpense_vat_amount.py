from decimal import Decimal
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0046_sale_shipping_cost"),
    ]

    operations = [
        migrations.AddField(
            model_name="taxexpense",
            name="vat_amount",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12),
        ),
    ]
