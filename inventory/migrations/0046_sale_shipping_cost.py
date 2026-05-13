from decimal import Decimal
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0045_avg_cost_strip_vat"),
    ]

    operations = [
        migrations.AddField(
            model_name="sale",
            name="shipping_cost",
            field=models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12),
        ),
    ]
