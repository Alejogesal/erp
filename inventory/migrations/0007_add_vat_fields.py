from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0006_stockmovement_sale_price"),
    ]

    operations = [
        migrations.AddField(
            model_name="product",
            name="vat_percent",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                default=Decimal("0.00"),
                help_text="IVA %",
                max_digits=5,
            ),
        ),
        migrations.AddField(
            model_name="stockmovement",
            name="vat_percent",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                default=Decimal("0.00"),
                help_text="IVA % aplicado",
                max_digits=5,
            ),
        ),
    ]

