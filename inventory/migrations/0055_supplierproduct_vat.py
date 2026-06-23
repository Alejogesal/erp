from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0054_performance_indexes"),
    ]

    operations = [
        migrations.AddField(
            model_name="supplierproduct",
            name="vat_percent",
            field=models.DecimalField(
                max_digits=5,
                decimal_places=2,
                default=Decimal("0.00"),
                help_text="Condición de IVA de este proveedor para el producto",
            ),
        ),
    ]
