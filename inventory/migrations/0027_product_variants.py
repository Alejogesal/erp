from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0026_ml_item_logistic_type"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProductVariant",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=255)),
                ("quantity", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=12)),
                (
                    "product",
                    models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="variants", to="inventory.product"),
                ),
            ],
            options={
                "ordering": ["name", "id"],
                "unique_together": {("product", "name")},
            },
        ),
    ]
