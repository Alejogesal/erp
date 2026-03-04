from decimal import Decimal

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0038_add_kits"),
    ]

    operations = [
        migrations.CreateModel(
            name="CustomerProductPrice",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("unit_price", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("unit_cost", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                (
                    "customer",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="custom_prices", to="inventory.customer"),
                ),
                (
                    "product",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="customer_custom_prices", to="inventory.product"),
                ),
            ],
            options={
                "ordering": ["customer__name", "product__sku"],
                "unique_together": {("customer", "product")},
            },
        ),
    ]
