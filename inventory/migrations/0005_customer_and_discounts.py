from decimal import Decimal

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0004_product_margins"),
    ]

    operations = [
        migrations.CreateModel(
            name="Customer",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=255)),
                ("email", models.EmailField(blank=True, max_length=254)),
                ("audience", models.CharField(choices=[("CONSUMER", "Consumidor final"), ("BARBER", "Peluquerías/Barberías"), ("DISTRIBUTOR", "Distribuidor")], default="CONSUMER", max_length=20)),
            ],
            options={"ordering": ["name"]},
        ),
        migrations.CreateModel(
            name="CustomerProductDiscount",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("discount_percent", models.DecimalField(decimal_places=2, default=Decimal("0.00"), max_digits=5)),
                ("customer", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="discounts", to="inventory.customer")),
                ("product", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="customer_discounts", to="inventory.product")),
            ],
            options={
                "ordering": ["customer__name", "product__sku"],
                "unique_together": {("customer", "product")},
            },
        ),
    ]
