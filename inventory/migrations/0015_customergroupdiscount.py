from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0014_taxexpense"),
    ]

    operations = [
        migrations.CreateModel(
            name="CustomerGroupDiscount",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("group", models.CharField(max_length=100)),
                ("discount_percent", models.DecimalField(decimal_places=2, default=0, max_digits=5)),
                ("customer", models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="group_discounts", to="inventory.customer")),
            ],
            options={
                "ordering": ["customer__name", "group"],
                "unique_together": {("customer", "group")},
            },
        ),
    ]
