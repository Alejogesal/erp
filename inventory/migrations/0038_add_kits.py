from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0037_add_purchase_item_variant"),
    ]

    operations = [
        migrations.AddField(
            model_name="product",
            name="is_kit",
            field=models.BooleanField(default=False),
        ),
        migrations.CreateModel(
            name="KitComponent",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("quantity", models.DecimalField(decimal_places=2, default=1, max_digits=12)),
                (
                    "component",
                    models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="used_in_kits", to="inventory.product"),
                ),
                (
                    "kit",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="kit_components", to="inventory.product"),
                ),
            ],
            options={
                "ordering": ["kit__sku", "id"],
                "unique_together": {("kit", "component")},
            },
        ),
    ]
