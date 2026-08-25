from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0064_ml_item_full_only"),
    ]

    operations = [
        migrations.AddField(
            model_name="supplierproduct",
            name="supplier_name",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Nombre del producto tal como lo lista este proveedor",
                max_length=255,
            ),
        ),
    ]
