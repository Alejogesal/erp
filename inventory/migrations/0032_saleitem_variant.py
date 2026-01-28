from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0031_add_purchase_invoice_image"),
    ]

    operations = [
        migrations.AddField(
            model_name="saleitem",
            name="variant",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="sale_items",
                to="inventory.productvariant",
            ),
        ),
    ]
