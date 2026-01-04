from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0016_update_product_margins"),
    ]

    operations = [
        migrations.AlterField(
            model_name="product",
            name="sku",
            field=models.CharField(blank=True, max_length=64, null=True, unique=True),
        ),
    ]
