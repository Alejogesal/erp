from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0052_mercadolibreitem_inventory_id"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="mercadolibreitem",
            name="inventory_id",
        ),
        migrations.AddField(
            model_name="mercadolibreitem",
            name="user_product_id",
            field=models.CharField(
                blank=True,
                default="",
                max_length=50,
                help_text="user_product_id de ML; compartido entre publicación tradicional y de catálogo del mismo producto (agrupa el stock Full)",
            ),
        ),
    ]
