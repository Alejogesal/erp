from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0051_afip_invoice"),
    ]

    operations = [
        migrations.AddField(
            model_name="mercadolibreitem",
            name="inventory_id",
            field=models.CharField(
                blank=True,
                default="",
                max_length=50,
                help_text="ID de inventario Full (compartido entre publicación tradicional y de catálogo)",
            ),
        ),
    ]
