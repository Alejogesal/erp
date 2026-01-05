from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0019_mercadolibre_connection_item"),
    ]

    operations = [
        migrations.AddField(
            model_name="mercadolibreitem",
            name="last_sold_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="mercadolibreitem",
            name="units_sold_30d",
            field=models.IntegerField(default=0),
        ),
    ]
