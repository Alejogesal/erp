from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0025_ml_item_permalink_len"),
    ]

    operations = [
        migrations.AddField(
            model_name="mercadolibreitem",
            name="logistic_type",
            field=models.CharField(blank=True, default="", max_length=50),
        ),
    ]
