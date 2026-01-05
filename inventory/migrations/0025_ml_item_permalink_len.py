from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0024_sale_ml_fields"),
    ]

    operations = [
        migrations.AlterField(
            model_name="mercadolibreitem",
            name="permalink",
            field=models.URLField(blank=True, default="", max_length=500),
        ),
    ]
