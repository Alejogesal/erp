from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0048_sale_ml_fraud_risk"),
    ]

    operations = [
        migrations.AddField(
            model_name="product",
            name="min_stock",
            field=models.PositiveIntegerField(
                blank=True,
                null=True,
                help_text="Stock mínimo — genera alerta si el stock COMUN cae por debajo",
            ),
        ),
    ]
