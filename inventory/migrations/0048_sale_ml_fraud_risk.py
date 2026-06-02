from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0047_taxexpense_vat_amount"),
    ]

    operations = [
        migrations.AddField(
            model_name="sale",
            name="ml_fraud_risk",
            field=models.BooleanField(default=False),
        ),
    ]
