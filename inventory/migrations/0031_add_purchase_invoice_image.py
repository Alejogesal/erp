from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0030_backfill_saleitem_cost_unit"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchase",
            name="invoice_image",
            field=models.FileField(blank=True, null=True, upload_to="purchase_invoices/"),
        ),
    ]
