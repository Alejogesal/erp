from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0053_mercadolibreitem_user_product_id"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="stockmovement",
            index=models.Index(
                fields=["product", "movement_type", "-created_at"],
                name="sm_prod_type_created_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="purchase",
            index=models.Index(fields=["created_at"], name="purchase_created_idx"),
        ),
        migrations.AddIndex(
            model_name="sale",
            index=models.Index(fields=["created_at"], name="sale_created_idx"),
        ),
        migrations.AddIndex(
            model_name="taxexpense",
            index=models.Index(fields=["paid_at"], name="taxexpense_paid_idx"),
        ),
        migrations.AddIndex(
            model_name="afipinvoice",
            index=models.Index(fields=["tipo_codigo", "date"], name="afip_tipo_date_idx"),
        ),
    ]
