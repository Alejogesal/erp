from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0033_add_purchase_discount_percent"),
    ]

    operations = [
        migrations.AddField(
            model_name="sale",
            name="delivery_status",
            field=models.CharField(
                choices=[
                    ("NOT_DELIVERED", "No entregado"),
                    ("IN_TRANSIT", "Stock en camino"),
                    ("DELIVERED", "Entregado"),
                ],
                default="NOT_DELIVERED",
                max_length=20,
            ),
        ),
    ]
