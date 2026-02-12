from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0036_customer_payment"),
    ]

    operations = [
        migrations.AddField(
            model_name="purchaseitem",
            name="variant",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="purchase_items",
                to="inventory.productvariant",
            ),
        ),
    ]
