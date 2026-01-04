from django.db import migrations, models
from decimal import Decimal


def apply_default_margins(apps, schema_editor):
    Product = apps.get_model("inventory", "Product")
    Product.objects.filter(margin_consumer=Decimal("0.00")).update(margin_consumer=Decimal("25.00"))
    Product.objects.filter(margin_barber=Decimal("0.00")).update(margin_barber=Decimal("20.00"))
    Product.objects.filter(margin_distributor=Decimal("0.00")).update(margin_distributor=Decimal("15.00"))


class Migration(migrations.Migration):
    dependencies = [
        ("inventory", "0015_customergroupdiscount"),
    ]

    operations = [
        migrations.AlterField(
            model_name="product",
            name="margin_consumer",
            field=models.DecimalField(
                decimal_places=2,
                default=Decimal("25.00"),
                help_text="Margen % consumidor final",
                max_digits=5,
            ),
        ),
        migrations.AlterField(
            model_name="product",
            name="margin_barber",
            field=models.DecimalField(
                decimal_places=2,
                default=Decimal("20.00"),
                help_text="Margen % peluquerías/barberías",
                max_digits=5,
            ),
        ),
        migrations.AlterField(
            model_name="product",
            name="margin_distributor",
            field=models.DecimalField(
                decimal_places=2,
                default=Decimal("15.00"),
                help_text="Margen % distribuidores",
                max_digits=5,
            ),
        ),
        migrations.RunPython(apply_default_margins, migrations.RunPython.noop),
    ]
