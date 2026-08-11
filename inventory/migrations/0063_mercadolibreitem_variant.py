import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    """Enlace publicación → variedad.

    Los productos con variedades tienen el stock repartido entre ellas y el
    COMUN es solo la suma. Publicar esa suma en cada publicación mostraba
    unidades que no existen para esa variedad. Con el enlace, el stock que se
    empuja sale de la variedad; sin enlace no se empuja nada.
    """

    dependencies = [
        ("inventory", "0062_ml_item_stock_breakdown"),
    ]

    operations = [
        migrations.AddField(
            model_name="mercadolibreitem",
            name="variant",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="ml_items",
                to="inventory.productvariant",
                help_text=(
                    "Variedad publicada. Solo para productos con variedades: el stock que se "
                    "publica es el de esta variedad, no el total del producto."
                ),
            ),
        ),
    ]
