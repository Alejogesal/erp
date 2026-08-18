# Solo el campo nuevo de MercadoLibreItem. `makemigrations` arrastra además
# drift viejo de otros modelos (constraints de afipinvoice, decimales) que no
# tiene nada que ver con esto y no conviene aplicar de arrastre en producción.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('inventory', '0063_mercadolibreitem_variant'),
    ]

    operations = [
        migrations.AddField(
            model_name='mercadolibreitem',
            name='full_only',
            field=models.BooleanField(
                default=False,
                help_text='ML rechazó el stock del depósito propio por ser fulfillment-only. Es la confirmación de que no hay convivencia: el ERP deja de intentar publicarle stock.',
            ),
        ),
        migrations.AlterField(
            model_name='mercadolibreitem',
            name='has_flex',
            field=models.BooleanField(
                default=False,
                help_text='La publicación vende desde el depósito propio: tag self_service_in en /items o una ubicación selling_address en el stock del user_product. Con logistic_type=fulfillment indica convivencia Full/Flex.',
            ),
        ),
    ]
