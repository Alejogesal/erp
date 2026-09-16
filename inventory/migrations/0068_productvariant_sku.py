from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('inventory', '0067_fiscal_multi_company'),
    ]

    operations = [
        migrations.AddField(
            model_name='productvariant',
            name='sku',
            field=models.CharField(
                blank=True,
                help_text=(
                    'SKU propio de esta variedad puntual. Opcional: sin cargar, el matcheo '
                    'automático de MercadoLibre solo llega al producto y hay que elegir la '
                    'variedad a mano.'
                ),
                max_length=64,
                null=True,
                unique=True,
            ),
        ),
    ]
