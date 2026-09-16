from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('inventory', '0068_productvariant_sku'),
    ]

    operations = [
        migrations.CreateModel(
            name='ExcludedBrand',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('group', models.CharField(help_text='Marca / grupo', max_length=100, unique=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'ordering': ['group'],
            },
        ),
    ]
