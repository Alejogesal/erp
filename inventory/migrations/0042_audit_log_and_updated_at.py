from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0041_supplier_payment"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # AuditLog model
        migrations.CreateModel(
            name="AuditLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("timestamp", models.DateTimeField(auto_now_add=True)),
                ("action", models.CharField(choices=[("CREATE", "Creación"), ("UPDATE", "Edición"), ("DELETE", "Eliminación")], max_length=10)),
                ("model_name", models.CharField(max_length=100)),
                ("object_id", models.PositiveIntegerField(blank=True, null=True)),
                ("object_repr", models.CharField(max_length=255)),
                ("changes", models.JSONField(blank=True, null=True)),
                ("user", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="audit_logs", to=settings.AUTH_USER_MODEL)),
            ],
            options={"ordering": ["-timestamp"]},
        ),
        # updated_at on Product
        migrations.AddField(
            model_name="product",
            name="updated_at",
            field=models.DateTimeField(auto_now=True),
        ),
        # updated_at on Customer
        migrations.AddField(
            model_name="customer",
            name="updated_at",
            field=models.DateTimeField(auto_now=True),
        ),
        # updated_at on Supplier
        migrations.AddField(
            model_name="supplier",
            name="updated_at",
            field=models.DateTimeField(auto_now=True),
        ),
        # updated_at on Sale
        migrations.AddField(
            model_name="sale",
            name="updated_at",
            field=models.DateTimeField(auto_now=True),
        ),
        # updated_at on Purchase
        migrations.AddField(
            model_name="purchase",
            name="updated_at",
            field=models.DateTimeField(auto_now=True),
        ),
    ]
