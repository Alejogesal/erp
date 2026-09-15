"""Diagnóstico de sólo lectura: estado de las conexiones de MercadoLibre por empresa.

Sirve para confirmar si al conectar la cuenta de una empresa nueva se pisó la
conexión de otra (bug en mercadolibre_callback: al resolver mal la empresa,
el OneToOneField hace que el token de la cuenta nueva se guarde encima de una
conexión ya existente en vez de crear una propia).

No modifica nada — solo compara cada Company contra su MercadoLibreConnection
y contra a qué empresa apuntan realmente las ventas/publicaciones que tiene
sincronizadas, que es un rastro que el token no puede borrar.

Uso:
  python manage.py diagnose_ml_connections
"""
from django.core.management.base import BaseCommand

from inventory.models import Company, MercadoLibreConnection, MercadoLibreItem, Sale


class Command(BaseCommand):
    help = "Lista las conexiones de MercadoLibre por empresa y detecta pisadas entre cuentas."

    def handle(self, *args, **options):
        companies = Company.objects.order_by("name")
        connections = {c.company_id: c for c in MercadoLibreConnection.objects.select_related("company")}

        self.stdout.write("=== Conexiones de MercadoLibre por empresa ===\n")
        seen_ml_user_ids: dict[str, str] = {}
        for company in companies:
            conn = connections.get(company.id)
            if not conn:
                self.stdout.write(f"  {company.name:<20} SIN conexión")
                continue
            items_count = MercadoLibreItem.objects.filter(company=company).count()
            sales_count = Sale.objects.filter(company=company, ml_order_id__gt="").count()
            self.stdout.write(
                f"  {company.name:<20} nickname={conn.nickname or '(vacío)':<20} "
                f"ml_user_id={conn.ml_user_id or '(vacío)':<12} "
                f"conectada={conn.connected_at:%Y-%m-%d %H:%M}  actualizada={conn.updated_at:%Y-%m-%d %H:%M}  "
                f"publicaciones_sincronizadas={items_count}  ventas_ml={sales_count}"
            )
            if conn.ml_user_id:
                if conn.ml_user_id in seen_ml_user_ids:
                    self.stdout.write(
                        self.style.ERROR(
                            f"    *** MISMO ml_user_id que {seen_ml_user_ids[conn.ml_user_id]}: "
                            f"dos empresas apuntando a la MISMA cuenta de MercadoLibre ***"
                        )
                    )
                seen_ml_user_ids[conn.ml_user_id] = company.name

        # Conexiones huérfanas: existen pero no tienen empresa (no debería pasar
        # en el modelo actual, pero se revisa por si quedó algo de antes de la
        # migración multi-cuenta).
        huerfanas = MercadoLibreConnection.objects.filter(company__isnull=True)
        if huerfanas.exists():
            self.stdout.write(self.style.WARNING("\n=== Conexiones sin empresa asignada ==="))
            for conn in huerfanas:
                self.stdout.write(f"  id={conn.id} nickname={conn.nickname} ml_user_id={conn.ml_user_id}")

        self.stdout.write(
            "\nSi el nickname/ml_user_id de una empresa no es el que esperás para esa cuenta, "
            "esa conexión está pisada: hay que reautorizarla desde el panel (Conectar MercadoLibre) "
            "asegurándose de tener ESA empresa elegida en el selector antes de apretar el botón."
        )
