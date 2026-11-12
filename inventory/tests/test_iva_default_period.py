"""IVA e Impuestos: sin fechas se muestra el mes en curso, no toda la historia
(listar cada línea de venta y compra desde siempre hacía pesar la página)."""
from datetime import timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from inventory.models import Product, Sale, SaleItem, Warehouse


class IvaDefaultPeriodTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="iva", password="secret")
        self.client.force_login(self.user)
        self.comun = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.product = Product.objects.create(sku="SKU-IVA", name="Producto IVA")
        self.today = timezone.localdate()
        self.month_start = self.today.replace(day=1)

    def _sale(self, line_total, created_at=None):
        sale = Sale.objects.create(warehouse=self.comun, total=line_total, user=self.user)
        if created_at is not None:
            Sale.objects.filter(pk=sale.pk).update(created_at=created_at)
        SaleItem.objects.create(
            sale=sale,
            product=self.product,
            quantity=Decimal("1.00"),
            unit_price=line_total,
            cost_unit=Decimal("0.00"),
            discount_percent=Decimal("0.00"),
            final_unit_price=line_total,
            line_total=line_total,
            vat_percent=Decimal("21.00"),
        )
        return sale

    def test_iva_page_defaults_to_current_month(self):
        self._sale(Decimal("100.00"))
        self._sale(Decimal("1000.00"), created_at=timezone.now() - timedelta(days=70))

        response = self.client.get(reverse("inventory_iva_position"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["start_date"], self.month_start.isoformat())
        self.assertEqual(response.context["end_date"], self.today.isoformat())
        # Solo la venta de este mes: 21 % de 100.
        self.assertEqual(response.context["debito_total"], Decimal("21.00"))
        self.assertEqual(len(response.context["debito_rows"]), 1)

    def test_iva_page_with_explicit_dates_shows_older_sales(self):
        self._sale(Decimal("100.00"))
        self._sale(Decimal("1000.00"), created_at=timezone.now() - timedelta(days=70))

        response = self.client.get(reverse("inventory_iva_position"), {"start_date": "2000-01-01"})
        self.assertEqual(response.context["debito_total"], Decimal("231.00"))
        self.assertEqual(len(response.context["debito_rows"]), 2)

    def test_taxes_page_period_section_defaults_to_current_month(self):
        response = self.client.get(reverse("inventory_taxes"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["iva_start"], self.month_start.isoformat())
        self.assertEqual(response.context["iva_end"], self.today.isoformat())

    def test_taxes_page_respects_explicit_dates(self):
        response = self.client.get(reverse("inventory_taxes"), {"iva_start": "2000-01-01"})
        self.assertEqual(response.context["iva_start"], "2000-01-01")
        self.assertEqual(response.context["iva_end"], "")
