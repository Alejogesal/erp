"""Ventas ML canceladas/expiradas: se marcan como canceladas (no se borran) y
dejan de contar en ventas/ganancias."""

from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory import mercadolibre as ml
from inventory.middleware import _current_user
from inventory.models import (
    MercadoLibreConnection,
    Product,
    Sale,
    SaleItem,
    Warehouse,
)


def _reset_current_user():
    _current_user.user = None


class SyncOrderCancellationTests(TestCase):
    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="c", password="x")
        self.connection = MercadoLibreConnection.objects.create(user=self.user)
        self.ml_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.MERCADOLIBRE)
        self.sale = Sale.objects.create(
            warehouse=self.ml_wh, user=self.user,
            ml_order_id="ORDX", reference="ML ORDER ORDX", total=Decimal("1000"),
        )

    def _sync_with_status(self, status):
        def dispatch(connection, func, *args, **kwargs):
            if func is ml.get_order:
                return {"id": "ORDX", "status": status, "tags": []}
            raise AssertionError(f"llamada inesperada: {func}")
        with patch.object(ml, "get_valid_access_token", return_value="tok"), patch.object(
            ml, "_call_with_refresh", side_effect=dispatch
        ):
            return ml.sync_order(self.connection, "ORDX", self.user)

    def test_cancelled_order_marks_sale(self):
        ok, reason = self._sync_with_status("cancelled")
        self.assertFalse(ok)
        self.assertEqual(reason, "cancelled_marked")
        self.sale.refresh_from_db()
        self.assertTrue(self.sale.is_cancelled)
        self.assertIsNotNone(self.sale.cancelled_at)

    def test_expired_order_marks_sale(self):
        self._sync_with_status("expired")
        self.sale.refresh_from_db()
        self.assertTrue(self.sale.is_cancelled)

    def test_already_cancelled_returns_ignored(self):
        self.sale.is_cancelled = True
        self.sale.save(update_fields=["is_cancelled"])
        ok, reason = self._sync_with_status("cancelled")
        self.assertEqual(reason, "ignored_status")

    def test_sale_is_not_deleted(self):
        self._sync_with_status("cancelled")
        self.assertTrue(Sale.objects.filter(pk=self.sale.pk).exists())


class DashboardExcludesCancelledTests(TestCase):
    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="d", password="x")
        self.client.force_login(self.user)
        self.comun = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.product = Product.objects.create(name="P", sku="P1", margin_consumer=Decimal("0.00"))

    def _sale(self, total, cancelled):
        sale = Sale.objects.create(
            warehouse=self.comun, user=self.user, total=Decimal(total), is_cancelled=cancelled,
        )
        SaleItem.objects.create(
            sale=sale, product=self.product, quantity=Decimal("1"),
            unit_price=Decimal(total), final_unit_price=Decimal(total),
            line_total=Decimal(total), cost_unit=Decimal("0"),
        )
        return sale

    def test_cancelled_sales_excluded_from_totals(self):
        self._sale("100", cancelled=False)
        self._sale("500", cancelled=True)  # no debe contar
        resp = self.client.get(reverse("inventory_dashboard"))
        self.assertEqual(resp.status_code, 200)
        # Ventas totales: solo la no cancelada.
        self.assertEqual(resp.context["sale_total"], Decimal("100.00"))
        # Margen común: 100 (venta) - 0 (costo), sin la cancelada.
        self.assertEqual(resp.context["margin_comun"], Decimal("100.00"))
