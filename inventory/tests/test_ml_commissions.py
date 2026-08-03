"""Regresión: comisión e impuestos de ML deben ser de TODA la orden, no de 1 unidad.

Bug histórico: cuando ML no devolvía `fee_details`, se usaba `sale_fee` (que es la
comisión POR UNIDAD) como si fuera el total de la venta. Una venta de 6 unidades
quedaba con la comisión de 1 sola unidad y el margen salía mal.
"""

from decimal import Decimal
from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase

from inventory import mercadolibre as ml
from inventory.models import (
    MercadoLibreConnection,
    MercadoLibreItem,
    Product,
    Sale,
    SaleItem,
    Warehouse,
)


def _dispatch(payments_result):
    """Devuelve un side_effect para _call_with_refresh que responde según la func."""

    def _inner(connection, func, *args, **kwargs):
        if func is ml.get_order:
            return _inner.order
        if func is ml.get_order_payments:
            return payments_result
        raise AssertionError(f"Llamada inesperada a la API de ML: {func}")

    return _inner


class MLCommissionTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="ml", password="x")
        self.connection = MercadoLibreConnection.objects.create(user=self.user)
        self.product = Product.objects.create(sku="P1", name="Producto ML")
        MercadoLibreItem.objects.create(item_id="MLA123", product=self.product)

    def _order(self, **extra):
        base = {
            "id": "ORD1",
            "status": "paid",
            "date_created": "2026-08-03T10:00:00.000-03:00",
            "total_amount": 31794,
            "tags": ["paid"],
            "order_items": [
                {
                    "item": {"id": "MLA123", "title": "Producto ML"},
                    "quantity": 6,
                    "unit_price": 5299,
                    "sale_fee": 1268.36,  # comisión POR UNIDAD
                }
            ],
        }
        base.update(extra)
        return base

    def _run(self, order, payments_result):
        dispatch = _dispatch(payments_result)
        dispatch.order = order
        with patch.object(ml, "get_valid_access_token", return_value="tok"), patch.object(
            ml, "_call_with_refresh", side_effect=dispatch
        ):
            ok, reason = ml.sync_order(self.connection, "ORD1", self.user)
        self.assertTrue(ok, f"sync_order falló: {reason}")
        return Sale.objects.get(reference="ML ORDER ORD1")

    def test_uses_real_payment_data_for_whole_order(self):
        """Si hay datos de pago, se usan tal cual (comisión e impuestos reales de la orden)."""
        payments = {
            "payments": [
                {"marketplace_fee": 7610.16, "taxes_amount": 699.47}
            ]
        }
        sale = self._run(self._order(), payments)
        self.assertEqual(sale.ml_commission_total, Decimal("7610.16"))
        self.assertEqual(sale.ml_tax_total, Decimal("699.47"))
        # Y la cantidad vendida se registra completa.
        item = SaleItem.objects.get(sale=sale)
        self.assertEqual(item.quantity, Decimal("6"))

    def test_sale_fee_fallback_multiplies_by_quantity(self):
        """Sin datos de pago, sale_fee (por unidad) se multiplica por la cantidad."""
        sale = self._run(self._order(), {"payments": []})
        # 1268.36 * 6 = 7610.16 (antes del fix quedaba 1268.36)
        self.assertEqual(sale.ml_commission_total, Decimal("7610.16"))
        # IIBB estimado 3.5% sobre la comisión total
        self.assertEqual(sale.ml_tax_total, Decimal("266.36"))

    def test_fee_details_take_priority(self):
        """fee_details de la orden es la fuente autoritativa y gana sobre todo lo demás."""
        order = self._order(
            fee_details=[
                {"type": "marketplace_fee", "amount": 7610.16},
                {"type": "iibb_tax", "amount": 699.47},
            ]
        )
        # Aunque get_order_payments devolviera otra cosa, no debería usarse.
        sale = self._run(order, {"payments": [{"marketplace_fee": 999, "taxes_amount": 9}]})
        self.assertEqual(sale.ml_commission_total, Decimal("7610.16"))
        self.assertEqual(sale.ml_tax_total, Decimal("699.47"))


class FindMLCommissionErrorsTests(TestCase):
    """Valida la lógica de detección del comando find_ml_commission_errors."""

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="d", password="x")
        self.ml_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.MERCADOLIBRE)
        self.comun = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.product = Product.objects.create(sku="D1", name="Prod")

    def _sale(self, *, warehouse, qty, commission, tax, order_id="1"):
        sale = Sale.objects.create(
            warehouse=warehouse,
            user=self.user,
            ml_order_id=order_id,
            reference=f"ML ORDER {order_id}",
            ml_commission_total=Decimal(commission),
            ml_tax_total=Decimal(tax),
        )
        SaleItem.objects.create(
            sale=sale, product=self.product, quantity=Decimal(qty),
            unit_price=Decimal("100"), final_unit_price=Decimal("100"),
            line_total=Decimal("100") * Decimal(qty),
        )
        return sale

    def _count(self):
        out = StringIO()
        call_command("find_ml_commission_errors", stdout=out)
        text = out.getvalue()
        for line in text.splitlines():
            if "AFECTADAS" in line:
                return int(line.split(":")[1].strip()), text
        return None, text

    def test_flags_multi_unit_with_estimated_tax(self):
        # 6 unidades, impuesto == 3,5% de la comisión -> AFECTADA
        self._sale(warehouse=self.ml_wh, qty="6", commission="1268.36", tax="44.39", order_id="A")
        n, _ = self._count()
        self.assertEqual(n, 1)

    def test_ignores_single_unit(self):
        # 1 unidad: sale_fee*1 es correcto aunque el impuesto sea 3,5% -> NO afectada
        self._sale(warehouse=self.ml_wh, qty="1", commission="1268.36", tax="44.39", order_id="B")
        n, _ = self._count()
        self.assertEqual(n, 0)

    def test_ignores_multi_unit_with_real_tax(self):
        # varias unidades pero impuesto real (no 3,5%) -> vino de datos reales, NO afectada
        self._sale(warehouse=self.ml_wh, qty="6", commission="7610.16", tax="699.47", order_id="C")
        n, _ = self._count()
        self.assertEqual(n, 0)

    def test_ignores_non_ml_sales(self):
        self._sale(warehouse=self.comun, qty="6", commission="1268.36", tax="44.39", order_id="D")
        n, _ = self._count()
        self.assertEqual(n, 0)
