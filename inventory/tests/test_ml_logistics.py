"""Ventas por Envíos Flex / Mercado Envíos.

Full lo despacha MercadoLibre desde su depósito, así que el stock del ERP no se
toca. Flex, Colecta, Places y Correo salen del depósito propio: se descuentan de
COMUN igual que una venta mostrador, y el COMUN resultante se empuja a las
publicaciones para que ML quede alineado.
"""

import os
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory import mercadolibre as ml
from inventory.middleware import _current_user
from inventory.models import (
    MercadoLibreConnection,
    MercadoLibreItem,
    Product,
    Sale,
    Stock,
    StockMovement,
    Warehouse,
)


def _reset_current_user():
    _current_user.user = None


class ShipmentParsingTests(TestCase):
    """Los helpers que leen el JSON de /shipments."""

    def test_logistic_type_from_new_format(self):
        shipment = {"logistic": {"mode": "me2", "type": "self_service"}}
        self.assertEqual(ml.extract_logistic_type(shipment), "self_service")

    def test_logistic_type_falls_back_to_flat_field(self):
        # Formato viejo, por si ML devuelve el JSON legacy.
        self.assertEqual(ml.extract_logistic_type({"logistic_type": "cross_docking"}), "cross_docking")

    def test_logistic_type_missing_is_empty(self):
        self.assertEqual(ml.extract_logistic_type({}), "")

    def test_seller_cost_picks_matching_sender(self):
        costs = {
            "gross_amount": Decimal("100"),
            "senders": [{"user_id": 111, "cost": 30.5}, {"user_id": 222, "cost": 80}],
        }
        self.assertEqual(ml.seller_shipping_cost(costs, 222), Decimal("80.00"))

    def test_seller_cost_uses_first_sender_when_id_unknown(self):
        costs = {"senders": [{"user_id": 111, "cost": 30.5}]}
        self.assertEqual(ml.seller_shipping_cost(costs, None), Decimal("30.50"))

    def test_seller_cost_without_senders_is_zero(self):
        # gross_amount es el bruto sin descuentos: no representa lo que paga el
        # vendedor, así que no se usa como respaldo.
        self.assertEqual(ml.seller_shipping_cost({"gross_amount": 500}, 1), Decimal("0.00"))

    def test_delivery_status_maps_shipment_status(self):
        self.assertEqual(ml.delivery_status_from_shipment({"status": "delivered"}), "DELIVERED")
        self.assertEqual(ml.delivery_status_from_shipment({"status": "ready_to_ship"}), "IN_TRANSIT")
        self.assertEqual(ml.delivery_status_from_shipment({"status": "handling"}), "NOT_DELIVERED")
        self.assertIsNone(ml.delivery_status_from_shipment({"status": "vaya_a_saber"}))

    def test_item_has_flex_reads_shipping_tag(self):
        self.assertTrue(ml.item_has_flex({"shipping": {"tags": ["self_service_in"]}}))
        self.assertFalse(ml.item_has_flex({"shipping": {"tags": ["self_service_out"]}}))
        self.assertFalse(ml.item_has_flex({}))


class SyncOrderLogisticsTests(TestCase):
    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="flex", password="x")
        self.connection = MercadoLibreConnection.objects.create(user=self.user, ml_user_id="777")
        self.ml_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.MERCADOLIBRE)
        self.comun_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.product = Product.objects.create(name="Shampoo", sku="SH1")
        MercadoLibreItem.objects.create(item_id="MLA1", product=self.product, logistic_type="self_service")
        Stock.objects.create(product=self.product, warehouse=self.comun_wh, quantity=Decimal("10"))

    def _order(self, **overrides):
        order = {
            "id": "ORD1",
            "status": "paid",
            "tags": [],
            "total_amount": 1000,
            "date_created": "2026-08-01T10:00:00.000-03:00",
            "shipping": {"id": "SHIP1"},
            "seller": {"id": 777},
            "order_items": [
                {"item": {"id": "MLA1"}, "quantity": 2, "unit_price": 500, "sale_fee": 50}
            ],
            "fee_details": [{"type": "sale_fee", "amount": 100}],
        }
        order.update(overrides)
        return order

    def _sync(self, order, shipment, costs=None):
        def dispatch(connection, func, *args, **kwargs):
            if func is ml.get_order:
                return order
            if func is ml.get_shipment:
                return shipment
            if func is ml.get_shipment_costs:
                return costs or {}
            raise AssertionError(f"llamada inesperada: {func}")

        with patch.object(ml, "get_valid_access_token", return_value="tok"), patch.object(
            ml, "_call_with_refresh", side_effect=dispatch
        ), patch.object(ml, "push_comun_stock_to_ml", return_value=0) as push:
            result = ml.sync_order(self.connection, "ORD1", self.user)
        return result, push

    def test_flex_sale_records_channel_and_discounts_comun(self):
        (ok, reason), push = self._sync(
            self._order(),
            {"status": "ready_to_ship", "logistic": {"mode": "me2", "type": "self_service"}},
            {"senders": [{"user_id": 777, "cost": 1234.5}]},
        )
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

        sale = Sale.objects.get(ml_order_id="ORD1")
        self.assertEqual(sale.ml_logistic_type, "self_service")
        self.assertEqual(sale.ml_shipment_id, "SHIP1")
        self.assertEqual(sale.shipping_cost, Decimal("1234.50"))
        # El estado sale del shipment, no de los tags de la orden.
        self.assertEqual(sale.delivery_status, Sale.DeliveryStatus.IN_TRANSIT)

        stock = Stock.objects.get(product=self.product, warehouse=self.comun_wh)
        self.assertEqual(stock.quantity, Decimal("8.00"))
        push.assert_called_once()

    def test_full_sale_does_not_touch_comun_stock(self):
        (ok, _reason), push = self._sync(
            self._order(),
            {"status": "handling", "logistic": {"mode": "me2", "type": "fulfillment"}},
        )
        self.assertTrue(ok)
        sale = Sale.objects.get(ml_order_id="ORD1")
        self.assertEqual(sale.ml_logistic_type, "fulfillment")
        # Full lo despacha ML: el stock propio queda intacto y no se empuja nada.
        self.assertEqual(
            Stock.objects.get(product=self.product, warehouse=self.comun_wh).quantity, Decimal("10.00")
        )
        push.assert_not_called()

    def test_resync_does_not_discount_twice(self):
        shipment = {"status": "shipped", "logistic": {"mode": "me2", "type": "self_service"}}
        self._sync(self._order(), shipment)
        self._sync(self._order(), shipment)
        self._sync(self._order(), shipment)
        # ML re-notifica la misma orden cada vez que cambia de estado.
        self.assertEqual(
            Stock.objects.get(product=self.product, warehouse=self.comun_wh).quantity, Decimal("8.00")
        )
        sale = Sale.objects.get(ml_order_id="ORD1")
        self.assertEqual(
            StockMovement.objects.filter(
                sale=sale, movement_type=StockMovement.MovementType.EXIT
            ).count(),
            1,
        )

    def test_cancelled_flex_sale_returns_stock(self):
        shipment = {"status": "ready_to_ship", "logistic": {"mode": "me2", "type": "self_service"}}
        self._sync(self._order(), shipment)
        self.assertEqual(
            Stock.objects.get(product=self.product, warehouse=self.comun_wh).quantity, Decimal("8.00")
        )

        (ok, reason), _push = self._sync(self._order(status="cancelled"), shipment)
        self.assertFalse(ok)
        self.assertEqual(reason, "cancelled_marked")
        # La mercadería nunca salió: vuelve al depósito.
        self.assertEqual(
            Stock.objects.get(product=self.product, warehouse=self.comun_wh).quantity, Decimal("10.00")
        )

    def test_falls_back_to_publication_type_without_shipment(self):
        # ML puede tardar en crear el envío; la orden llega con shipping.id nulo.
        (ok, _reason), _push = self._sync(self._order(shipping={"id": None}), {})
        self.assertTrue(ok)
        sale = Sale.objects.get(ml_order_id="ORD1")
        self.assertEqual(sale.ml_logistic_type, "self_service")
        self.assertEqual(
            Stock.objects.get(product=self.product, warehouse=self.comun_wh).quantity, Decimal("8.00")
        )


class PushComunStockTests(TestCase):
    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="push", password="x")
        MercadoLibreConnection.objects.create(user=self.user, access_token="tok", ml_user_id="777")
        self.comun_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.product = Product.objects.create(name="Crema", sku="CR1")
        Stock.objects.create(product=self.product, warehouse=self.comun_wh, quantity=Decimal("7"))

    def _push(self):
        with patch.object(ml, "get_valid_access_token", return_value="tok"), patch.object(
            ml, "push_item_stock_and_price"
        ) as push_item, patch.object(
            ml, "push_selling_address_stock", return_value=True
        ) as push_flex:
            ml.push_comun_stock_to_ml([self.product])
        return push_item, push_flex

    def test_flex_publication_gets_item_quantity(self):
        MercadoLibreItem.objects.create(item_id="MLA1", product=self.product, logistic_type="self_service")
        push_item, push_flex = self._push()
        push_item.assert_called_once_with("MLA1", 7, None, "tok")
        push_flex.assert_not_called()

    def test_pure_full_publication_is_skipped(self):
        MercadoLibreItem.objects.create(item_id="MLA2", product=self.product, logistic_type="fulfillment")
        push_item, push_flex = self._push()
        # El stock de Full lo administra ML: pisarlo desde el ERP lo rompería.
        push_item.assert_not_called()
        push_flex.assert_not_called()

    def test_coexistence_publication_uses_selling_address(self):
        MercadoLibreItem.objects.create(
            item_id="MLA3",
            product=self.product,
            logistic_type="fulfillment",
            has_flex=True,
            user_product_id="MLAU1",
        )
        push_item, push_flex = self._push()
        # Un PUT /items pisaría el stock de Full junto con el propio.
        push_item.assert_not_called()
        push_flex.assert_called_once_with("MLAU1", 7, "tok")

    def test_coexistence_user_product_pushed_once(self):
        # Catálogo + tradicional comparten el user_product_id y el mismo stock.
        for item_id in ("MLA4", "MLA5"):
            MercadoLibreItem.objects.create(
                item_id=item_id,
                product=self.product,
                logistic_type="fulfillment",
                has_flex=True,
                user_product_id="MLAU2",
            )
        _push_item, push_flex = self._push()
        self.assertEqual(push_flex.call_count, 1)


class DeleteFlexSaleRestoresStockTests(TestCase):
    """Borrar una venta Flex tiene que reponer el stock que descontó de COMUN.

    Antes las ventas de ML nunca tenían movimientos (Full lo maneja ML), así que
    el borrado los ignoraba a propósito. Con Flex sí los hay.
    """

    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="del", password="x")
        self.client.force_login(self.user)
        self.ml_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.MERCADOLIBRE)
        self.comun_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.product = Product.objects.create(name="Gel", sku="GL1")
        Stock.objects.create(product=self.product, warehouse=self.comun_wh, quantity=Decimal("5"))
        self.sale = Sale.objects.create(
            warehouse=self.ml_wh,
            user=self.user,
            total=Decimal("300"),
            ml_order_id="ORD-DEL",
            reference="ML ORDER ORD-DEL",
            ml_logistic_type="self_service",
        )
        StockMovement.objects.create(
            product=self.product,
            sale=self.sale,
            movement_type=StockMovement.MovementType.EXIT,
            from_warehouse=self.comun_wh,
            quantity=Decimal("3"),
            user=self.user,
        )
        Stock.objects.filter(product=self.product, warehouse=self.comun_wh).update(
            quantity=Decimal("2")
        )

    def test_delete_restores_comun_stock(self):
        resp = self.client.post(reverse("inventory_sale_delete", args=[self.sale.id]))
        self.assertEqual(resp.status_code, 302)
        self.assertFalse(Sale.objects.filter(pk=self.sale.pk).exists())
        self.assertEqual(
            Stock.objects.get(product=self.product, warehouse=self.comun_wh).quantity,
            Decimal("5.00"),
        )

    def test_bulk_delete_restores_comun_stock(self):
        resp = self.client.post(
            reverse("inventory_sales_list"),
            {"action": "bulk_delete_selected", "sale_ids": [str(self.sale.id)]},
        )
        self.assertEqual(resp.status_code, 302)
        self.assertFalse(Sale.objects.filter(pk=self.sale.pk).exists())
        self.assertEqual(
            Stock.objects.get(product=self.product, warehouse=self.comun_wh).quantity,
            Decimal("5.00"),
        )

    def test_full_sale_mirror_stock_is_not_restored(self):
        # Una salida del depósito ML es espejo del stock en Full: reponerla acá
        # lo duplicaría contra lo que devuelve la API.
        full_sale = Sale.objects.create(
            warehouse=self.ml_wh, user=self.user, total=Decimal("100"),
            ml_order_id="ORD-FULL", ml_logistic_type="fulfillment",
        )
        Stock.objects.create(product=self.product, warehouse=self.ml_wh, quantity=Decimal("4"))
        StockMovement.objects.create(
            product=self.product, sale=full_sale,
            movement_type=StockMovement.MovementType.EXIT,
            from_warehouse=self.ml_wh, quantity=Decimal("1"), user=self.user,
        )
        self.client.post(reverse("inventory_sale_delete", args=[full_sale.id]))
        self.assertEqual(
            Stock.objects.get(product=self.product, warehouse=self.ml_wh).quantity, Decimal("4.00")
        )


class StockReconciliationTests(TestCase):
    """El sync periódico empuja COMUN a ML aunque no haya habido ninguna venta.

    El stock propio también cambia con compras, ajustes y transferencias, que no
    pasan por el push inmediato de la venta; y un push puede fallar. Sin esta
    reconciliación la publicación queda desfasada hasta la próxima venta.
    """

    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="rec", password="x")
        self.connection = MercadoLibreConnection.objects.create(
            user=self.user, access_token="tok", ml_user_id="777"
        )
        self.comun_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.product = Product.objects.create(name="Cera", sku="CE1")
        Stock.objects.create(product=self.product, warehouse=self.comun_wh, quantity=Decimal("12"))

    def _run_sync(self, item, reconcile="1"):
        MercadoLibreItem.objects.update_or_create(
            item_id=item["id"],
            defaults={
                "product": self.product,
                "logistic_type": ml.item_logistic_type(item),
            },
        )

        def dispatch(connection, func, *args, **kwargs):
            if func is ml.get_item_ids:
                return ([item["id"]], False)
            if func is ml.get_item:
                return item
            if func is ml.get_orders_summary:
                return {"item_sales": {}}
            if func is ml.get_user_product_stock:
                return self.user_product_stock
            raise AssertionError(f"llamada inesperada: {func}")

        with patch.dict(os.environ, {"ML_STOCK_RECONCILE": reconcile}), patch.object(
            ml, "get_valid_access_token", return_value="tok"
        ), patch.object(ml, "_call_with_refresh", side_effect=dispatch), patch.object(
            ml, "push_item_stock_and_price"
        ) as push_item, patch.object(
            ml, "push_selling_address_stock", return_value=True
        ) as push_flex:
            result = ml.sync_items_and_stock(self.connection, self.user, ignore_env_limit=True)
        return result, push_item, push_flex

    def test_reconciliation_is_off_unless_enabled(self):
        item = {
            "id": "MLA1",
            "title": "Cera",
            "status": "active",
            "available_quantity": 3,
            "shipping": {"logistic_type": "self_service"},
        }
        _result, push_item, _push_flex = self._run_sync(item, reconcile="0")
        # Sin el interruptor no se toca ninguna publicación, aunque difiera.
        push_item.assert_not_called()

    def test_flex_publication_is_corrected_to_comun(self):
        # ML publica 3, el depósito tiene 12: sin venta de por medio (una compra).
        item = {
            "id": "MLA1",
            "title": "Cera",
            "status": "active",
            "available_quantity": 3,
            "shipping": {"logistic_type": "self_service"},
        }
        result, push_item, _push_flex = self._run_sync(item)
        push_item.assert_called_once_with("MLA1", 12, None, "tok")
        self.assertEqual(result.metrics["stock_pushed"], 1)

    def test_no_push_when_already_in_sync(self):
        item = {
            "id": "MLA1",
            "title": "Cera",
            "status": "active",
            "available_quantity": 12,
            "shipping": {"logistic_type": "self_service"},
        }
        _result, push_item, _push_flex = self._run_sync(item)
        push_item.assert_not_called()

    def test_closed_publication_is_left_alone(self):
        item = {
            "id": "MLA1",
            "title": "Cera",
            "status": "closed",
            "available_quantity": 0,
            "shipping": {"logistic_type": "self_service"},
        }
        _result, push_item, _push_flex = self._run_sync(item)
        push_item.assert_not_called()

    def test_pure_full_publication_is_never_pushed(self):
        item = {
            "id": "MLA1",
            "title": "Cera",
            "status": "active",
            "available_quantity": 3,
            "user_product_id": "MLAU1",
            "shipping": {"logistic_type": "fulfillment"},
        }
        self.user_product_stock = {"locations": [{"type": "meli_facility", "quantity": 3}]}
        _result, push_item, push_flex = self._run_sync(item)
        # El stock de Full lo administra ML.
        push_item.assert_not_called()
        push_flex.assert_not_called()

    def test_coexistence_corrects_only_selling_address(self):
        item = {
            "id": "MLA1",
            "title": "Cera",
            "status": "active",
            "available_quantity": 9,
            "user_product_id": "MLAU1",
            "shipping": {"logistic_type": "fulfillment", "tags": ["self_service_in"]},
        }
        self.user_product_stock = {
            "locations": [
                {"type": "meli_facility", "quantity": 6},
                {"type": "selling_address", "quantity": 2},
            ]
        }
        _result, push_item, push_flex = self._run_sync(item)
        # Se corrige la ubicación propia (2 -> 12) sin tocar las 6 de Full.
        push_flex.assert_called_once_with("MLAU1", 12, "tok")
        push_item.assert_not_called()

    def test_coexistence_in_sync_is_left_alone(self):
        item = {
            "id": "MLA1",
            "title": "Cera",
            "status": "active",
            "available_quantity": 18,
            "user_product_id": "MLAU1",
            "shipping": {"logistic_type": "fulfillment", "tags": ["self_service_in"]},
        }
        self.user_product_stock = {
            "locations": [
                {"type": "meli_facility", "quantity": 6},
                {"type": "selling_address", "quantity": 12},
            ]
        }
        _result, _push_item, push_flex = self._run_sync(item)
        push_flex.assert_not_called()


class SalesChannelFilterTests(TestCase):
    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="filtro", password="x")
        self.client.force_login(self.user)
        self.ml_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.MERCADOLIBRE)
        self.comun_wh = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.flex = self._ml_sale("self_service", "O-FLEX")
        self.full = self._ml_sale("fulfillment", "O-FULL")
        self.legacy = self._ml_sale("", "O-VIEJA")
        self.comun = Sale.objects.create(
            warehouse=self.comun_wh, user=self.user, total=Decimal("50")
        )

    def _ml_sale(self, logistic_type, order_id):
        return Sale.objects.create(
            warehouse=self.ml_wh,
            user=self.user,
            total=Decimal("100"),
            ml_order_id=order_id,
            ml_logistic_type=logistic_type,
        )

    def _ids(self, response):
        return {sale.id for sale in response.context["sales"]}

    def test_no_filter_shows_every_channel(self):
        resp = self.client.get(reverse("inventory_sales_list"), {"show_history": "1"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            self._ids(resp), {self.flex.id, self.full.id, self.legacy.id, self.comun.id}
        )

    def test_filter_by_flex(self):
        resp = self.client.get(
            reverse("inventory_sales_list"), {"show_history": "1", "wh_ml": "1", "ml_type": "self_service"}
        )
        self.assertEqual(self._ids(resp), {self.flex.id})

    def test_filter_accepts_several_channels(self):
        resp = self.client.get(
            reverse("inventory_sales_list"),
            {"show_history": "1", "wh_ml": "1", "ml_type": ["self_service", "fulfillment"]},
        )
        self.assertEqual(self._ids(resp), {self.flex.id, self.full.id})

    def test_unknown_channel_matches_legacy_sales(self):
        resp = self.client.get(
            reverse("inventory_sales_list"), {"show_history": "1", "wh_ml": "1", "ml_type": "unknown"}
        )
        self.assertEqual(self._ids(resp), {self.legacy.id})

    def test_channel_filter_leaves_comun_sales_alone(self):
        # El canal es una propiedad de ML; el depósito común entra o no según su
        # propio checkbox, no según el tipo logístico.
        resp = self.client.get(
            reverse("inventory_sales_list"),
            {"show_history": "1", "wh_comun": "1", "wh_ml": "1", "ml_type": "self_service"},
        )
        self.assertEqual(self._ids(resp), {self.flex.id, self.comun.id})

    def test_bogus_channel_value_is_ignored(self):
        resp = self.client.get(
            reverse("inventory_sales_list"), {"show_history": "1", "ml_type": "no_existe"}
        )
        self.assertEqual(
            self._ids(resp), {self.flex.id, self.full.id, self.legacy.id, self.comun.id}
        )
