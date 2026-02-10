from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase

from inventory import services
from inventory.models import Product, StockMovement, Warehouse


class InventoryServiceTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="tester", password="secret")
        self.product = Product.objects.create(sku="SKU1", name="Test Product", target_margin=Decimal("20.00"))
        self.comun = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.mercado_libre = Warehouse.objects.get(type=Warehouse.WarehouseType.MERCADOLIBRE)

    def test_entry_updates_stock_and_avg_cost(self):
        services.register_entry(self.product, self.comun, Decimal("10"), Decimal("5.00"), self.user, reference="PO1")
        self.product.refresh_from_db()
        stock_qty = self.product.stocks.get(warehouse=self.comun).quantity
        self.assertEqual(stock_qty, Decimal("10.00"))
        self.assertEqual(self.product.avg_cost, Decimal("5.00"))

        services.register_entry(self.product, self.comun, Decimal("10"), Decimal("7.00"), self.user)
        self.product.refresh_from_db()
        stock_qty = self.product.stocks.get(warehouse=self.comun).quantity
        self.assertEqual(stock_qty, Decimal("20.00"))
        self.assertEqual(self.product.avg_cost, Decimal("7.00"))

    def test_exit_blocks_negative_stock(self):
        with self.assertRaises(services.NegativeStockError):
            services.register_exit(self.product, self.comun, Decimal("1"), self.user)

        services.register_entry(self.product, self.comun, Decimal("5"), Decimal("2.00"), self.user)
        services.register_exit(self.product, self.comun, Decimal("3"), self.user)
        stock_qty = self.product.stocks.get(warehouse=self.comun).quantity
        self.assertEqual(stock_qty, Decimal("2.00"))

    def test_transfer_moves_between_warehouses(self):
        services.register_entry(self.product, self.comun, Decimal("8"), Decimal("4.00"), self.user)
        services.register_transfer(
            self.product,
            from_warehouse=self.comun,
            to_warehouse=self.mercado_libre,
            quantity=Decimal("5"),
            user=self.user,
            reference="T-1",
        )
        comun_qty = self.product.stocks.get(warehouse=self.comun).quantity
        self.assertEqual(comun_qty, Decimal("3.00"))
        self.assertFalse(self.product.stocks.filter(warehouse=self.mercado_libre).exists())
        movement = StockMovement.objects.filter(movement_type=StockMovement.MovementType.TRANSFER).last()
        self.assertEqual(movement.from_warehouse, self.comun)
        self.assertEqual(movement.to_warehouse, self.mercado_libre)

    def test_adjustment_can_decrease(self):
        services.register_entry(self.product, self.comun, Decimal("5"), Decimal("1.50"), self.user)
        services.register_adjustment(
            self.product, self.comun, quantity=Decimal("-2"), user=self.user, reference="COUNT-1"
        )
        stock_qty = self.product.stocks.get(warehouse=self.comun).quantity
        self.assertEqual(stock_qty, Decimal("3.00"))

    def test_suggested_price_uses_margin(self):
        services.register_entry(self.product, self.comun, Decimal("1"), Decimal("10.00"), self.user)
        self.product.refresh_from_db()
        self.assertEqual(self.product.suggested_price, Decimal("12.00"))
