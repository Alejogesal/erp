from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory import services
from inventory.models import Customer, Product, ProductVariant, Purchase, Sale, SaleItem, Stock, Supplier, Warehouse


class StockConsistencyTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(self.user)
        self.comun = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.product = Product.objects.create(sku="P1", name="Con variedades", target_margin=Decimal("10"))
        self.var = ProductVariant.objects.create(product=self.product, name="Rojo", quantity=Decimal("5.00"))
        Stock.objects.update_or_create(product=self.product, warehouse=self.comun, defaults={"quantity": Decimal("5.00")})
        self.supplier = Supplier.objects.create(name="Prov", phone="1")

    def _qty(self):
        return Stock.objects.get(product=self.product, warehouse=self.comun).quantity

    def _buy(self, qty="3"):
        return self.client.post(
            reverse("inventory_register_purchase"),
            {
                "warehouse": self.comun.id,
                "form-TOTAL_FORMS": "1",
                "form-INITIAL_FORMS": "0",
                "form-MIN_NUM_FORMS": "0",
                "form-MAX_NUM_FORMS": "1000",
                "form-0-product": self.product.id,
                "form-0-variant_id": self.var.id,
                "form-0-quantity": qty,
                "form-0-unit_cost": "5.00",
                "form-0-supplier": self.supplier.id,
            },
        )

    def test_purchase_with_variant_adds_units_once(self):
        self.assertEqual(self._buy("3").status_code, 302)
        self.var.refresh_from_db()
        self.assertEqual(self.var.quantity, Decimal("8.00"))
        self.assertEqual(self._qty(), Decimal("8.00"))

    def test_purchase_delete_with_variant_restores_both(self):
        self._buy("3")
        purchase = Purchase.objects.order_by("-id").first()
        self.client.post(reverse("inventory_purchase_delete", args=[purchase.id]))
        self.var.refresh_from_db()
        self.assertEqual(self.var.quantity, Decimal("5.00"))
        self.assertEqual(self._qty(), Decimal("5.00"))

    def test_credit_note_restocks_units(self):
        customer = Customer.objects.create(name="Cli")
        services.register_exit(self.product, self.comun, Decimal("2"), self.user, allow_negative=True)
        sale = Sale.objects.create(warehouse=self.comun, customer=customer, total=Decimal("20"), user=self.user)
        item = SaleItem.objects.create(
            sale=sale, product=self.product, quantity=Decimal("2"), unit_price=Decimal("10"),
            final_unit_price=Decimal("10"), line_total=Decimal("20"),
        )
        from inventory.models import StockMovement
        StockMovement.objects.filter(product=self.product, movement_type="EXIT").update(sale=sale)
        before = self._qty()
        response = self.client.post(
            reverse("inventory_create_credit_note", args=[customer.id]),
            {"action": "create", "sale_id": sale.id, f"qty_{item.id}": "1", "restock": "1", "date": "2026-01-01"},
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self._qty(), before + Decimal("1.00"))
