from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory import services
from inventory.models import Customer, Product, Supplier, Warehouse


class DashboardViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="viewer", password="secret")
        self.product = Product.objects.create(sku="SKU-V", name="View Product", target_margin=Decimal("25.00"))
        self.comun = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.supplier = Supplier.objects.create(name="Proveedor Test", phone="123")
        self.client.force_login(self.user)

    def test_dashboard_totals_and_ranking(self):
        services.register_entry(self.product, self.comun, Decimal("4"), Decimal("10.00"), self.user)
        services.register_exit(self.product, self.comun, Decimal("3"), self.user)

        response = self.client.get(reverse("inventory_dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertIn("purchase_total", response.context)
        self.assertIn("sale_total", response.context)
        self.assertIn("ranking", response.context)

        ranking = response.context["ranking"]
        self.assertEqual(len(ranking), 1)
        self.assertEqual(ranking[0]["sku"], "SKU-V")
        self.assertEqual(ranking[0]["quantity"], Decimal("3.00"))

    def test_create_product_from_dashboard(self):
        response = self.client.post(
            reverse("inventory_create_product"),
            {
                "sku": "SKU-FORM",
                "name": "Form Product",
                "avg_cost": "10.00",
                "margin_consumer": "20.00",
                "margin_barber": "10.00",
                "margin_distributor": "5.00",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(Product.objects.filter(sku="SKU-FORM").exists())

    def test_register_purchase_and_sale_from_dashboard(self):
        product = Product.objects.create(sku="SKU-FLOW", name="Flow", target_margin=Decimal("10.00"))
        response = self.client.post(
            reverse("inventory_register_purchase"),
            {
                "warehouse": self.comun.id,
                "form-TOTAL_FORMS": "1",
                "form-INITIAL_FORMS": "0",
                "form-MIN_NUM_FORMS": "0",
                "form-MAX_NUM_FORMS": "1000",
                "form-0-product": product.id,
                "form-0-quantity": "2",
                "form-0-unit_cost": "5.00",
                "form-0-supplier": self.supplier.id,
            },
        )
        self.assertEqual(response.status_code, 302)

        response = self.client.post(
            reverse("inventory_register_sale"),
            {
                "warehouse": self.comun.id,
                "form-TOTAL_FORMS": "1",
                "form-INITIAL_FORMS": "0",
                "form-MIN_NUM_FORMS": "0",
                "form-MAX_NUM_FORMS": "1000",
                "form-0-product": product.id,
                "form-0-quantity": "1",
            },
        )
        self.assertEqual(response.status_code, 302)

        product.refresh_from_db()
        stock_qty = product.stocks.get(warehouse=self.comun).quantity
        self.assertEqual(stock_qty, Decimal("1.00"))

    def test_stock_list_per_warehouse(self):
        services.register_entry(self.product, self.comun, Decimal("3"), Decimal("2.00"), self.user)
        ml = Warehouse.objects.get(type=Warehouse.WarehouseType.MERCADOLIBRE)
        services.register_entry(self.product, ml, Decimal("5"), Decimal("2.50"), self.user)

        response = self.client.get(reverse("inventory_stock_list"))
        self.assertEqual(response.status_code, 200)
        products = list(response.context["products"])
        self.assertEqual(len(products), 1)
        p = products[0]
        self.assertEqual(p.comun_qty, Decimal("3.00"))
        self.assertEqual(p.total_qty, Decimal("3.00"))

    def test_product_price_list(self):
        self.product.avg_cost = Decimal("50.00")
        self.product.vat_percent = Decimal("21.00")
        self.product.margin_consumer = Decimal("20.00")
        self.product.margin_barber = Decimal("10.00")
        self.product.margin_distributor = Decimal("5.00")
        self.product.save()
        response = self.client.get(reverse("inventory_product_prices"))
        self.assertEqual(response.status_code, 200)
        products = list(response.context["products"])
        self.assertEqual(products[0].consumer_price, Decimal("72.60"))
        self.assertEqual(products[0].barber_price, Decimal("66.55"))
        self.assertEqual(products[0].distributor_price, Decimal("63.525"))

    def test_product_price_download(self):
        self.product.avg_cost = Decimal("100.00")
        self.product.margin_consumer = Decimal("20.00")
        self.product.save()
        response = self.client.get(reverse("inventory_product_prices_download", args=["consumer"]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response["Content-Type"], "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    def test_delete_product_from_price_list(self):
        product_to_delete = Product.objects.create(
            sku="SKU-DEL",
            name="Eliminar",
            avg_cost=Decimal("10.00"),
            margin_consumer=Decimal("10.00"),
            margin_barber=Decimal("10.00"),
            margin_distributor=Decimal("10.00"),
        )
        response = self.client.post(reverse("inventory_product_delete", args=[product_to_delete.pk]))
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Product.objects.filter(pk=product_to_delete.pk).exists())

    def test_customers_and_discounts(self):
        customer_data = {"name": "Cliente 1", "email": "c1@example.com", "audience": "CONSUMER"}
        response = self.client.post(reverse("inventory_customers"), {"action": "create_customer", **customer_data})
        self.assertEqual(response.status_code, 302)
        customer = Customer.objects.get(name="Cliente 1")

        product = Product.objects.create(
            sku="SKU-CL",
            name="Prod",
            avg_cost=Decimal("10.00"),
            margin_consumer=Decimal("10.00"),
            margin_barber=Decimal("10.00"),
            margin_distributor=Decimal("10.00"),
        )
        response = self.client.post(
            reverse("inventory_customers"),
            {
                "action": "create_discount",
                "customer": customer.id,
                "product": product.id,
                "discount_percent": "5.00",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(customer.discounts.filter(product=product).exists())
