from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory import services
from inventory.models import (
    Customer,
    CustomerProductPrice,
    Product,
    Purchase,
    Sale,
    SaleItem,
    Supplier,
    SupplierProduct,
    Warehouse,
)


class DashboardViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="viewer", password="secret")
        self.product = Product.objects.create(sku="SKU-V", name="View Product", target_margin=Decimal("25.00"))
        self.comun = Warehouse.objects.get(type=Warehouse.WarehouseType.COMUN)
        self.supplier = Supplier.objects.create(name="Proveedor Test", phone="123")
        self.client.force_login(self.user)

    def test_dashboard_totals_and_ranking(self):
        services.register_entry(self.product, self.comun, Decimal("4"), Decimal("10.00"), self.user)
        sale = Sale.objects.create(warehouse=self.comun, total=Decimal("60.00"))
        SaleItem.objects.create(
            sale=sale,
            product=self.product,
            quantity=Decimal("3.00"),
            unit_price=Decimal("20.00"),
            cost_unit=self.product.cost_with_vat(),
            discount_percent=Decimal("0.00"),
            final_unit_price=Decimal("20.00"),
            line_total=Decimal("60.00"),
            vat_percent=Decimal("0.00"),
        )

        response = self.client.get(reverse("inventory_dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertIn("purchase_total", response.context)
        self.assertIn("sale_total", response.context)
        self.assertIn("ranking", response.context)

        ranking = response.context["ranking"]
        self.assertEqual(len(ranking), 1)
        self.assertEqual(ranking[0]["sku"], "SKU-V")
        self.assertEqual(ranking[0]["quantity"], Decimal("3.00"))
        self.assertEqual(ranking[0]["profit"], Decimal("30.00"))

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

    def test_register_purchase_distributes_shipping_cost_per_unit(self):
        product = Product.objects.create(sku="SKU-SHIP", name="Flow Shipping", target_margin=Decimal("10.00"))
        response = self.client.post(
            reverse("inventory_register_purchase"),
            {
                "warehouse": self.comun.id,
                "costo_envio": "6.00",
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

        purchase = Purchase.objects.order_by("-id").first()
        self.assertIsNotNone(purchase)
        self.assertEqual(purchase.shipping_cost, Decimal("6.00"))
        self.assertEqual(purchase.total, Decimal("16.00"))

        product.refresh_from_db()
        self.assertEqual(product.avg_cost, Decimal("8.00"))

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

    def test_bulk_update_margins_by_group_updates_only_matching_products(self):
        target = Product.objects.create(
            sku="SKU-BRAND-A",
            name="Brand A Product",
            group="Bellissima",
            margin_consumer=Decimal("10.00"),
        )
        other = Product.objects.create(
            sku="SKU-BRAND-B",
            name="Brand B Product",
            group="Otra",
            margin_consumer=Decimal("10.00"),
        )

        response = self.client.post(
            reverse("inventory_product_costs"),
            {
                "action": "bulk_update_margins",
                "group": "Bellissima",
                "margin_consumer": "35.00",
            },
        )
        self.assertEqual(response.status_code, 302)

        target.refresh_from_db()
        other.refresh_from_db()
        self.assertEqual(target.margin_consumer, Decimal("35.00"))
        self.assertEqual(other.margin_consumer, Decimal("10.00"))

    def test_bulk_update_margins_without_group_updates_all_products(self):
        first = Product.objects.create(
            sku="SKU-ALL-1",
            name="All 1",
            margin_barber=Decimal("8.00"),
        )
        second = Product.objects.create(
            sku="SKU-ALL-2",
            name="All 2",
            margin_barber=Decimal("12.00"),
        )

        response = self.client.post(
            reverse("inventory_product_costs"),
            {
                "action": "bulk_update_margins",
                "margin_barber": "22.00",
            },
        )
        self.assertEqual(response.status_code, 302)

        first.refresh_from_db()
        second.refresh_from_db()
        self.assertEqual(first.margin_barber, Decimal("22.00"))
        self.assertEqual(second.margin_barber, Decimal("22.00"))

    def test_suppliers_link_supplier_group_creates_links_for_matching_brand(self):
        product_a = Product.objects.create(sku="SKU-GROUP-1", name="Producto A", group="Bellissima")
        product_b = Product.objects.create(sku="SKU-GROUP-2", name="Producto B", group="Bellissima")
        product_c = Product.objects.create(sku="SKU-GROUP-3", name="Producto C", group="Otra")

        response = self.client.post(
            reverse("inventory_suppliers"),
            {
                "action": "link_supplier_group",
                "supplier": str(self.supplier.id),
                "group": "Bellissima",
                "last_cost": "321.50",
            },
        )
        self.assertEqual(response.status_code, 302)

        links = SupplierProduct.objects.filter(supplier=self.supplier)
        self.assertEqual(links.count(), 2)
        self.assertTrue(links.filter(product=product_a, last_cost=Decimal("321.50")).exists())
        self.assertTrue(links.filter(product=product_b, last_cost=Decimal("321.50")).exists())
        self.assertFalse(links.filter(product=product_c).exists())

        product_a.refresh_from_db()
        product_b.refresh_from_db()
        product_c.refresh_from_db()
        self.assertEqual(product_a.default_supplier_id, self.supplier.id)
        self.assertEqual(product_b.default_supplier_id, self.supplier.id)
        self.assertIsNone(product_c.default_supplier_id)

    def test_suppliers_link_supplier_group_without_products_does_not_create_links(self):
        response = self.client.post(
            reverse("inventory_suppliers"),
            {
                "action": "link_supplier_group",
                "supplier": str(self.supplier.id),
                "group": "Marca inexistente",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertFalse(SupplierProduct.objects.filter(supplier=self.supplier).exists())

    def test_product_costs_update_saves_margins_per_product(self):
        product = Product.objects.create(
            sku="SKU-MARGINS",
            name="Producto margen",
            group="Linea",
            avg_cost=Decimal("100.00"),
            vat_percent=Decimal("21.00"),
            margin_consumer=Decimal("25.00"),
            margin_barber=Decimal("20.00"),
            margin_distributor=Decimal("15.00"),
            default_supplier=self.supplier,
        )

        response = self.client.post(
            reverse("inventory_product_costs"),
            {
                "action": "update_costs",
                "form-TOTAL_FORMS": "1",
                "form-INITIAL_FORMS": "1",
                "form-MIN_NUM_FORMS": "0",
                "form-MAX_NUM_FORMS": "1000",
                "form-0-product_id": str(product.id),
                "form-0-name": product.name,
                "form-0-group": product.group,
                "form-0-supplier": str(self.supplier.id),
                "form-0-avg_cost": "100.00",
                "form-0-vat_percent": "21.00",
                "form-0-margin_consumer": "32.50",
                "form-0-margin_barber": "24.00",
                "form-0-margin_distributor": "18.75",
            },
        )
        self.assertEqual(response.status_code, 302)

        product.refresh_from_db()
        self.assertEqual(product.margin_consumer, Decimal("32.50"))
        self.assertEqual(product.margin_barber, Decimal("24.00"))
        self.assertEqual(product.margin_distributor, Decimal("18.75"))

    def test_product_margins_screen_loads(self):
        Product.objects.create(
            sku="SKU-MARG-VIEW",
            name="Vista margenes",
            margin_consumer=Decimal("20.00"),
            margin_barber=Decimal("15.00"),
            margin_distributor=Decimal("10.00"),
        )
        response = self.client.get(reverse("inventory_product_margins"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Márgenes por producto")

    def test_product_margins_row_update(self):
        product = Product.objects.create(
            sku="SKU-MARG-EDIT",
            name="Editar margenes",
            margin_consumer=Decimal("20.00"),
            margin_barber=Decimal("15.00"),
            margin_distributor=Decimal("10.00"),
        )
        response = self.client.post(
            reverse("inventory_product_margins"),
            {
                "action": "update_margin_row",
                "product_id": str(product.id),
                "margin_consumer": "33.00",
                "margin_barber": "27.50",
                "margin_distributor": "18.25",
            },
        )
        self.assertEqual(response.status_code, 200)
        product.refresh_from_db()
        self.assertEqual(product.margin_consumer, Decimal("33.00"))
        self.assertEqual(product.margin_barber, Decimal("27.50"))
        self.assertEqual(product.margin_distributor, Decimal("18.25"))

    def test_create_customer_product_price(self):
        customer = Customer.objects.create(name="Cliente precio", audience=Customer.Audience.CONSUMER)
        product = Product.objects.create(sku="SKU-CP", name="Prod CP")
        response = self.client.post(
            reverse("inventory_customers"),
            {
                "action": "create_custom_price",
                "customer": str(customer.id),
                "product": str(product.id),
                "unit_price": "1234.50",
                "unit_cost": "777.25",
            },
        )
        self.assertEqual(response.status_code, 302)
        custom = CustomerProductPrice.objects.get(customer=customer, product=product)
        self.assertEqual(custom.unit_price, Decimal("1234.50"))
        self.assertEqual(custom.unit_cost, Decimal("777.25"))

    def test_register_sale_uses_customer_specific_product_price(self):
        product = Product.objects.create(
            sku="SKU-CUSTOM-PRICE",
            name="Prod precio especial",
            avg_cost=Decimal("10.00"),
            margin_consumer=Decimal("20.00"),
        )
        customer = Customer.objects.create(name="Cliente especial", audience=Customer.Audience.CONSUMER)
        CustomerProductPrice.objects.create(
            customer=customer,
            product=product,
            unit_price=Decimal("99.00"),
            unit_cost=Decimal("55.00"),
        )
        response = self.client.post(
            reverse("inventory_register_sale"),
            {
                "warehouse": self.comun.id,
                "cliente": customer.id,
                "audiencia": Customer.Audience.CONSUMER,
                "form-TOTAL_FORMS": "1",
                "form-INITIAL_FORMS": "0",
                "form-MIN_NUM_FORMS": "0",
                "form-MAX_NUM_FORMS": "1000",
                "form-0-product": product.id,
                "form-0-quantity": "2",
                "form-0-vat_percent": "0",
            },
        )
        self.assertEqual(response.status_code, 302)
        sale = Sale.objects.order_by("-id").first()
        self.assertIsNotNone(sale)
        item = sale.items.get(product=product)
        self.assertEqual(item.unit_price, Decimal("99.00"))
        self.assertEqual(item.final_unit_price, Decimal("99.00"))
        self.assertEqual(item.line_total, Decimal("198.00"))
        self.assertEqual(item.cost_unit, Decimal("55.00"))

    def test_register_sale_manual_price_and_cost_overrides_defaults(self):
        product = Product.objects.create(
            sku="SKU-MANUAL",
            name="Prod manual",
            avg_cost=Decimal("100.00"),
            vat_percent=Decimal("21.00"),
            margin_consumer=Decimal("20.00"),
        )
        response = self.client.post(
            reverse("inventory_register_sale"),
            {
                "warehouse": self.comun.id,
                "audiencia": Customer.Audience.CONSUMER,
                "form-TOTAL_FORMS": "1",
                "form-INITIAL_FORMS": "0",
                "form-MIN_NUM_FORMS": "0",
                "form-MAX_NUM_FORMS": "1000",
                "form-0-product": product.id,
                "form-0-quantity": "3",
                "form-0-unit_price_override": "250.00",
                "form-0-cost_unit_override": "180.00",
                "form-0-vat_percent": "0",
            },
        )
        self.assertEqual(response.status_code, 302)
        sale = Sale.objects.order_by("-id").first()
        self.assertIsNotNone(sale)
        item = sale.items.get(product=product)
        self.assertEqual(item.unit_price, Decimal("250.00"))
        self.assertEqual(item.final_unit_price, Decimal("250.00"))
        self.assertEqual(item.cost_unit, Decimal("180.00"))

    def test_register_sale_without_manual_cost_uses_product_cost_with_vat(self):
        product = Product.objects.create(
            sku="SKU-COST-VAT",
            name="Prod costo iva",
            avg_cost=Decimal("3297.00"),
            vat_percent=Decimal("21.00"),
            margin_consumer=Decimal("10.00"),
        )
        response = self.client.post(
            reverse("inventory_register_sale"),
            {
                "warehouse": self.comun.id,
                "audiencia": Customer.Audience.CONSUMER,
                "form-TOTAL_FORMS": "1",
                "form-INITIAL_FORMS": "0",
                "form-MIN_NUM_FORMS": "0",
                "form-MAX_NUM_FORMS": "1000",
                "form-0-product": product.id,
                "form-0-quantity": "1",
                "form-0-vat_percent": "0",
            },
        )
        self.assertEqual(response.status_code, 302)
        sale = Sale.objects.order_by("-id").first()
        self.assertIsNotNone(sale)
        item = sale.items.get(product=product)
        self.assertEqual(item.cost_unit, product.cost_with_vat())

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
