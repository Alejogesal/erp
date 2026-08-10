"""Proveedor principal por marca (BrandSupplier): la elección del usuario manda
sobre el 'más barato' automático, con fallback si no hay precio."""

from decimal import Decimal
from io import BytesIO

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from openpyxl import load_workbook

from inventory import services
from inventory.middleware import _current_user
from inventory.models import BrandSupplier, Product, Supplier, SupplierProduct


def _reset_current_user():
    # El middleware guarda el usuario en un threadlocal que persiste entre tests;
    # lo limpiamos para que los AuditLog de saves fuera de un request no apunten a
    # un usuario de otro test (que ya no existe en esta DB).
    _current_user.user = None


class BrandSupplierServiceTests(TestCase):
    def setUp(self):
        _reset_current_user()
        self.a = Supplier.objects.create(name="Barato A")
        self.b = Supplier.objects.create(name="Caro B")
        self.product = Product.objects.create(name="Prod", group="Marca1", margin_consumer=Decimal("0.00"))
        # A es más barato (50) que B (80). IVA 0 => last_cost == cost_net.
        SupplierProduct.objects.create(supplier=self.a, product=self.product, last_cost=Decimal("50"))
        SupplierProduct.objects.create(supplier=self.b, product=self.product, last_cost=Decimal("80"))

    def test_without_brand_pin_uses_cheapest(self):
        services.sync_principal_to_cheapest(self.product)
        self.product.refresh_from_db()
        self.assertEqual(self.product.default_supplier_id, self.a.id)
        self.assertEqual(self.product.avg_cost, Decimal("50.00"))

    def test_brand_pin_overrides_cheapest(self):
        BrandSupplier.objects.create(group="Marca1", supplier=self.b)
        services.sync_principal_to_cheapest(self.product)
        self.product.refresh_from_db()
        # Aunque A es más barato, manda el proveedor elegido para la marca (B).
        self.assertEqual(self.product.default_supplier_id, self.b.id)
        self.assertEqual(self.product.avg_cost, Decimal("80.00"))

    def test_brand_pin_case_insensitive(self):
        BrandSupplier.objects.create(group="MARCA1", supplier=self.b)
        services.sync_principal_to_cheapest(self.product)
        self.product.refresh_from_db()
        self.assertEqual(self.product.default_supplier_id, self.b.id)

    def test_falls_back_to_cheapest_when_chosen_not_linked(self):
        # Marca apunta a un proveedor al que el producto NO está vinculado.
        other = Supplier.objects.create(name="Sin vínculo C")
        BrandSupplier.objects.create(group="Marca1", supplier=other)
        services.sync_principal_to_cheapest(self.product)
        self.product.refresh_from_db()
        self.assertEqual(self.product.default_supplier_id, self.a.id)

    def test_falls_back_when_chosen_has_no_price(self):
        c = Supplier.objects.create(name="Precio 0 C")
        SupplierProduct.objects.create(supplier=c, product=self.product, last_cost=Decimal("0"))
        BrandSupplier.objects.create(group="Marca1", supplier=c)
        services.sync_principal_to_cheapest(self.product)
        self.product.refresh_from_db()
        self.assertEqual(self.product.default_supplier_id, self.a.id)


class BrandSupplierViewTests(TestCase):
    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(self.user)
        self.a = Supplier.objects.create(name="Barato A")
        self.b = Supplier.objects.create(name="Caro B")
        self.product = Product.objects.create(name="Prod", group="Marca1", margin_consumer=Decimal("0.00"))
        SupplierProduct.objects.create(supplier=self.a, product=self.product, last_cost=Decimal("50"))
        SupplierProduct.objects.create(supplier=self.b, product=self.product, last_cost=Decimal("80"))
        services.sync_principal_to_cheapest(self.product)  # arranca en A

    def test_set_brand_supplier_reassigns_products(self):
        resp = self.client.post(reverse("inventory_suppliers"), {
            "action": "set_brand_supplier", "group": "Marca1", "supplier": self.b.id,
        })
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(BrandSupplier.objects.filter(group="Marca1", supplier=self.b).exists())
        self.product.refresh_from_db()
        self.assertEqual(self.product.default_supplier_id, self.b.id)
        self.assertEqual(self.product.avg_cost, Decimal("80.00"))

    def test_remove_brand_supplier_reverts_to_cheapest(self):
        BrandSupplier.objects.create(group="Marca1", supplier=self.b)
        services.sync_principal_to_cheapest(self.product)
        resp = self.client.post(reverse("inventory_suppliers"), {
            "action": "remove_brand_supplier", "group": "Marca1",
        })
        self.assertEqual(resp.status_code, 302)
        self.assertFalse(BrandSupplier.objects.filter(group="Marca1").exists())
        self.product.refresh_from_db()
        self.assertEqual(self.product.default_supplier_id, self.a.id)


class BrandSupplierDownloadTests(TestCase):
    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(self.user)
        self.a = Supplier.objects.create(name="Prov A")
        self.b = Supplier.objects.create(name="Prov B")

    def test_download_uses_explicit_brand_supplier(self):
        # Dos productos de "Marca1": uno cuyo principal (por más barato) es A, otro B.
        p1 = Product.objects.create(name="Uno", group="Marca1", margin_consumer=Decimal("0.00"))
        SupplierProduct.objects.create(supplier=self.a, product=p1, last_cost=Decimal("10"))
        p2 = Product.objects.create(name="Dos", group="Marca1", margin_consumer=Decimal("0.00"))
        SupplierProduct.objects.create(supplier=self.b, product=p2, last_cost=Decimal("20"))
        services.sync_principal_to_cheapest(p1)
        services.sync_principal_to_cheapest(p2)
        # Sin BrandSupplier, el deducido (más frecuente) sería ambiguo (1 y 1).
        # Fijamos explícitamente B como principal de la marca.
        BrandSupplier.objects.create(group="Marca1", supplier=self.b)
        # Reasignar: ahora ambos deberían intentar B; p1 no está vinculado a B => cae a A.
        services.sync_principal_to_cheapest(p1)
        services.sync_principal_to_cheapest(p2)

        url = reverse("inventory_product_prices_download", args=["consumer"])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        ws = load_workbook(BytesIO(resp.content)).active
        nombres = [r[1] for r in ws.iter_rows(min_row=2, values_only=True)]
        # Solo el producto cuyo principal es B (el elegido para la marca) aparece.
        self.assertIn("Dos", nombres)
        self.assertNotIn("Uno", nombres)


class BulkBrandSupplierTests(TestCase):
    def setUp(self):
        _reset_current_user()
        self.user = get_user_model().objects.create_user(username="b", password="x")
        self.client.force_login(self.user)
        self.a = Supplier.objects.create(name="Prov A")
        self.b = Supplier.objects.create(name="Prov B")
        # Marca1: dos productos vinculados a A y B; A más barato.
        self.p1 = Product.objects.create(name="Uno", group="Marca1", margin_consumer=Decimal("0.00"))
        SupplierProduct.objects.create(supplier=self.a, product=self.p1, last_cost=Decimal("50"))
        SupplierProduct.objects.create(supplier=self.b, product=self.p1, last_cost=Decimal("80"))
        services.sync_principal_to_cheapest(self.p1)  # A
        # Marca2: un producto solo con A.
        self.p2 = Product.objects.create(name="Dos", group="Marca2", margin_consumer=Decimal("0.00"))
        SupplierProduct.objects.create(supplier=self.a, product=self.p2, last_cost=Decimal("30"))
        services.sync_principal_to_cheapest(self.p2)

    def test_bulk_assigns_and_reassigns(self):
        # Asigna Marca1 -> B (manda sobre A), Marca2 -> sin asignar (vacío).
        resp = self.client.post(reverse("inventory_brand_suppliers"), {
            "action": "bulk_set_brand_suppliers",
            "bg_group": ["Marca1", "Marca2"],
            "bg_supplier": [str(self.b.id), ""],
        })
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(BrandSupplier.objects.filter(group="Marca1", supplier=self.b).exists())
        self.assertFalse(BrandSupplier.objects.filter(group="Marca2").exists())
        self.p1.refresh_from_db()
        self.assertEqual(self.p1.default_supplier_id, self.b.id)  # reasignado a B
        self.assertEqual(self.p1.avg_cost, Decimal("80.00"))

    def test_bulk_removes_existing_when_emptied(self):
        BrandSupplier.objects.create(group="Marca1", supplier=self.b)
        services.sync_principal_to_cheapest(self.p1)  # B
        resp = self.client.post(reverse("inventory_brand_suppliers"), {
            "action": "bulk_set_brand_suppliers",
            "bg_group": ["Marca1"],
            "bg_supplier": [""],  # vaciar => quitar asignación
        })
        self.assertEqual(resp.status_code, 302)
        self.assertFalse(BrandSupplier.objects.filter(group="Marca1").exists())
        self.p1.refresh_from_db()
        self.assertEqual(self.p1.default_supplier_id, self.a.id)  # vuelve al más barato

    def test_brand_suppliers_page_loads(self):
        resp = self.client.get(reverse("inventory_brand_suppliers"))
        self.assertEqual(resp.status_code, 200)
        # La marca aparece en la tabla.
        self.assertContains(resp, "Marca1")
