"""Buscador de productos dentro del panel de cada proveedor.

Las filas se piden al servidor al abrir el panel (no vienen en la página), y la
búsqueda es del lado del servidor: por nombre, texto del proveedor, SKU y
marca, sin distinguir tildes ni mayúsculas."""

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory.models import Product, Supplier, SupplierProduct
from inventory.views.suppliers import SUPPLIER_ROWS_LIMIT


class SupplierSearchBoxTests(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(user)
        self.aris = Supplier.objects.create(name="Aris Norma")
        p = Product.objects.create(name="Banda termica", group="FIDELITE", sku="ACC-1")
        SupplierProduct.objects.create(supplier=self.aris, product=p, supplier_name="BANDA TÉRMICA ARIS", last_cost=Decimal("1"))
        otro = Product.objects.create(name="Shampoo", group="Alfaparf", sku="SH-1")
        SupplierProduct.objects.create(supplier=self.aris, product=otro, last_cost=Decimal("1"))

    def _rows(self, q=""):
        url = reverse("inventory_supplier_products_rows", args=[self.aris.id])
        resp = self.client.get(url, {"q": q})
        self.assertEqual(resp.status_code, 200)
        return resp.json()

    def test_page_does_not_embed_rows_but_loads_them_on_open(self):
        html = self.client.get(reverse("inventory_suppliers")).content.decode()
        self.assertIn("data-supplier-items", html)
        self.assertIn('querySelector("[data-supplier-items]")', html)
        self.assertIn(reverse("inventory_supplier_products_rows", args=[self.aris.id]), html)
        self.assertIn("2 productos", html)
        # Las filas (con su checkbox de selección masiva) no vienen en la página.
        self.assertNotIn('name="link_ids"', html)

    def test_rows_endpoint_returns_all_without_query(self):
        data = self._rows()
        self.assertEqual((data["total"], data["shown"]), (2, 2))
        self.assertIn("Banda termica", data["html"])
        self.assertIn("Shampoo", data["html"])

    def test_search_matches_name_supplier_text_sku_and_brand_ignoring_accents(self):
        for q in ("termica", "TÉRMICA aris", "acc-1", "fidelite banda"):
            data = self._rows(q)
            self.assertEqual(data["shown"], 1, q)
            self.assertIn("Banda termica", data["html"])
            self.assertNotIn("Shampoo", data["html"])
        self.assertEqual(self._rows("inexistente")["shown"], 0)

    def test_without_query_rows_are_capped(self):
        for i in range(SUPPLIER_ROWS_LIMIT + 5):
            p = Product.objects.create(name=f"Relleno {i}", sku=f"R-{i}")
            SupplierProduct.objects.create(supplier=self.aris, product=p)
        data = self._rows()
        self.assertEqual(data["shown"], SUPPLIER_ROWS_LIMIT)
        self.assertEqual(data["total"], SUPPLIER_ROWS_LIMIT + 7)
        self.assertIn("Mostrando", data["html"])
        # Buscando sí aparece lo que quedó fuera del tope.
        self.assertEqual(self._rows("relleno 304")["shown"], 1)
