"""Buscador de productos dentro del panel de cada proveedor."""

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory.models import Product, Supplier, SupplierProduct


class SupplierSearchBoxTests(TestCase):
    def test_search_targets_products_table_and_indexes_brand(self):
        user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(user)
        aris = Supplier.objects.create(name="Aris Norma")
        p = Product.objects.create(name="Banda termica", group="FIDELITE", sku="ACC-1")
        SupplierProduct.objects.create(supplier=aris, product=p, supplier_name="BANDA TÉRMICA ARIS", last_cost=Decimal("1"))
        html = self.client.get(reverse("inventory_suppliers")).content.decode()
        # El filtro apunta a la tabla de productos, no a la de marcas que está antes.
        self.assertIn("data-supplier-items", html)
        self.assertIn('querySelector("[data-supplier-items]")', html)
        # Se busca por nombre, texto del proveedor, SKU y marca.
        self.assertIn('data-search="Banda termica BANDA TÉRMICA ARIS ACC-1 FIDELITE"', html)
