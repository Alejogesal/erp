"""Acciones masivas sobre la lista de productos de un proveedor."""

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory.models import Product, Supplier, SupplierProduct


class SupplierBulkActionsTests(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(user)
        self.a = Supplier.objects.create(name="Proveedor A")
        self.b = Supplier.objects.create(name="Proveedor B")
        self.p1 = Product.objects.create(name="Uno", sku="U-1", default_supplier=self.a)
        self.p2 = Product.objects.create(name="Dos", sku="D-1", default_supplier=self.a)
        self.p3 = Product.objects.create(name="Tres", sku="T-1", default_supplier=self.a)
        self.l1 = SupplierProduct.objects.create(supplier=self.a, product=self.p1, last_cost=Decimal("100"))
        self.l2 = SupplierProduct.objects.create(supplier=self.a, product=self.p2, last_cost=Decimal("100"))
        self.l3 = SupplierProduct.objects.create(supplier=self.a, product=self.p3, last_cost=Decimal("100"))
        # p1 también lo vende B: al quitarlo de A, B pasa a ser el principal.
        self.lb = SupplierProduct.objects.create(supplier=self.b, product=self.p1, last_cost=Decimal("120"))

    def _post(self, supplier, mode, links):
        return self.client.post(reverse("inventory_suppliers"), {
            "action": "bulk_supplier_links",
            "supplier_id": supplier.id,
            "mode": mode,
            "link_ids": [l.id for l in links],
        })

    def test_unlink_removes_only_selected_links_and_keeps_products(self):
        self._post(self.a, "unlink", [self.l1, self.l2])
        self.assertEqual(list(SupplierProduct.objects.filter(supplier=self.a)), [self.l3])
        self.assertTrue(SupplierProduct.objects.filter(pk=self.lb.pk).exists())
        self.assertEqual(Product.objects.count(), 3)
        self.p1.refresh_from_db()
        self.p2.refresh_from_db()
        self.assertEqual(self.p1.default_supplier_id, self.b.id)
        self.assertIsNone(self.p2.default_supplier_id)

    def test_delete_removes_selected_products(self):
        self._post(self.a, "delete", [self.l2, self.l3])
        self.assertEqual(set(Product.objects.values_list("id", flat=True)), {self.p1.id})

    def test_ignores_links_from_other_supplier(self):
        # El vínculo de B no se toca aunque se mande con el id de A.
        self._post(self.a, "unlink", [self.lb])
        self.assertTrue(SupplierProduct.objects.filter(pk=self.lb.pk).exists())
        self._post(self.a, "delete", [self.lb])
        self.assertTrue(Product.objects.filter(pk=self.p1.pk).exists())

    def test_page_renders_bulk_controls(self):
        html = self.client.get(reverse("inventory_suppliers")).content.decode()
        self.assertIn(f'id="bulk-form-{self.a.id}"', html)
        self.assertIn(f'name="link_ids" value="{self.l1.id}" form="bulk-form-{self.a.id}"', html)
