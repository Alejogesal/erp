"""Capa 2: pantalla "Marcas disponibles" para armar la lista propia elegiendo,
marca por marca, de qué proveedor sale — de forma explícita y opcional."""

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory.models import BrandSupplier, ExcludedBrand, Product, Supplier, SupplierProduct


class AvailableBrandsPageTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(self.user)
        self.aris = Supplier.objects.create(name="Aris Norma")
        self.glm = Supplier.objects.create(name="GLM Distribuidora")

    def _linked_product(self, name, group, supplier, cost, priced=True):
        p = Product.objects.create(name=name, group=group, margin_consumer=Decimal("0.00"))
        SupplierProduct.objects.create(
            supplier=supplier, product=p, last_cost=Decimal(cost) if priced else Decimal("0.00")
        )
        return p

    def test_lists_every_brand_with_its_suppliers_and_counts(self):
        self._linked_product("Fidelite Uno", "FIDELITE", self.aris, "100")
        self._linked_product("Fidelite Dos", "FIDELITE", self.aris, "0", priced=False)
        self._linked_product("Fidelite Tres", "FIDELITE", self.glm, "90")

        resp = self.client.get(reverse("inventory_available_brands"))
        self.assertEqual(resp.status_code, 200)
        blocks = {b["group"]: b for b in resp.context["brand_blocks"]}
        fidelite = blocks["FIDELITE"]
        self.assertEqual(fidelite["supplier_count"], 2)
        suppliers = {s["name"]: s for s in fidelite["suppliers"]}
        self.assertEqual(suppliers["Aris Norma"]["total"], 2)
        self.assertEqual(suppliers["Aris Norma"]["con_precio"], 1)
        self.assertEqual(suppliers["GLM Distribuidora"]["total"], 1)
        self.assertEqual(suppliers["GLM Distribuidora"]["con_precio"], 1)
        self.assertFalse(fidelite["is_selected"])

    def test_brand_not_shown_until_explicitly_added(self):
        self._linked_product("Fidelite Uno", "FIDELITE", self.aris, "100")

        # Todavía no está en la lista propia (nada se agrega solo).
        resp = self.client.get(reverse("inventory_product_prices"))
        self.assertEqual(len(resp.context["entries"]), 0)

        # La agrego eligiendo Aris desde Marcas disponibles.
        resp = self.client.post(
            reverse("inventory_available_brands"),
            {"action": "set_brand_supplier", "group": "FIDELITE", "supplier": self.aris.id},
        )
        self.assertRedirects(resp, reverse("inventory_available_brands"))
        self.assertTrue(BrandSupplier.objects.filter(group__iexact="FIDELITE", supplier=self.aris).exists())

        resp = self.client.get(reverse("inventory_product_prices"))
        entries = resp.context["entries"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["principal_supplier"], "Aris Norma")

    def test_remove_brand_supplier_takes_it_out_of_the_list(self):
        self._linked_product("Fidelite Uno", "FIDELITE", self.aris, "100")
        BrandSupplier.objects.create(group="FIDELITE", supplier=self.aris)

        resp = self.client.post(
            reverse("inventory_available_brands"),
            {"action": "remove_brand_supplier", "group": "FIDELITE"},
        )
        self.assertRedirects(resp, reverse("inventory_available_brands"))
        self.assertFalse(BrandSupplier.objects.filter(group__iexact="FIDELITE").exists())

        resp = self.client.get(reverse("inventory_product_prices"))
        self.assertEqual(len(resp.context["entries"]), 0)

    def test_set_brand_excluded_from_available_brands_page(self):
        self._linked_product("Fidelite Uno", "FIDELITE", self.aris, "100")
        BrandSupplier.objects.create(group="FIDELITE", supplier=self.aris)

        resp = self.client.post(
            reverse("inventory_available_brands"),
            {"action": "set_brand_excluded", "group": "FIDELITE", "excluded": "1"},
        )
        self.assertRedirects(resp, reverse("inventory_available_brands"))
        self.assertTrue(ExcludedBrand.objects.filter(group__iexact="FIDELITE").exists())

        resp = self.client.get(reverse("inventory_product_prices"))
        self.assertEqual(len(resp.context["entries"]), 0)

    def test_switching_brand_supplier_replaces_previous_choice(self):
        self._linked_product("Fidelite Uno", "FIDELITE", self.aris, "100")
        self._linked_product("Fidelite Uno GLM", "FIDELITE", self.glm, "90")
        BrandSupplier.objects.create(group="FIDELITE", supplier=self.aris)

        resp = self.client.post(
            reverse("inventory_available_brands"),
            {"action": "set_brand_supplier", "group": "FIDELITE", "supplier": self.glm.id},
        )
        self.assertRedirects(resp, reverse("inventory_available_brands"))
        bs = BrandSupplier.objects.get(group__iexact="FIDELITE")
        self.assertEqual(bs.supplier_id, self.glm.id)

    def test_reported_scenario_end_to_end(self):
        # El caso real reportado: Aris y GLM tienen cada uno su propio Fidelite
        # (sin fusionarse, gracias a la Capa 1). Sin elegir nada, la lista
        # propia está vacía. Al elegir Aris en Marcas disponibles, solo sus
        # productos entran; los de GLM quedan afuera, con su propio nombre y
        # costo intactos (no contaminan a Aris).
        aris_p = self._linked_product("Fidelite Coloracion 8 Rubio Oscuro", "FIDELITE", self.aris, "146900")
        glm_p = self._linked_product("Fidelite Coloracion 8 Rubio", "FIDELITE", self.glm, "140000")
        self.assertNotEqual(aris_p.id, glm_p.id)

        resp = self.client.get(reverse("inventory_product_prices"))
        self.assertEqual(len(resp.context["entries"]), 0)

        self.client.post(
            reverse("inventory_available_brands"),
            {"action": "set_brand_supplier", "group": "FIDELITE", "supplier": self.aris.id},
        )

        resp = self.client.get(reverse("inventory_product_prices"))
        entries = resp.context["entries"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["product"].id, aris_p.id)
        self.assertEqual(entries[0]["principal_supplier"], "Aris Norma")
