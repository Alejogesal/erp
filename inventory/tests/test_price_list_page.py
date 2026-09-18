"""Lista de precios (pantalla): proveedor principal, costo neto/con IVA por
fila, y excluir/incluir una marca sin salir de la pantalla."""

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventory.models import BrandSupplier, ExcludedBrand, Product, Supplier, SupplierProduct


class PriceListPageTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(self.user)
        self.prov_a = Supplier.objects.create(name="Proveedor A")

    def _product(self, name, group, principal, cost, vat=Decimal("21.00")):
        p = Product.objects.create(
            name=name,
            group=group,
            avg_cost=Decimal(cost),
            vat_percent=vat,
            margin_consumer=Decimal("0.00"),
            default_supplier=principal,
        )
        SupplierProduct.objects.create(
            supplier=principal,
            product=p,
            last_cost=(Decimal(cost) * (Decimal("1.00") + vat / Decimal("100.00"))).quantize(Decimal("0.01")),
            vat_percent=vat,
        )
        return p

    def test_entry_shows_principal_supplier_and_cost_net_and_with_vat(self):
        self._product("Zeta Uno", "Zeta", self.prov_a, "100.00")

        resp = self.client.get(reverse("inventory_product_prices"))
        self.assertEqual(resp.status_code, 200)
        entries = resp.context["entries"]
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["principal_supplier"], "Proveedor A")
        self.assertEqual(entry["cost_net"], "100.00")
        self.assertEqual(entry["cost_with_vat"], "121.00")

    def test_set_brand_excluded_from_price_list_page(self):
        self._product("Zeta Uno", "Zeta", self.prov_a, "100.00")

        resp = self.client.post(
            reverse("inventory_product_prices"),
            {"action": "set_brand_excluded", "group": "Zeta", "excluded": "1"},
        )
        self.assertRedirects(resp, reverse("inventory_product_prices"))
        self.assertTrue(ExcludedBrand.objects.filter(group__iexact="Zeta").exists())

        # Ya no aparece en la vista normal (solo lo que se descarga).
        resp = self.client.get(reverse("inventory_product_prices"))
        self.assertEqual(len(resp.context["entries"]), 0)

        # Con ?todos=1 aparece marcada como fuera de lista.
        resp = self.client.get(reverse("inventory_product_prices"), {"todos": "1"})
        entries = resp.context["entries"]
        self.assertEqual(len(entries), 1)
        self.assertFalse(entries[0]["in_list"])
        self.assertTrue(entries[0]["excluded"])

    def test_unset_brand_excluded_restores_visibility(self):
        self._product("Zeta Uno", "Zeta", self.prov_a, "100.00")
        ExcludedBrand.objects.create(group="Zeta")

        resp = self.client.post(
            reverse("inventory_product_prices"),
            {"action": "set_brand_excluded", "group": "Zeta", "excluded": "0"},
        )
        self.assertRedirects(resp, reverse("inventory_product_prices"))
        self.assertFalse(ExcludedBrand.objects.filter(group__iexact="Zeta").exists())

        resp = self.client.get(reverse("inventory_product_prices"))
        entries = resp.context["entries"]
        self.assertEqual(len(entries), 1)
        self.assertTrue(entries[0]["in_list"])

    def test_set_brand_excluded_preserves_todos_query_param_on_redirect(self):
        self._product("Zeta Uno", "Zeta", self.prov_a, "100.00")

        resp = self.client.post(
            reverse("inventory_product_prices"),
            {"action": "set_brand_excluded", "group": "Zeta", "excluded": "1", "todos": "1"},
        )
        self.assertRedirects(resp, reverse("inventory_product_prices") + "?todos=1")

    def test_principal_supplier_reflects_pinned_brand_supplier(self):
        prov_b = Supplier.objects.create(name="Proveedor B")
        p = self._product("Zeta Uno", "Zeta", self.prov_a, "100.00")
        SupplierProduct.objects.create(supplier=prov_b, product=p, last_cost=Decimal("90.00"))
        BrandSupplier.objects.create(group="Zeta", supplier=prov_b)

        resp = self.client.get(reverse("inventory_product_prices"))
        entries = resp.context["entries"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["principal_supplier"], "Proveedor B")

    def test_kit_entry_has_no_net_cost_but_has_cost_with_vat(self):
        kit = Product.objects.create(name="Kit Combo", is_kit=True)
        component = self._product("Componente", "Zeta", self.prov_a, "50.00")
        from inventory.models import KitComponent

        KitComponent.objects.create(kit=kit, component=component, quantity=Decimal("2"))

        resp = self.client.get(reverse("inventory_product_prices"))
        entries = {e["product"].id: e for e in resp.context["entries"]}
        kit_entry = entries[kit.id]
        self.assertEqual(kit_entry["cost_net"], "")
        self.assertEqual(kit_entry["cost_with_vat"], "121.00")


class PriceListSupplierPickerTests(TestCase):
    """El desplegable de proveedor principal por marca, ahora también disponible
    directo en Lista de precios (antes solo estaba en Proveedores)."""

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="u5", password="x")
        self.client.force_login(self.user)
        self.aris = Supplier.objects.create(name="Aris Norma")
        self.glm = Supplier.objects.create(name="GLM Distribuidora")

    def test_entry_lists_available_suppliers_and_current_selection(self):
        p = Product.objects.create(name="Fidelite X", group="Fidelite", margin_consumer=Decimal("0.00"))
        SupplierProduct.objects.create(supplier=self.aris, product=p, last_cost=Decimal("100"))
        SupplierProduct.objects.create(supplier=self.glm, product=p, last_cost=Decimal("90"))

        resp = self.client.get(reverse("inventory_product_prices"), {"todos": "1"})
        entry = next(e for e in resp.context["entries"] if e["product"].id == p.id)
        option_names = {o["name"] for o in entry["brand_supplier_options"]}
        self.assertEqual(option_names, {"Aris Norma", "GLM Distribuidora"})
        # Sin BrandSupplier explícito, el deducido es el default_supplier ya
        # resuelto (el más barato tras sync, GLM en este caso al crearse el link).

    def test_set_brand_supplier_from_price_list_page_reassigns_brand(self):
        # Reproduce el caso reportado: FIDELITE tenía a Aris como principal pero
        # esos productos son en realidad de GLM. Se corrige eligiendo GLM desde
        # el desplegable de Lista de precios (sin ir a Proveedores).
        p1 = Product.objects.create(name="Fidelite Uno", group="Fidelite", margin_consumer=Decimal("0.00"))
        p2 = Product.objects.create(name="Fidelite Dos", group="Fidelite", margin_consumer=Decimal("0.00"))
        for p in (p1, p2):
            SupplierProduct.objects.create(supplier=self.aris, product=p, last_cost=Decimal("100"))
            SupplierProduct.objects.create(supplier=self.glm, product=p, last_cost=Decimal("90"))
        BrandSupplier.objects.create(group="Fidelite", supplier=self.aris)

        resp = self.client.post(
            reverse("inventory_product_prices"),
            {"action": "set_brand_supplier", "group": "Fidelite", "supplier": self.glm.id},
        )
        self.assertRedirects(resp, reverse("inventory_product_prices"))
        self.assertTrue(BrandSupplier.objects.filter(group__iexact="Fidelite", supplier=self.glm).exists())
        p1.refresh_from_db()
        self.assertEqual(p1.default_supplier_id, self.glm.id)

        resp = self.client.get(reverse("inventory_product_prices"))
        entries = {e["product"].id: e for e in resp.context["entries"]}
        self.assertEqual(entries[p1.id]["principal_supplier"], "GLM Distribuidora")
        self.assertEqual(entries[p2.id]["principal_supplier"], "GLM Distribuidora")

    def test_remove_brand_supplier_from_price_list_page_falls_back_to_cheapest(self):
        p = Product.objects.create(name="Fidelite X", group="Fidelite", margin_consumer=Decimal("0.00"))
        SupplierProduct.objects.create(supplier=self.aris, product=p, last_cost=Decimal("100"))
        SupplierProduct.objects.create(supplier=self.glm, product=p, last_cost=Decimal("90"))
        BrandSupplier.objects.create(group="Fidelite", supplier=self.aris)

        resp = self.client.post(
            reverse("inventory_product_prices"),
            {"action": "remove_brand_supplier", "group": "Fidelite"},
        )
        self.assertRedirects(resp, reverse("inventory_product_prices"))
        self.assertFalse(BrandSupplier.objects.filter(group__iexact="Fidelite").exists())
        p.refresh_from_db()
        # GLM es más barato (90 < 100): al quitar la elección explícita, vuelve
        # a resolverse automáticamente al más barato.
        self.assertEqual(p.default_supplier_id, self.glm.id)

    def test_page_renders_supplier_dropdown_for_each_brand_row(self):
        p = Product.objects.create(
            name="Fidelite X", group="Fidelite", margin_consumer=Decimal("0.00"), default_supplier=self.aris
        )
        SupplierProduct.objects.create(supplier=self.aris, product=p, last_cost=Decimal("100"))
        SupplierProduct.objects.create(supplier=self.glm, product=p, last_cost=Decimal("90"))

        resp = self.client.get(reverse("inventory_product_prices"))
        html = resp.content.decode()
        self.assertContains(resp, "Aris Norma")
        self.assertContains(resp, "GLM Distribuidora")
        self.assertIn('name="supplier"', html)
