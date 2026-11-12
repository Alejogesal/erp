"""Pantalla de Costos: las filas se dibujan a mano (sin widgets de Django) para
que cargue rápido con miles de productos. Estos tests cuidan que el formulario
siga mandando lo mismo y que la cantidad de consultas no dependa del catálogo."""
import re
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from inventory.models import Product, Supplier


class CostListPageTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="costos", password="secret")
        self.client.force_login(self.user)
        self.supplier = Supplier.objects.create(name="Proveedor Uno")
        self.other_supplier = Supplier.objects.create(name="Proveedor Dos")
        self.product = Product.objects.create(
            sku="SKU-C1",
            name='Shampoo "Pro" <1L>',
            group="Marca A",
            avg_cost=Decimal("100.00"),
            vat_percent=Decimal("21.00"),
            default_supplier=self.supplier,
        )

    def _get(self):
        response = self.client.get(reverse("inventory_product_costs"))
        self.assertEqual(response.status_code, 200)
        return response

    def _row_fields(self, html, prefix):
        """Devuelve {campo: valor} de los inputs/select de una fila, como los mandaría el navegador."""
        fields = {}
        for tag in re.findall(r"<input[^>]*name=\"%s-[^>]*>" % re.escape(prefix), html):
            name = re.search(r'name="([^"]+)"', tag).group(1)
            value = re.search(r'value="([^"]*)"', tag)
            fields[name] = value.group(1) if value else ""
        select = re.search(r"<select[^>]*name=\"%s-supplier\"[^>]*>(.*?)</select>" % re.escape(prefix), html, re.S)
        selected = re.search(r'<option value="([^"]*)" selected>', select.group(1)) if select else None
        fields[f"{prefix}-supplier"] = selected.group(1) if selected else ""
        return fields

    def test_row_renders_fields_with_current_values(self):
        html = self._get().content.decode()
        fields = self._row_fields(html, "form-0")
        self.assertEqual(fields["form-0-product_id"], str(self.product.id))
        self.assertEqual(fields["form-0-sku"], "SKU-C1")
        # El nombre va escapado en el HTML.
        self.assertEqual(fields["form-0-name"], "Shampoo &quot;Pro&quot; &lt;1L&gt;")
        self.assertEqual(fields["form-0-group"], "Marca A")
        self.assertEqual(fields["form-0-avg_cost"], "100.00")
        self.assertEqual(fields["form-0-vat_percent"], "21.00")
        self.assertEqual(fields["form-0-supplier"], str(self.supplier.id))
        self.assertIn(f'<option value="{self.supplier.id}" selected>Proveedor Uno</option>', html)

    def test_row_select_only_carries_current_supplier(self):
        """El resto de los proveedores viaja una sola vez (JSON), no en cada fila."""
        html = self._get().content.decode()
        select = re.search(r'<select[^>]*name="form-0-supplier"[^>]*>(.*?)</select>', html, re.S).group(1)
        self.assertNotIn("Proveedor Dos", select)
        self.assertIn("data-lazy-suppliers", html)
        options = self._get().context["supplier_options"]
        self.assertEqual({o["name"] for o in options}, {"Proveedor Uno", "Proveedor Dos"})

    def test_product_without_supplier_renders_blank_select(self):
        self.product.default_supplier = None
        self.product.save()
        html = self._get().content.decode()
        self.assertEqual(self._row_fields(html, "form-0")["form-0-supplier"], "")

    def test_query_count_does_not_grow_with_products(self):
        with CaptureQueriesContext(connection) as few:
            self._get()
        for i in range(15):
            Product.objects.create(
                sku=f"SKU-N{i}", name=f"Producto {i}", avg_cost=Decimal("10.00"),
                default_supplier=self.other_supplier if i % 2 else self.supplier,
            )
        with CaptureQueriesContext(connection) as many:
            self._get()
        self.assertEqual(len(many), len(few))

    def test_posting_back_the_rendered_row_keeps_data(self):
        html = self._get().content.decode()
        data = self._row_fields(html, "form-0")
        data = {k: v.replace("&quot;", '"').replace("&lt;", "<").replace("&gt;", ">") for k, v in data.items()}
        data.update({
            "action": "update_costs",
            "form-TOTAL_FORMS": "1",
            "form-INITIAL_FORMS": "1",
            "form-MIN_NUM_FORMS": "0",
            "form-MAX_NUM_FORMS": "1000",
            "form-0-group": "Marca B",
        })
        response = self.client.post(reverse("inventory_product_costs"), data)
        self.assertEqual(response.status_code, 302)
        self.product.refresh_from_db()
        self.assertEqual(self.product.name, 'Shampoo "Pro" <1L>')
        self.assertEqual(self.product.group, "Marca B")
        self.assertEqual(self.product.default_supplier, self.supplier)

    def test_changing_supplier_from_the_row_is_saved(self):
        response = self.client.post(
            reverse("inventory_product_costs"),
            {
                "action": "update_costs",
                "form-TOTAL_FORMS": "1",
                "form-INITIAL_FORMS": "1",
                "form-MIN_NUM_FORMS": "0",
                "form-MAX_NUM_FORMS": "1000",
                "form-0-product_id": str(self.product.id),
                "form-0-sku": "SKU-C1",
                "form-0-name": "Shampoo",
                "form-0-group": "Marca A",
                "form-0-supplier": str(self.other_supplier.id),
            },
        )
        self.assertEqual(response.status_code, 302)
        self.product.refresh_from_db()
        self.assertEqual(self.product.default_supplier, self.other_supplier)
