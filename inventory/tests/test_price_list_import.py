"""Importar la lista de precios de un proveedor (XLSX): cada proveedor se
respeta tal cual, sin fusionar por parecido de nombre con lo que ya cargó
OTRO proveedor."""

from decimal import Decimal
from io import BytesIO

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse

from openpyxl import Workbook

from inventory.models import Product, Supplier, SupplierProduct


def _xlsx_upload(rows, filename="lista.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.append(["Grupo", "Nombre", "Precio", "IVA"])
    for row in rows:
        ws.append(row)
    buf = BytesIO()
    wb.save(buf)
    return SimpleUploadedFile(
        filename,
        buf.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


class PriceListImportNoFuzzyMatchTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(self.user)
        self.aris = Supplier.objects.create(name="Aris Norma")
        self.glm = Supplier.objects.create(name="GLM Distribuidora")

    def _import(self, supplier, rows, vat="0"):
        return self.client.post(
            reverse("inventory_suppliers"),
            {
                "action": "import_price_list",
                "price_supplier_id": supplier.id,
                "price_file": _xlsx_upload(rows),
                "price_vat": vat,
            },
        )

    def test_similar_but_different_names_across_suppliers_stay_separate(self):
        # Aris ya tiene cargado "Fidelite Coloracion 8 Rubio Oscuro".
        self._import(self.aris, [["FIDELITE", "Fidelite Coloracion 8 Rubio Oscuro", 100]])
        self.assertEqual(Product.objects.count(), 1)

        # GLM importa "Fidelite Coloracion 8 Rubio" (mismo número "8", y sus
        # palabras son subconjunto de las del producto de Aris): antes, el
        # matcheo difuso (subconjunto de palabras con mismos números) los
        # fusionaba en el mismo Product aunque son tonos distintos. Ahora
        # tienen que quedar separados, cada uno solo vinculado a su proveedor.
        self._import(self.glm, [["FIDELITE", "Fidelite Coloracion 8 Rubio", 90]])

        self.assertEqual(Product.objects.count(), 2)
        aris_product = Product.objects.get(name="Fidelite Coloracion 8 Rubio Oscuro")
        glm_product = Product.objects.get(name="Fidelite Coloracion 8 Rubio")
        self.assertTrue(SupplierProduct.objects.filter(supplier=self.aris, product=aris_product).exists())
        self.assertFalse(SupplierProduct.objects.filter(supplier=self.glm, product=aris_product).exists())
        self.assertTrue(SupplierProduct.objects.filter(supplier=self.glm, product=glm_product).exists())
        self.assertFalse(SupplierProduct.objects.filter(supplier=self.aris, product=glm_product).exists())

    def test_same_name_across_different_suppliers_never_merges(self):
        # Capa 1: la lista de cada proveedor es su propio mundo. Ni siquiera un
        # nombre EXACTAMENTE igual entre dos proveedores distintos los fusiona
        # en el mismo Product — decidir si "son el mismo producto" es manual
        # (Marcas disponibles), no algo que el import resuelva solo.
        self._import(self.aris, [["FIDELITE", "Fidelite Coloracion 8", 100]])
        self._import(self.glm, [["FIDELITE", "  fidelite   coloracion 8  ", 90]])

        self.assertEqual(Product.objects.count(), 2)
        aris_product = Product.objects.get(supplier_products__supplier=self.aris)
        glm_product = Product.objects.get(supplier_products__supplier=self.glm)
        self.assertNotEqual(aris_product.id, glm_product.id)
        self.assertFalse(SupplierProduct.objects.filter(supplier=self.glm, product=aris_product).exists())
        self.assertFalse(SupplierProduct.objects.filter(supplier=self.aris, product=glm_product).exists())

    def test_reimporting_same_supplier_updates_price_without_duplicating(self):
        # El match exacto SIGUE funcionando, pero acotado al propio proveedor:
        # reimportar la lista de Aris con precio actualizado no duplica el
        # producto, actualiza el vínculo existente.
        self._import(self.aris, [["FIDELITE", "Fidelite Coloracion 8", 100]])
        self._import(self.aris, [["FIDELITE", "  fidelite   coloracion 8  ", 110]])

        self.assertEqual(Product.objects.count(), 1)
        link = SupplierProduct.objects.get(supplier=self.aris)
        self.assertEqual(link.last_cost, Decimal("110.00"))

    def test_only_products_of_pinned_supplier_reach_the_price_list(self):
        # Escenario completo reportado: Aris es principal de FIDELITE. Antes,
        # el matcheo difuso podía "colar" un producto de GLM (incluso con
        # precio y todo) bajo el mismo Product de Aris. Ahora, si Aris no
        # tiene ese producto exacto, no aparece en la lista propia.
        self._import(self.aris, [["FIDELITE", "Fidelite Coloracion 8 Rubio Oscuro", 100]])
        self._import(self.glm, [["FIDELITE", "Fidelite Coloracion 8 Rubio", 90]])
        from inventory.models import BrandSupplier

        BrandSupplier.objects.create(group="FIDELITE", supplier=self.aris)
        from inventory import services

        for p in Product.objects.all():
            services.sync_principal_to_cheapest(p)

        url = reverse("inventory_product_prices_download", args=["consumer"])
        resp = self.client.get(url)
        from openpyxl import load_workbook

        ws = load_workbook(BytesIO(resp.content)).active
        nombres = [r[2] for r in ws.iter_rows(min_row=2, values_only=True)]
        self.assertIn("Fidelite Coloracion 8 Rubio Oscuro", nombres)
        self.assertNotIn("Fidelite Coloracion 8 Rubio", nombres)


class PriceListImportReplacesPreviousListTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="u", password="x")
        self.client.force_login(self.user)
        self.aris = Supplier.objects.create(name="Aris Norma")
        self.st = Supplier.objects.create(name="ST Distribuidora")

    def _import(self, supplier, rows, **extra):
        return self.client.post(
            reverse("inventory_suppliers"),
            {
                "action": "import_price_list",
                "price_supplier_id": supplier.id,
                "price_file": _xlsx_upload(rows),
                "price_vat": "0",
                **extra,
            },
        )

    def test_new_list_drops_discontinued_and_adds_new(self):
        self._import(self.aris, [["IGORA", "Igora Royal", 100], ["IGORA", "Igora Vibrance", 80]])
        self._import(self.aris, [["IGORA", "Igora Royal", 120], ["IGORA", "Igora Zero", 90]])

        names = set(
            SupplierProduct.objects.filter(supplier=self.aris).values_list("product__name", flat=True)
        )
        self.assertEqual(names, {"Igora Royal", "Igora Zero"})
        # El discontinuado no tenía otro proveedor ni movimientos: se elimina.
        self.assertFalse(Product.objects.filter(name="Igora Vibrance").exists())
        self.assertEqual(Product.objects.get(name="Igora Royal").avg_cost, Decimal("120.00"))

    def test_discontinued_product_with_other_supplier_is_kept(self):
        self._import(self.aris, [["IGORA", "Igora Royal", 100]])
        product = Product.objects.get(name="Igora Royal")
        SupplierProduct.objects.create(supplier=self.st, product=product, last_cost=Decimal("150.00"))

        self._import(self.aris, [["IGORA", "Igora Zero", 90]])

        product.refresh_from_db()
        self.assertFalse(SupplierProduct.objects.filter(supplier=self.aris, product=product).exists())
        # Pasa a salir del proveedor que le queda, con su costo.
        self.assertEqual(product.default_supplier, self.st)
        self.assertEqual(product.avg_cost, Decimal("150.00"))

    def test_partial_list_keeps_missing_products(self):
        self._import(self.aris, [["IGORA", "Igora Royal", 100], ["IGORA", "Igora Vibrance", 80]])
        self._import(self.aris, [["IGORA", "Igora Royal", 120]], price_keep_missing="1")

        self.assertEqual(SupplierProduct.objects.filter(supplier=self.aris).count(), 2)
