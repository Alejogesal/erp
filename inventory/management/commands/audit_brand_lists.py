"""Audita, marca por marca, si la lista de precios propia sale del proveedor correcto.

Responde dos preguntas:
  1) ¿En qué marcas los productos están escritos con el texto de un proveedor que
     NO es el principal de esa marca? (Product.name conserva el texto del primer
     proveedor que creó el producto; la lista ahora usa SupplierProduct.supplier_name
     del principal, que solo se completa al importar SU lista de precios.)
  2) ¿Qué productos quedan afuera de la lista por no estar en la lista del
     principal, y cuáles podrían ser duplicados de un mismo producto real?

Uso (en el server / Docker):
    python manage.py audit_brand_lists            # solo marcas con problemas
    python manage.py audit_brand_lists --all      # todas las marcas
    python manage.py audit_brand_lists --brand FIDELITE
"""

from collections import Counter, defaultdict
from decimal import Decimal
from difflib import SequenceMatcher

from django.core.management.base import BaseCommand

from inventory.models import BrandSupplier, Product, Supplier, SupplierProduct
from inventory.views.common import _normalize_lookup_text
from inventory.views.suppliers import _name_numbers


def _gkey(group: str) -> str:
    return (group or "").strip().casefold()


class Command(BaseCommand):
    help = "Audita el proveedor y el texto de cada marca en la lista de precios propia."

    def add_arguments(self, parser):
        parser.add_argument("--all", action="store_true", help="Muestra también las marcas sin problemas.")
        parser.add_argument("--brand", default="", help="Audita una sola marca.")
        parser.add_argument("--full", action="store_true", help="Lista todos los productos, no solo los primeros 15.")

    def handle(self, *args, **options):
        only_brand = _gkey(options["brand"])
        limit = None if options["full"] else 15

        suppliers = {s.id: s.name for s in Supplier.objects.all()}
        pinned = {_gkey(bs.group): bs.supplier_id for bs in BrandSupplier.objects.select_related("supplier")}

        products = [p for p in Product.objects.all() if p.group and not p.is_kit]
        by_brand: dict[str, list[Product]] = defaultdict(list)
        for p in products:
            by_brand[_gkey(p.group)].append(p)

        links_by_product: dict[int, dict[int, SupplierProduct]] = defaultdict(dict)
        for link in SupplierProduct.objects.filter(product_id__in=[p.id for p in products]):
            links_by_product[link.product_id][link.supplier_id] = link

        problem_brands = 0
        for key in sorted(by_brand):
            if only_brand and key != only_brand:
                continue
            brand_products = by_brand[key]
            label = brand_products[0].group.strip()

            principal_id = pinned.get(key)
            if principal_id is None:
                counts = Counter(p.default_supplier_id for p in brand_products if p.default_supplier_id)
                principal_id = counts.most_common(1)[0][0] if counts else None
                origin = "deducido (más frecuente)"
            else:
                origin = "elegido"

            if principal_id is None:
                self.stdout.write(self.style.WARNING(f"\n{label}: sin proveedor principal — la marca no sale en la lista."))
                problem_brands += 1
                continue

            included, missing, unnamed, foreign_text = [], [], [], []
            for p in brand_products:
                link = links_by_product[p.id].get(principal_id)
                has_price = link is not None and link.cost_net > Decimal("0.00")
                if not has_price:
                    missing.append(p)
                    continue
                included.append(p)
                if not link.supplier_name.strip():
                    unnamed.append(p)
                elif link.supplier_name.strip() != p.name.strip():
                    foreign_text.append((p, link.supplier_name.strip()))

            dupes = self._duplicates(included)
            # foreign_text es informativo: es justamente el caso que la lista ahora
            # resuelve bien (texto del proveedor ≠ nombre interno).
            has_problem = bool(unnamed or dupes)
            if not has_problem and not missing and not options["all"] and not only_brand:
                continue
            if has_problem:
                problem_brands += 1

            self.stdout.write(
                f"\n{self.style.MIGRATE_HEADING(label)} → {suppliers.get(principal_id, '?')} ({origin})"
                f" | en la lista: {len(included)} | fuera (el principal no los tiene): {len(missing)}"
            )

            if unnamed:
                self.stdout.write(self.style.WARNING(
                    f"  ⚠ {len(unnamed)} producto(s) se muestran con el nombre interno: nunca se importó "
                    f"la lista de {suppliers.get(principal_id, '?')} para ellos. Reimportala para corregir el texto."
                ))
                self._dump(unnamed, limit, lambda p: p.name)
            if foreign_text:
                self.stdout.write(
                    f"  · {len(foreign_text)} producto(s) se listan con el texto del proveedor "
                    f"(distinto del nombre interno) — esto es lo esperado:"
                )
                self._dump(foreign_text, limit, lambda t: f"{t[1]}   [interno: {t[0].name}]")
            if missing:
                self.stdout.write(f"  · fuera de la lista (los tiene otro proveedor, no el principal):")
                self._dump(missing, limit, lambda p: p.name)
            if dupes:
                self.stdout.write(self.style.WARNING(
                    f"  ⚠ {len(dupes)} par(es) que parecen el MISMO producto cargado dos veces:"
                ))
                self._dump(dupes, limit, lambda t: f"{t[0].name}  ≈  {t[1].name}")

        self.stdout.write("")
        if problem_brands:
            self.stdout.write(self.style.WARNING(f"Marcas con algo para revisar: {problem_brands}."))
        else:
            self.stdout.write(self.style.SUCCESS("Ninguna marca con problemas."))

    def _duplicates(self, items: list[Product]) -> list[tuple[Product, Product]]:
        """Pares de productos de la misma marca que parecen el mismo artículo cargado
        dos veces (mismos números —tamaño/gramaje— y texto muy parecido)."""
        by_nums: dict[tuple, list[tuple[str, Product]]] = defaultdict(list)
        for p in items:
            norm = _normalize_lookup_text(p.name)
            by_nums[_name_numbers(norm)].append((norm, p))
        pairs = []
        for candidates in by_nums.values():
            for i in range(len(candidates)):
                for j in range(i + 1, len(candidates)):
                    (norm_a, pa), (norm_b, pb) = candidates[i], candidates[j]
                    tokens_a, tokens_b = set(norm_a.split()), set(norm_b.split())
                    if tokens_a <= tokens_b or tokens_b <= tokens_a:
                        pairs.append((pa, pb))
                    elif SequenceMatcher(None, norm_a, norm_b).ratio() >= 0.86:
                        pairs.append((pa, pb))
        return pairs

    def _dump(self, items, limit, render):
        shown = items if limit is None else items[:limit]
        for item in shown:
            self.stdout.write(f"      - {render(item)}")
        if limit is not None and len(items) > limit:
            self.stdout.write(f"      … y {len(items) - limit} más (--full para verlos todos)")
