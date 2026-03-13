"""Product views: create, edit, delete, variants, prices, margins, costs, info, search, import, download."""
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Q
from django.db.models.deletion import ProtectedError
from django.forms import formset_factory
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.text import slugify
from django.views.decorators.http import require_http_methods

from ..models import (
    KitComponent,
    Product,
    ProductVariant,
    SupplierProduct,
    Warehouse,
)
from .common import (
    _products_with_last_cost_queryset,
    _product_label_with_last_cost,
)
from .forms import (
    ProductBulkUpdateForm,
    ProductCostRowForm,
    ProductForm,
    ProductVariantForm,
    ProductVariantRowForm,
)
from .utils_xlsx import (
    _build_xlsx,
    _decimal_or_zero,
    _parse_decimal,
    _process_costs_xlsx,
    _read_costs_xlsx_rows,
    _sku_prefix,
)


@login_required
def create_product(request):
    if request.method == "POST":
        form = ProductForm(request.POST)
        if form.is_valid():
            product = form.save(commit=False)
            if not product.sku:
                prefix = _sku_prefix(product.group or "", product.name or "")
                existing = Product.objects.filter(sku__startswith=prefix).values_list("sku", flat=True)
                max_suffix = 0
                for sku in existing:
                    suffix = sku[len(prefix):]
                    if suffix.isdigit():
                        max_suffix = max(max_suffix, int(suffix))
                product.sku = f"{prefix}{max_suffix + 1:04d}"
            product.save()
            messages.success(request, "Producto creado.")
            return redirect("inventory_dashboard")
    else:
        form = ProductForm()
    return render(request, "inventory/product_form.html", {"form": form, "title": "Nuevo producto"})


@login_required
def edit_product(request, pk: int):
    product = get_object_or_404(Product, pk=pk)
    if request.method == "POST":
        form = ProductForm(request.POST, instance=product)
        if form.is_valid():
            product = form.save(commit=False)
            if not product.sku:
                prefix = _sku_prefix(product.group or "", product.name or "")
                existing = Product.objects.filter(sku__startswith=prefix).values_list("sku", flat=True)
                max_suffix = 0
                for sku in existing:
                    suffix = sku[len(prefix):]
                    if suffix.isdigit():
                        max_suffix = max(max_suffix, int(suffix))
                product.sku = f"{prefix}{max_suffix + 1:04d}"
            product.save()
            messages.success(request, "Producto actualizado.")
            return redirect("inventory_product_prices")
        messages.error(request, "Revisá los datos del producto.")
    else:
        form = ProductForm(instance=product)
    return render(request, "inventory/product_form.html", {"form": form, "title": "Editar producto"})


@login_required
def product_variants(request, product_id: int):
    product = get_object_or_404(Product, pk=product_id)
    VariantFormSet = formset_factory(ProductVariantRowForm, extra=0)
    if request.method == "POST":
        action = request.POST.get("action") or ""
        if action == "add_variant":
            add_form = ProductVariantForm(request.POST)
            if add_form.is_valid():
                ProductVariant.objects.create(
                    product=product,
                    name=add_form.cleaned_data["name"].strip(),
                    quantity=add_form.cleaned_data["quantity"],
                )
                messages.success(request, "Variedad agregada.")
                return redirect("inventory_product_variants", product_id=product.id)
            messages.error(request, "Revisá los datos de la variedad.")
        elif action == "update_variants":
            formset = VariantFormSet(request.POST)
            if formset.is_valid():
                for form in formset:
                    variant_id = form.cleaned_data["variant_id"]
                    variant = ProductVariant.objects.filter(id=variant_id, product=product).first()
                    if not variant:
                        continue
                    if form.cleaned_data.get("delete"):
                        variant.delete()
                        continue
                    variant.name = form.cleaned_data["name"].strip()
                    variant.quantity = form.cleaned_data["quantity"]
                    variant.save(update_fields=["name", "quantity"])
                messages.success(request, "Variedades actualizadas.")
                return redirect("inventory_product_variants", product_id=product.id)
            messages.error(request, "Revisá los datos de las variedades.")
    variants = ProductVariant.objects.filter(product=product).order_by("name", "id")
    formset = VariantFormSet(
        initial=[
            {"variant_id": v.id, "name": v.name, "quantity": v.quantity, "delete": False}
            for v in variants
        ]
    )
    add_form = ProductVariantForm()
    return render(
        request,
        "inventory/product_variants.html",
        {
            "product": product,
            "formset": formset,
            "add_form": add_form,
        },
    )


@login_required
def product_prices(request):
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_prices":
            product_id = request.POST.get("product_id")
            product = Product.objects.filter(id=product_id).first()
            if not product:
                messages.error(request, "Producto no encontrado.")
                return redirect("inventory_product_prices")
            price_consumer = _parse_decimal(request.POST.get("price_consumer"))
            price_barber = _parse_decimal(request.POST.get("price_barber"))
            price_distributor = _parse_decimal(request.POST.get("price_distributor"))
            product.price_consumer = price_consumer
            product.price_barber = price_barber
            product.price_distributor = price_distributor
            product.save(update_fields=["price_consumer", "price_barber", "price_distributor"])
            messages.success(request, "Precios actualizados.")
            return redirect("inventory_product_prices")
        if action in {"create_kit", "update_kit"}:
            kit_id = request.POST.get("kit_id") if action == "update_kit" else None
            name = (request.POST.get("kit_name") or "").strip()
            sku = (request.POST.get("kit_sku") or "").strip()
            group = (request.POST.get("kit_group") or "").strip()
            price_distributor = _parse_decimal(request.POST.get("kit_price_distributor"))
            margin_barber = _parse_decimal(request.POST.get("kit_margin_barber"))
            margin_consumer = _parse_decimal(request.POST.get("kit_margin_consumer"))
            if margin_barber > Decimal("100.00") or margin_consumer > Decimal("100.00"):
                messages.error(request, "Los márgenes deben ser porcentajes entre 0 y 100.")
                return redirect("inventory_product_prices")
            component_ids = request.POST.getlist("kit_component")
            component_qtys = request.POST.getlist("kit_quantity")
            if not name:
                messages.error(request, "Ingresá el nombre del kit.")
                return redirect("inventory_product_prices")
            if len(component_ids) < 2:
                messages.error(request, "Agregá al menos 2 productos al kit.")
                return redirect("inventory_product_prices")
            components: list[tuple[Product, Decimal]] = []
            for product_id, qty_raw in zip(component_ids, component_qtys):
                if not product_id:
                    continue
                product = Product.objects.filter(id=product_id).first()
                if not product:
                    continue
                if product.is_kit:
                    messages.error(request, "No se permiten kits dentro de kits.")
                    return redirect("inventory_product_prices")
                if ProductVariant.objects.filter(product=product).exists():
                    messages.error(request, f"El producto {product.name} tiene variedades. No se puede usar en kits.")
                    return redirect("inventory_product_prices")
                qty = _parse_decimal(qty_raw)
                if qty <= 0:
                    continue
                components.append((product, qty))
            if len(components) < 2:
                messages.error(request, "Agregá al menos 2 productos válidos.")
                return redirect("inventory_product_prices")
            with transaction.atomic():
                if kit_id:
                    kit = Product.objects.filter(id=kit_id, is_kit=True).first()
                    if not kit:
                        messages.error(request, "Kit no encontrado.")
                        return redirect("inventory_product_prices")
                else:
                    kit = Product(is_kit=True)
                kit.name = name
                kit.sku = sku or None
                kit.group = group
                kit.price_distributor = price_distributor
                kit.margin_barber = margin_barber
                kit.margin_consumer = margin_consumer
                kit.save()
                KitComponent.objects.filter(kit=kit).delete()
                for component, qty in components:
                    KitComponent.objects.create(kit=kit, component=component, quantity=qty)
            messages.success(request, "Kit guardado.")
            return redirect("inventory_product_prices")
    products = Product.objects.order_by("sku")
    products_no_kits = Product.objects.filter(is_kit=False).order_by("sku")
    kits = Product.objects.filter(is_kit=True).order_by("sku")
    kit_components = KitComponent.objects.select_related("kit", "component").all()
    kit_map: dict[int, dict] = {}
    for kit in kits:
        kit_map[kit.id] = {
            "id": kit.id,
            "name": kit.name,
            "sku": kit.sku or "",
            "group": kit.group or "",
            "price_distributor": f"{(kit.price_distributor or Decimal('0.00')):.2f}",
            "margin_barber": f"{(kit.margin_barber or Decimal('0.00')):.2f}",
            "margin_consumer": f"{(kit.margin_consumer or Decimal('0.00')):.2f}",
            "components": [],
        }
    for comp in kit_components:
        if comp.kit_id in kit_map:
            kit_map[comp.kit_id]["components"].append(
                {
                    "product_id": comp.component_id,
                    "quantity": f"{(comp.quantity or Decimal('0.00')):.2f}",
                }
            )
    group_options = (
        Product.objects.exclude(group="")
        .exclude(group__isnull=True)
        .values_list("group", flat=True)
        .distinct()
        .order_by("group")
    )
    return render(
        request,
        "inventory/product_prices.html",
        {
            "products": products,
            "group_options": group_options,
            "products_no_kits": products_no_kits,
            "kits": kits,
            "kit_data": list(kit_map.values()),
        },
    )


def _parse_margin(value: str | None) -> Decimal:
    """Parse a margin percentage from a web form input (handles both dot and comma as decimal separator)."""
    if not value:
        return Decimal("0")
    try:
        return Decimal(str(value).strip().replace(",", ".")).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


@login_required
@require_http_methods(["GET", "POST"])
def product_margins(request):
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_margin_row":
            product_id = request.POST.get("product_id")
            product = Product.objects.filter(id=product_id).first()
            if not product:
                return JsonResponse({"ok": False, "error": "product_not_found"}, status=404)
            product.margin_consumer = _parse_margin(request.POST.get("margin_consumer"))
            product.margin_barber = _parse_margin(request.POST.get("margin_barber"))
            product.margin_distributor = _parse_margin(request.POST.get("margin_distributor"))
            product.save(update_fields=["margin_consumer", "margin_barber", "margin_distributor"])
            return JsonResponse({"ok": True})

        if action == "bulk_update_all_margins":
            updates = {}
            if request.POST.get("bulk_consumer", "").strip():
                updates["margin_consumer"] = _parse_margin(request.POST.get("bulk_consumer"))
            if request.POST.get("bulk_barber", "").strip():
                updates["margin_barber"] = _parse_margin(request.POST.get("bulk_barber"))
            if request.POST.get("bulk_distributor", "").strip():
                updates["margin_distributor"] = _parse_margin(request.POST.get("bulk_distributor"))
            count = Product.objects.update(**updates) if updates else 0
            return JsonResponse({"ok": True, "count": count})

        if action == "bulk_update_brand_margins":
            brand = request.POST.get("brand", "").strip()
            if not brand:
                return JsonResponse({"ok": False, "error": "no_brand"}, status=400)
            updates = {}
            if request.POST.get("bulk_consumer", "").strip():
                updates["margin_consumer"] = _parse_margin(request.POST.get("bulk_consumer"))
            if request.POST.get("bulk_barber", "").strip():
                updates["margin_barber"] = _parse_margin(request.POST.get("bulk_barber"))
            if request.POST.get("bulk_distributor", "").strip():
                updates["margin_distributor"] = _parse_margin(request.POST.get("bulk_distributor"))
            count = Product.objects.filter(group=brand).update(**updates) if updates else 0
            return JsonResponse({"ok": True, "count": count})

    products = Product.objects.order_by("sku")
    brands = list(
        Product.objects.exclude(group="")
        .exclude(group__isnull=True)
        .values_list("group", flat=True)
        .distinct()
        .order_by("group")
    )
    return render(request, "inventory/product_margins.html", {"products": products, "brands": brands})


@login_required
@require_http_methods(["GET", "POST"])
def product_costs(request):
    ProductCostFormSet = formset_factory(ProductCostRowForm, extra=0)
    products_qs = Product.objects.select_related("default_supplier").order_by("sku")
    products = list(products_qs)
    group_options = (
        Product.objects.exclude(group="")
        .exclude(group__isnull=True)
        .values_list("group", flat=True)
        .distinct()
        .order_by("group")
    )
    product_map = {product.id: product for product in products}
    product_form = ProductForm()
    bulk_form = ProductBulkUpdateForm()
    initial = [
        {
            "product_id": product.id,
            "name": product.name,
            "group": product.group,
            "supplier": product.default_supplier,
            "avg_cost": product.avg_cost,
            "vat_percent": product.vat_percent,
            "margin_consumer": product.margin_consumer,
            "margin_barber": product.margin_barber,
            "margin_distributor": product.margin_distributor,
        }
        for product in products
    ]
    formset = ProductCostFormSet(initial=initial)
    if request.method == "POST":
        action = request.POST.get("action", "update_costs")
        if action == "create_product":
            product_form = ProductForm(request.POST)
            if product_form.is_valid():
                product = product_form.save(commit=False)
                if not product.sku:
                    prefix = _sku_prefix(product.group or "", product.name or "")
                    existing = Product.objects.filter(sku__startswith=prefix).values_list("sku", flat=True)
                    max_suffix = 0
                    for sku in existing:
                        suffix = sku[len(prefix):]
                        if suffix.isdigit():
                            max_suffix = max(max_suffix, int(suffix))
                    product.sku = f"{prefix}{max_suffix + 1:04d}"
                product.save()
                messages.success(request, "Producto creado.")
                return redirect("inventory_product_costs")
            messages.error(request, "Revisá los datos del producto.")
        elif action == "bulk_update_margins":
            def parse_optional_decimal(raw: str | None) -> Decimal | None:
                if raw is None:
                    return None
                value = str(raw).strip().replace(",", ".")
                if value == "":
                    return None
                try:
                    return Decimal(value).quantize(Decimal("0.01"))
                except Exception:
                    return None

            margin_consumer = parse_optional_decimal(request.POST.get("margin_consumer"))
            margin_barber = parse_optional_decimal(request.POST.get("margin_barber"))
            margin_distributor = parse_optional_decimal(request.POST.get("margin_distributor"))
            group = (request.POST.get("group") or "").strip()
            if margin_consumer is None and margin_barber is None and margin_distributor is None:
                messages.error(request, "Completá al menos un margen para aplicar cambios.")
                return redirect("inventory_product_costs")

            target_qs = Product.objects.order_by("sku")
            if group:
                target_qs = target_qs.filter(group__iexact=group)
            target_products = list(target_qs)
            if not target_products:
                messages.error(request, "No se encontraron productos para aplicar los cambios.")
                return redirect("inventory_product_costs")

            updated = 0
            for product in target_products:
                update_fields = []
                if margin_consumer is not None:
                    product.margin_consumer = margin_consumer
                    update_fields.append("margin_consumer")
                if margin_barber is not None:
                    product.margin_barber = margin_barber
                    update_fields.append("margin_barber")
                if margin_distributor is not None:
                    product.margin_distributor = margin_distributor
                    update_fields.append("margin_distributor")
                if update_fields:
                    product.save(update_fields=update_fields)
                    updated += 1
            if group:
                messages.success(request, f"Márgenes actualizados en {updated} productos de la marca/grupo '{group}'.")
            else:
                messages.success(request, f"Márgenes actualizados en {updated} productos.")
            return redirect("inventory_product_costs")
        elif action == "import_costs":
            upload = request.FILES.get("file")
            if not upload:
                messages.error(request, "Subí un archivo XLSX.")
            else:
                group_override = (request.POST.get("group_override") or "").strip()
                result = _process_costs_xlsx(upload, group_override=group_override or None)
                if isinstance(result, str):
                    messages.error(request, result)
                else:
                    created, updated, skipped = result
                    if created == 0 and updated == 0 and skipped == 0:
                        messages.warning(
                            request,
                            "No se encontraron filas válidas para importar. Revisá las columnas y datos.",
                        )
                        return redirect("inventory_product_costs")
                    messages.success(
                        request,
                        f"Importación completa. Nuevos: {created}, Actualizados: {updated}, Omitidos: {skipped}.",
                    )
                    return redirect("inventory_product_costs")
        elif action == "delete_import":
            upload = request.FILES.get("file")
            if not upload:
                messages.error(request, "Subí un archivo XLSX.")
            else:
                rows, error = _read_costs_xlsx_rows(upload)
                if error:
                    messages.error(request, error)
                else:
                    deleted = 0
                    skipped = 0
                    not_found = 0
                    for group, description, _cost, sku in rows:
                        product = None
                        if sku:
                            product = Product.objects.filter(sku__iexact=sku).first()
                        if not product:
                            product = Product.objects.filter(name=description, group=group).first()
                        if not product:
                            not_found += 1
                            continue
                        if (
                            product.sale_items.exists()
                            or product.purchase_items.exists()
                            or product.movements.exists()
                            or product.stocks.exists()
                        ):
                            skipped += 1
                            continue
                        product.delete()
                        deleted += 1
                    messages.success(
                        request,
                        f"Eliminación completa. Borrados: {deleted}, En uso: {skipped}, No encontrados: {not_found}.",
                    )
                    return redirect("inventory_product_costs")
        elif action == "delete_product":
            product_id = request.POST.get("product_id")
            if not product_id:
                messages.error(request, "No se pudo identificar el producto.")
                return redirect("inventory_product_costs")
            product = Product.objects.filter(id=product_id).first()
            if not product:
                messages.error(request, "Producto no encontrado.")
                return redirect("inventory_product_costs")
            try:
                product.delete()
                messages.success(request, "Producto eliminado.")
            except ProtectedError:
                messages.error(
                    request,
                    "No se puede eliminar el producto porque está usado en ventas o movimientos. Podés desactivarlo retirando stock o duplicarlo.",
                )
            return redirect("inventory_product_costs")
        elif action == "quick_update_cost":
            product_id = request.POST.get("product_id")
            if not product_id:
                return JsonResponse({"ok": False, "error": "missing_product_id"}, status=400)
            product = Product.objects.filter(id=product_id).first()
            if not product:
                return JsonResponse({"ok": False, "error": "product_not_found"}, status=404)
            update_fields = []
            if "avg_cost" in request.POST:
                avg_cost = _parse_decimal(request.POST.get("avg_cost"))
                if product.avg_cost != avg_cost:
                    product.avg_cost = avg_cost
                    update_fields.append("avg_cost")
            if "vat_percent" in request.POST:
                vat_percent = _parse_decimal(request.POST.get("vat_percent"))
                if product.vat_percent != vat_percent:
                    product.vat_percent = vat_percent
                    update_fields.append("vat_percent")
            if "margin_consumer" in request.POST:
                margin_consumer = _parse_decimal(request.POST.get("margin_consumer"))
                if product.margin_consumer != margin_consumer:
                    product.margin_consumer = margin_consumer
                    update_fields.append("margin_consumer")
            if "margin_barber" in request.POST:
                margin_barber = _parse_decimal(request.POST.get("margin_barber"))
                if product.margin_barber != margin_barber:
                    product.margin_barber = margin_barber
                    update_fields.append("margin_barber")
            if "margin_distributor" in request.POST:
                margin_distributor = _parse_decimal(request.POST.get("margin_distributor"))
                if product.margin_distributor != margin_distributor:
                    product.margin_distributor = margin_distributor
                    update_fields.append("margin_distributor")
            if update_fields:
                product.save(update_fields=update_fields)
            return JsonResponse({"ok": True})
        elif action == "bulk_update":
            bulk_form = ProductBulkUpdateForm(request.POST)
            if bulk_form.is_valid():
                group = (bulk_form.cleaned_data.get("group") or "").strip()
                supplier = bulk_form.cleaned_data.get("supplier")
                cost_percent = bulk_form.cleaned_data.get("cost_percent")
                margin_consumer = bulk_form.cleaned_data.get("margin_consumer")
                margin_barber = bulk_form.cleaned_data.get("margin_barber")
                margin_distributor = bulk_form.cleaned_data.get("margin_distributor")
                if (
                    not group
                    and not supplier
                    and cost_percent is None
                    and margin_consumer is None
                    and margin_barber is None
                    and margin_distributor is None
                ):
                    messages.error(request, "Completá al menos un campo para aplicar cambios.")
                else:
                    target_qs = Product.objects.select_related("default_supplier").order_by("sku")
                    if group:
                        target_qs = target_qs.filter(group__iexact=group)

                    target_products = list(target_qs)
                    if not target_products:
                        messages.error(request, "No se encontraron productos para aplicar los cambios.")
                        return redirect("inventory_product_costs")

                    updated = 0
                    for product in target_products:
                        update_fields = []
                        if group and product.group != group:
                            product.group = group
                            update_fields.append("group")
                        if supplier and product.default_supplier_id != supplier.id:
                            product.default_supplier = supplier
                            update_fields.append("default_supplier")
                        if cost_percent is not None:
                            multiplier = Decimal("1.00") + (cost_percent / Decimal("100.00"))
                            product.avg_cost = (product.avg_cost or Decimal("0.00")) * multiplier
                            product.avg_cost = product.avg_cost.quantize(Decimal("0.01"))
                            update_fields.append("avg_cost")
                        if margin_consumer is not None:
                            product.margin_consumer = margin_consumer
                            update_fields.append("margin_consumer")
                        if margin_barber is not None:
                            product.margin_barber = margin_barber
                            update_fields.append("margin_barber")
                        if margin_distributor is not None:
                            product.margin_distributor = margin_distributor
                            update_fields.append("margin_distributor")
                        if update_fields:
                            product.save(update_fields=update_fields)
                            updated += 1
                        if supplier:
                            SupplierProduct.objects.update_or_create(
                                supplier=supplier,
                                product=product,
                                defaults={
                                    "last_cost": product.avg_cost,
                                    "last_purchase_at": timezone.now(),
                                },
                            )
                    messages.success(request, f"Actualización masiva aplicada a {updated} productos.")
                    return redirect("inventory_product_costs")
        elif action == "update_cost_by_product":
            product_id = request.POST.get("product_id")
            avg_cost_raw = request.POST.get("avg_cost")
            if not product_id:
                messages.error(request, "Seleccioná un producto.")
                return redirect("inventory_product_costs")
            product = Product.objects.filter(id=product_id).first()
            if not product:
                messages.error(request, "Producto no encontrado.")
                return redirect("inventory_product_costs")
            avg_cost = _parse_decimal(avg_cost_raw)
            product.avg_cost = avg_cost
            product.save(update_fields=["avg_cost"])
            messages.success(request, "Costo actualizado.")
            return redirect("inventory_product_costs")
        else:
            formset = ProductCostFormSet(request.POST)
            if formset.is_valid():
                has_errors = False
                for form in formset:
                    product = product_map.get(form.cleaned_data["product_id"])
                    if not product:
                        continue
                    name = (form.cleaned_data.get("name") or "").strip()
                    group = (form.cleaned_data.get("group") or "").strip()
                    if not name:
                        form.add_error("name", "Nombre requerido.")
                        has_errors = True
                if has_errors:
                    messages.error(request, "Revisá el SKU o nombre del producto.")
                    return render(
                        request,
                        "inventory/cost_list.html",
                        {"formset": formset, "product_form": product_form},
                    )

                for form in formset:
                    product = product_map.get(form.cleaned_data["product_id"])
                    if not product:
                        continue
                    avg_cost = form.cleaned_data["avg_cost"]
                    supplier = form.cleaned_data.get("supplier")
                    name = form.cleaned_data.get("name") or product.name
                    group = (form.cleaned_data.get("group") or "").strip()
                    vat_percent = form.cleaned_data.get("vat_percent")
                    margin_consumer = form.cleaned_data.get("margin_consumer")
                    margin_barber = form.cleaned_data.get("margin_barber")
                    margin_distributor = form.cleaned_data.get("margin_distributor")
                    if vat_percent is None:
                        vat_percent = Decimal("0.00")
                    update_fields = []
                    if product.name != name:
                        product.name = name
                        update_fields.append("name")
                    if product.group != group:
                        product.group = group
                        update_fields.append("group")
                    if product.avg_cost != avg_cost:
                        product.avg_cost = avg_cost
                        update_fields.append("avg_cost")
                    if product.vat_percent != vat_percent:
                        product.vat_percent = vat_percent
                        update_fields.append("vat_percent")
                    if margin_consumer is not None and product.margin_consumer != margin_consumer:
                        product.margin_consumer = margin_consumer
                        update_fields.append("margin_consumer")
                    if margin_barber is not None and product.margin_barber != margin_barber:
                        product.margin_barber = margin_barber
                        update_fields.append("margin_barber")
                    if margin_distributor is not None and product.margin_distributor != margin_distributor:
                        product.margin_distributor = margin_distributor
                        update_fields.append("margin_distributor")
                    if supplier and product.default_supplier_id != supplier.id:
                        product.default_supplier = supplier
                        update_fields.append("default_supplier")
                    if update_fields:
                        product.save(update_fields=update_fields)
                    if supplier:
                        SupplierProduct.objects.update_or_create(
                            supplier=supplier,
                            product=product,
                            defaults={"last_cost": avg_cost, "last_purchase_at": timezone.now()},
                        )
                messages.success(request, "Costos, márgenes y proveedores actualizados.")
                return redirect("inventory_product_costs")
            messages.error(request, "Revisá los costos ingresados.")
    return render(
        request,
        "inventory/cost_list.html",
        {
            "formset": formset,
            "product_form": product_form,
            "bulk_form": bulk_form,
            "group_options": group_options,
            "products": products,
        },
    )


@login_required
def product_info(request):
    product_id = request.GET.get("product_id")
    if not product_id:
        return JsonResponse({"ok": False, "error": "missing_product_id"}, status=400)
    product = _products_with_last_cost_queryset().select_related("default_supplier").filter(id=product_id).first()
    if not product:
        return JsonResponse({"ok": False, "error": "product_not_found"}, status=404)
    return JsonResponse(
        {
            "ok": True,
            "default_supplier_id": product.default_supplier_id,
            "avg_cost": f"{(product.avg_cost or Decimal('0.00')):.2f}",
            "vat_percent": f"{(product.vat_percent or Decimal('0.00')):.2f}",
            "cost_with_vat": f"{product.cost_with_vat():.2f}",
        }
    )


@login_required
def product_search(request):
    term = (request.GET.get("q") or "").strip()
    if not term:
        return JsonResponse({"ok": True, "results": []})
    qs = _products_with_last_cost_queryset().filter(
        Q(sku__icontains=term) | Q(name__icontains=term) | Q(group__icontains=term)
    )
    results = []
    for product in qs.order_by("sku")[:20]:
        results.append(
            {
                "id": product.id,
                "label": _product_label_with_last_cost(product),
                "default_supplier_id": product.default_supplier_id,
                "avg_cost": f"{(product.avg_cost or Decimal('0.00')):.2f}",
                "vat_percent": f"{(product.vat_percent or Decimal('0.00')):.2f}",
                "cost_with_vat": f"{product.cost_with_vat():.2f}",
            }
        )
    return JsonResponse({"ok": True, "results": results})


@login_required
def product_prices_download(request, audience: str):
    products = Product.objects.order_by("sku")
    groups_raw = request.GET.get("groups", "")
    if groups_raw:
        groups = [g.strip() for g in groups_raw.split(",") if g.strip()]
        if groups:
            products = products.filter(group__in=groups)
    headers = ["Marca", "Producto", "Precio"]
    price_attr_map = {
        "consumer": "consumer_price",
        "barber": "barber_price",
        "distributor": "distributor_price",
    }
    if audience not in price_attr_map:
        return redirect("inventory_product_prices")

    attr = price_attr_map[audience]
    rows = [[p.group or "", p.name, getattr(p, attr)] for p in products]
    xlsx_bytes = _build_xlsx(headers, rows, blue_cols={1}, number_cols={3})
    audience_label_map = {
        "consumer": "consumidor",
        "barber": "peluqueria",
        "distributor": "distribuidor",
    }
    audience_label = audience_label_map.get(audience, audience)
    if groups_raw:
        if len(groups) == 1:
            brand_slug = slugify(groups[0])
        else:
            brand_slug = "varias_marcas"
        filename = f"{brand_slug}_{audience_label}.xlsx"
    else:
        filename = f"precios_{audience_label}.xlsx"
    response = HttpResponse(
        xlsx_bytes,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


@login_required
@require_http_methods(["GET", "POST"])
def import_products(request):
    """
    Importa productos desde un CSV exportado de Google Sheets.
    Encabezados esperados (case-insensitive):
    SKU, Grupo, Nombre, Costo unitario, IVA, Margen consumidor, Margen barber, Margen distribuidor.
    Columnas faltantes se llenan con 0.
    """
    if request.method == "POST":
        upload = request.FILES.get("file")
        if not upload:
            messages.error(request, "Subí un archivo CSV.")
            return redirect("inventory_import_products")

        import csv

        try:
            decoded = upload.read().decode("utf-8-sig").splitlines()
            reader = csv.DictReader(decoded)
        except Exception:
            messages.error(request, "No se pudo leer el CSV. Verificá el formato.")
            return redirect("inventory_import_products")

        required_cols = ["sku", "nombre", "costo unitario"]
        normalized_fieldnames = [name.strip().lower() for name in (reader.fieldnames or [])]
        missing = [col for col in required_cols if col not in normalized_fieldnames]
        if missing:
            messages.error(request, f"Faltan columnas obligatorias: {', '.join(missing)}")
            return redirect("inventory_import_products")

        created = 0
        updated = 0
        for row in reader:
            data = {k.strip().lower(): (v or "").strip() for k, v in row.items()}
            sku = data.get("sku")
            if not sku:
                continue
            product, was_created = Product.objects.get_or_create(
                sku=sku,
                defaults={
                    "name": data.get("nombre", ""),
                    "group": data.get("grupo", ""),
                    "avg_cost": _decimal_or_zero(data.get("costo unitario")),
                    "vat_percent": _decimal_or_zero(data.get("iva")),
                    "margin_consumer": _decimal_or_zero(data.get("margen consumidor")),
                    "margin_barber": _decimal_or_zero(data.get("margen barber")),
                    "margin_distributor": _decimal_or_zero(data.get("margen distribuidor")),
                },
            )
            if not was_created:
                product.name = data.get("nombre", product.name)
                product.group = data.get("grupo", product.group)
                product.avg_cost = _decimal_or_zero(data.get("costo unitario"))
                product.vat_percent = _decimal_or_zero(data.get("iva"))
                product.margin_consumer = _decimal_or_zero(data.get("margen consumidor"))
                product.margin_barber = _decimal_or_zero(data.get("margen barber"))
                product.margin_distributor = _decimal_or_zero(data.get("margen distribuidor"))
                product.save(
                    update_fields=[
                        "name",
                        "group",
                        "avg_cost",
                        "vat_percent",
                        "margin_consumer",
                        "margin_barber",
                        "margin_distributor",
                    ]
                )
                updated += 1
            else:
                created += 1

        messages.success(request, f"Importación completa. Nuevos: {created}, Actualizados: {updated}.")
        return redirect("inventory_product_prices")

    return render(request, "inventory/product_import.html", {"title": "Importar productos"})


@login_required
@require_http_methods(["GET", "POST"])
def import_costs_xlsx(request):
    """
    Importa costos desde un Excel (.xlsx) con columnas:
    Grupo, Descripción, Precio Venta.
    """
    if request.method == "POST":
        upload = request.FILES.get("file")
        if not upload:
            messages.error(request, "Subí un archivo XLSX.")
            return redirect("inventory_import_costs")

        group_override = (request.POST.get("group_override") or "").strip()
        result = _process_costs_xlsx(upload, group_override=group_override or None)
        if isinstance(result, str):
            messages.error(request, result)
            return redirect("inventory_import_costs")
        created, updated, skipped = result
        messages.success(
            request,
            f"Importación completa. Nuevos: {created}, Actualizados: {updated}, Omitidos: {skipped}.",
        )
        return redirect("inventory_product_prices")

    group_options = (
        Product.objects.exclude(group="")
        .order_by("group")
        .values_list("group", flat=True)
        .distinct()
    )
    return render(
        request,
        "inventory/cost_import.html",
        {"title": "Importar costos", "group_options": group_options},
    )


@login_required
def product_delete(request, pk: int):
    product = get_object_or_404(Product, pk=pk)
    try:
        product.delete()
        messages.success(request, "Producto eliminado.")
    except ProtectedError:
        messages.error(
            request,
            "No se puede eliminar el producto porque está usado en ventas o movimientos. Podés desactivarlo retirando stock o duplicarlo.",
        )
    return redirect("inventory_product_prices")
