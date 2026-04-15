"""Stock list view."""
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Case, DecimalField, Sum, Value, When, Q
from django.db.models.fields import DecimalField as ModelDecimalField
from django.db.models.functions import Coalesce
from django.shortcuts import redirect, render

from .. import services
from ..models import (
    Product,
    ProductVariant,
    Purchase,
    PurchaseItem,
    Stock,
    StockMovement,
    Supplier,
    Warehouse,
)
from .forms import StockTransferForm


def _sync_common_with_variants(product: Product, comun_wh: Warehouse):
    from django.db.models import Sum as _Sum
    total = (
        ProductVariant.objects.filter(product=product)
        .aggregate(total=_Sum("quantity"))
        .get("total")
    )
    total = total if total is not None else Decimal("0.00")
    stock = Stock.objects.select_for_update().get_or_create(
        product=product,
        warehouse=comun_wh,
        defaults={"quantity": total},
    )[0]
    stock.quantity = total
    stock.save(update_fields=["quantity"])


@login_required
def stock_list(request):
    decimal_field = DecimalField(max_digits=12, decimal_places=2)
    comun_code = Warehouse.WarehouseType.COMUN
    ml_code = Warehouse.WarehouseType.MERCADOLIBRE
    comun_wh = Warehouse.objects.filter(type=comun_code).first()
    ml_wh = Warehouse.objects.filter(type=ml_code).first()
    transfer_form = StockTransferForm()
    query = (request.GET.get("q") or "").strip()
    show_history = (request.GET.get("show_history") or "").strip() == "1"
    aris_supplier = Supplier.objects.filter(name__iexact="Aris Norma").first()
    aurill_supplier = Supplier.objects.filter(name__iexact="Aurill- Dario").first()

    def parse_decimal(value: str) -> Decimal:
        raw = (value or "").strip().replace(" ", "")
        if not raw:
            return Decimal("0.00")
        if "," in raw and "." in raw:
            raw = raw.replace(".", "").replace(",", ".")
        elif "," in raw:
            raw = raw.replace(",", ".")
        try:
            return Decimal(raw)
        except Exception:
            return Decimal("0.00")

    if request.method == "POST":
        action = request.POST.get("action") or ""
        if action == "set_comun_stock":
            if not comun_wh:
                messages.error(request, "Falta el depósito común.")
                return redirect("inventory_stock_list")
            product_id = request.POST.get("product_id")
            desired_raw = request.POST.get("quantity")
            product = Product.objects.filter(pk=product_id).first()
            if not product:
                messages.error(request, "Producto no encontrado.")
                return redirect("inventory_stock_list")
            desired = parse_decimal(desired_raw)
            stock = Stock.objects.filter(product=product, warehouse=comun_wh).first()
            current = stock.quantity if stock else Decimal("0.00")
            diff = (desired - current).quantize(Decimal("0.01"))
            try:
                if diff != 0:
                    services.register_adjustment(
                        product=product,
                        warehouse=comun_wh,
                        quantity=diff,
                        user=request.user,
                        reference="Ajuste manual depósito común",
                        allow_negative=True,
                    )
                messages.success(request, "Stock actualizado.")
                return redirect("inventory_stock_list")
            except services.InvalidMovementError as exc:
                messages.error(request, str(exc))
        elif action == "create_ml_purchase":
            if not ml_wh:
                messages.error(request, "Falta el depósito MercadoLibre.")
                return redirect("inventory_stock_list")

            from decimal import ROUND_HALF_UP

            products = (
                Product.objects.order_by("sku")
                .annotate(
                    ml_qty=Coalesce(
                        Sum(
                            Case(
                                When(stocks__warehouse__type=ml_code, then="stocks__quantity"),
                                output_field=decimal_field,
                            )
                        ),
                        Value(0, output_field=decimal_field),
                        output_field=decimal_field,
                    ),
                )
                .filter(ml_qty__gt=0)
            )
            if not products.exists():
                messages.error(request, "No hay stock en MercadoLibre para importar.")
                return redirect("inventory_stock_list")

            purchases = {
                "aurill": {"supplier": aurill_supplier, "items": []},
                "aris": {"supplier": aris_supplier, "items": []},
            }
            for product in products:
                qty = product.ml_qty or Decimal("0.00")
                if qty <= 0:
                    continue
                vat = product.vat_percent or Decimal("0.00")
                unit_cost = product.avg_cost or Decimal("0.00")
                target = "aurill" if "aurill" in (product.group or "").lower() else "aris"
                purchases[target]["items"].append(
                    {
                        "product": product,
                        "quantity": qty,
                        "unit_cost": unit_cost.quantize(Decimal("0.01")),
                        "vat_percent": vat,
                    }
                )

            created = 0
            with transaction.atomic():
                for data in purchases.values():
                    if not data["items"]:
                        continue
                    purchase = Purchase.objects.create(
                        supplier=data["supplier"],
                        warehouse=ml_wh,
                        reference="Compra ML stock inicial",
                        user=request.user,
                    )
                    total = Decimal("0.00")
                    for item in data["items"]:
                        qty = item["quantity"]
                        unit_cost = item["unit_cost"]
                        unit_cost_with_vat = (
                            unit_cost * (Decimal("1.00") + ((item["vat_percent"] or Decimal("0.00")) / Decimal("100.00")))
                        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        total += qty * unit_cost_with_vat
                        PurchaseItem.objects.create(
                            purchase=purchase,
                            product=item["product"],
                            quantity=qty,
                            unit_cost=unit_cost,
                            vat_percent=item["vat_percent"],
                        )
                    purchase.total = total.quantize(Decimal("0.01"))
                    purchase.save(update_fields=["total"])
                    created += 1
            messages.success(request, f"Compras creadas: {created}.")
            return redirect("inventory_stock_list")
        else:
            transfer_form = StockTransferForm(request.POST)
            if not comun_wh or not ml_wh:
                messages.error(request, "Faltan depósitos configurados para transferir stock.")
                return redirect("inventory_stock_list")

            def sync_common_with_variants(product):
                total = (
                    ProductVariant.objects.filter(product=product)
                    .aggregate(total=Sum("quantity"))
                    .get("total")
                )
                total = total if total is not None else Decimal("0.00")
                stock = Stock.objects.select_for_update().get_or_create(
                    product=product,
                    warehouse=comun_wh,
                    defaults={"quantity": total},
                )[0]
                stock.quantity = total
                stock.save(update_fields=["quantity"])

            bulk_product_ids = request.POST.getlist("bulk_product")
            bulk_quantities = request.POST.getlist("bulk_quantity")
            bulk_variant_ids = request.POST.getlist("bulk_variant")
            if bulk_product_ids or bulk_quantities:
                if len(bulk_product_ids) != len(bulk_quantities) or len(bulk_product_ids) != len(bulk_variant_ids):
                    messages.error(request, "La lista de productos está incompleta.")
                    return redirect("inventory_stock_list")

                bulk_items = []
                errors = []
                for index, (product_id, qty_raw, variant_id) in enumerate(
                    zip(bulk_product_ids, bulk_quantities, bulk_variant_ids), start=1
                ):
                    try:
                        quantity = Decimal(qty_raw.replace(",", "."))
                    except (InvalidOperation, ValueError):
                        errors.append(f"Línea {index}: cantidad inválida.")
                        continue
                    if quantity <= 0:
                        errors.append(f"Línea {index}: la cantidad debe ser mayor a 0.")
                        continue
                    product = Product.objects.filter(id=product_id).first()
                    if not product:
                        errors.append(f"Línea {index}: producto no encontrado.")
                        continue
                    variant = None
                    if variant_id:
                        variant = ProductVariant.objects.filter(id=variant_id, product=product).first()
                        if not variant:
                            errors.append(f"Línea {index}: variedad no encontrada.")
                            continue
                    else:
                        if ProductVariant.objects.filter(product=product).exists():
                            errors.append(f"Línea {index}: seleccioná una variedad.")
                            continue
                    bulk_items.append({"product": product, "quantity": quantity, "variant": variant})

                if errors:
                    messages.error(request, " ".join(errors))
                    return redirect("inventory_stock_list")

                try:
                    with transaction.atomic():
                        for item in bulk_items:
                            if item["variant"] is not None:
                                sync_common_with_variants(item["product"])
                                if item["variant"].quantity - item["quantity"] < 0:
                                    raise services.NegativeStockError(
                                        "No hay stock suficiente en depósito común."
                                    )
                                item["variant"].quantity = (item["variant"].quantity - item["quantity"]).quantize(
                                    Decimal("0.01"), rounding=Decimal("0.01").__class__
                                )
                                item["variant"].save(update_fields=["quantity"])
                            services.register_transfer(
                                product=item["product"],
                                from_warehouse=comun_wh,
                                to_warehouse=ml_wh,
                                quantity=item["quantity"],
                                user=request.user,
                                reference="Transferencia Comun -> MercadoLibre",
                            )
                    messages.success(request, f"Transferencias registradas: {len(bulk_items)}.")
                    return redirect("inventory_stock_list")
                except services.NegativeStockError:
                    messages.error(request, "No hay stock suficiente en depósito común.")
                except services.InvalidMovementError as exc:
                    messages.error(request, str(exc))
            elif transfer_form.is_valid():
                try:
                    product = transfer_form.cleaned_data["product"]
                    variant_id = (request.POST.get("variant_id") or "").strip()
                    if variant_id:
                        variant = ProductVariant.objects.filter(id=variant_id, product=product).first()
                        if not variant:
                            messages.error(request, "Variedad no encontrada.")
                            return redirect("inventory_stock_list")
                        with transaction.atomic():
                            sync_common_with_variants(product)
                            if variant.quantity - transfer_form.cleaned_data["quantity"] < 0:
                                raise services.NegativeStockError(
                                    "No hay stock suficiente en depósito común."
                                )
                            variant.quantity = (variant.quantity - transfer_form.cleaned_data["quantity"]).quantize(
                                Decimal("0.01")
                            )
                            variant.save(update_fields=["quantity"])
                            services.register_transfer(
                                product=product,
                                from_warehouse=comun_wh,
                                to_warehouse=ml_wh,
                                quantity=transfer_form.cleaned_data["quantity"],
                                user=request.user,
                                reference="Transferencia Comun -> MercadoLibre",
                            )
                        messages.success(request, "Transferencia registrada.")
                        return redirect("inventory_stock_list")
                    else:
                        if ProductVariant.objects.filter(product=product).exists():
                            messages.error(request, "Seleccioná una variedad.")
                            return redirect("inventory_stock_list")
                    services.register_transfer(
                        product=transfer_form.cleaned_data["product"],
                        from_warehouse=comun_wh,
                        to_warehouse=ml_wh,
                        quantity=transfer_form.cleaned_data["quantity"],
                        user=request.user,
                        reference="Transferencia Comun -> MercadoLibre",
                    )
                    messages.success(request, "Transferencia registrada.")
                    return redirect("inventory_stock_list")
                except services.NegativeStockError:
                    messages.error(request, "No hay stock suficiente en depósito común.")
                except services.InvalidMovementError as exc:
                    messages.error(request, str(exc))
            else:
                error_list = []
                for field_errors in transfer_form.errors.values():
                    error_list.extend(field_errors)
                if error_list:
                    messages.error(request, " ".join(error_list))
                else:
                    messages.error(request, "Revisá los datos de la transferencia.")

    products = (
        Product.objects.order_by("sku")
        .annotate(
            comun_qty=Coalesce(
                Sum(
                    Case(
                        When(stocks__warehouse__type=comun_code, then="stocks__quantity"),
                        output_field=decimal_field,
                    )
                ),
                Value(0, output_field=decimal_field),
                output_field=decimal_field,
            ),
        )
    )
    variant_qty_map = {
        row["product_id"]: Decimal(str(row["total_qty"]))
        for row in ProductVariant.objects.values("product_id").annotate(
            total_qty=Coalesce(Sum("quantity"), Value(0, output_field=decimal_field), output_field=decimal_field)
        )
    }
    variant_data = {}
    for row in ProductVariant.objects.values("id", "product_id", "name").order_by("name", "id"):
        variant_data.setdefault(str(row["product_id"]), []).append({"id": row["id"], "name": row["name"]})
    if query:
        products = products.filter(
            Q(sku__icontains=query) | Q(name__icontains=query) | Q(group__icontains=query)
        )
    for product in products:
        if product.id in variant_qty_map:
            product.comun_qty = variant_qty_map[product.id]
            product.has_variants = True
        else:
            product.has_variants = False
        product.total_qty = product.comun_qty
    if ml_wh and show_history:
        transfer_movements = (
            StockMovement.objects.filter(
                movement_type=StockMovement.MovementType.TRANSFER,
                to_warehouse=ml_wh,
            )
            .select_related("product", "user", "from_warehouse", "to_warehouse")
            .order_by("-created_at", "-id")[:200]
        )
        from django.utils import timezone as tz
        grouped: dict[str, dict] = {}
        for movement in transfer_movements:
            local_date = tz.localtime(movement.created_at).date()
            key = local_date.isoformat()
            group = grouped.get(key)
            if not group:
                group = {
                    "date": local_date,
                    "items": [],
                    "total_qty": Decimal("0.00"),
                    "users": set(),
                    "origins": set(),
                    "references": set(),
                }
                grouped[key] = group
            group["items"].append(
                {
                    "sku": movement.product.sku,
                    "name": movement.product.name,
                    "quantity": movement.quantity,
                    "origin": movement.from_warehouse.name if movement.from_warehouse else "",
                    "user": movement.user.get_full_name() or movement.user.username,
                    "reference": movement.reference,
                }
            )
            group["total_qty"] += movement.quantity
            group["users"].add(movement.user.get_full_name() or movement.user.username)
            group["origins"].add(movement.from_warehouse.name if movement.from_warehouse else "")
            group["references"].add(movement.reference or "")
        transfer_history = []
        for key, group in grouped.items():
            users = sorted(u for u in group["users"] if u)
            origins = sorted(o for o in group["origins"] if o)
            references = sorted(r for r in group["references"] if r)
            transfer_history.append(
                {
                    "date": group["date"],
                    "items": group["items"],
                    "total_qty": group["total_qty"],
                    "user_label": users[0] if len(users) == 1 else "Varios",
                    "origin_label": origins[0] if len(origins) == 1 else "Varios",
                    "reference_label": references[0] if len(references) == 1 else "Varios",
                }
            )
    else:
        transfer_history = []
    return render(
        request,
        "inventory/stock_list.html",
        {
            "products": products,
            "transfer_form": transfer_form,
            "can_transfer": comun_wh is not None and ml_wh is not None,
            "query": query,
            "variant_data": variant_data,
            "transfer_history": transfer_history,
            "show_history": show_history,
        },
    )
