from decimal import Decimal

from django import forms
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.utils import OperationalError
from django.db.models import Case, DecimalField, Sum, Value, When, Q
from django.db.models.deletion import ProtectedError
from django.db.models.functions import Coalesce
from django.forms import formset_factory
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from datetime import datetime, time, timedelta
import io
import zipfile
import re
import unicodedata
import json
import secrets
from xml.sax.saxutils import escape

from . import services
from . import mercadolibre as ml
from .models import (
    Customer,
    CustomerGroupDiscount,
    CustomerProductDiscount,
    MercadoLibreNotification,
    MercadoLibreConnection,
    MercadoLibreItem,
    Product,
    Purchase,
    PurchaseItem,
    Stock,
    StockMovement,
    Supplier,
    SupplierProduct,
    Warehouse,
    Sale,
    SaleItem,
    TaxExpense,
)


class ProductForm(forms.ModelForm):
    class Meta:
        model = Product
        fields = [
            "sku",
            "name",
            "group",
            "avg_cost",
            "vat_percent",
            "margin_consumer",
            "margin_barber",
            "margin_distributor",
            "default_supplier",
        ]
        labels = {
            "avg_cost": "Costo unitario",
            "vat_percent": "IVA %",
            "sku": "SKU",
            "name": "Nombre",
            "group": "Grupo / Marca",
            "margin_consumer": "Margen % consumidor final",
            "margin_barber": "Margen % peluquerías/barberías",
            "margin_distributor": "Margen % distribuidores",
            "default_supplier": "Proveedor principal",
        }


class PurchaseHeaderForm(forms.Form):
    warehouse = forms.ModelChoiceField(queryset=Warehouse.objects.all())


class PurchaseItemForm(forms.Form):
    product = forms.ModelChoiceField(queryset=Product.objects.all())
    quantity = forms.IntegerField(min_value=1)
    unit_cost = forms.DecimalField(min_value=Decimal("0.00"), decimal_places=2)
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.all(), label="Proveedor")
    vat_percent = forms.DecimalField(
        label="IVA %",
        min_value=Decimal("0.00"),
        max_value=Decimal("100.00"),
        decimal_places=2,
        required=False,
        initial=Decimal("0.00"),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["product"].label_from_instance = (
            lambda obj: f"{obj.sku or 'Sin SKU'} - {obj.name} (último costo: {obj.last_purchase_cost_display()})"
        )


class SaleHeaderForm(forms.Form):
    warehouse = forms.ModelChoiceField(queryset=Warehouse.objects.all())
    audiencia = forms.ChoiceField(
        choices=Customer.Audience.choices,
        label="Tipo de venta",
        initial=Customer.Audience.CONSUMER,
        required=False,
    )
    cliente = forms.ModelChoiceField(queryset=Customer.objects.all(), required=False)


class SaleItemForm(forms.Form):
    product = forms.ModelChoiceField(queryset=Product.objects.all())
    quantity = forms.IntegerField(min_value=1)
    vat_percent = forms.DecimalField(
        label="IVA %",
        min_value=Decimal("0.00"),
        max_value=Decimal("100.00"),
        decimal_places=2,
        required=False,
        initial=Decimal("0.00"),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["product"].label_from_instance = (
            lambda obj: f"{obj.sku or 'Sin SKU'} - {obj.name} (último costo: {obj.last_purchase_cost_display()})"
        )


class StockTransferForm(forms.Form):
    product = forms.ModelChoiceField(queryset=Product.objects.all(), label="Producto")
    quantity = forms.DecimalField(min_value=Decimal("0.01"), decimal_places=2, label="Cantidad")


class SupplierForm(forms.ModelForm):
    class Meta:
        model = Supplier
        fields = ["name", "phone"]
        labels = {"name": "Nombre", "phone": "Teléfono"}


class SupplierProductForm(forms.Form):
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.all(), label="Proveedor")
    product = forms.ModelChoiceField(queryset=Product.objects.all(), label="Producto")
    last_cost = forms.DecimalField(
        label="Último costo",
        min_value=Decimal("0.00"),
        decimal_places=2,
        required=False,
    )


class ProductCostRowForm(forms.Form):
    product_id = forms.IntegerField(widget=forms.HiddenInput)
    name = forms.CharField(required=True, label="Producto")
    group = forms.CharField(required=False, label="Marca / Grupo")
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.all(), required=False, label="Proveedor")
    avg_cost = forms.DecimalField(min_value=Decimal("0.00"), decimal_places=2, label="Costo")
    vat_percent = forms.DecimalField(
        min_value=Decimal("0.00"),
        max_value=Decimal("100.00"),
        decimal_places=2,
        required=False,
        label="IVA %",
    )


class ProductBulkUpdateForm(forms.Form):
    group = forms.CharField(required=False, label="Marca / Grupo")
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.all(), required=False, label="Proveedor")
    cost_percent = forms.DecimalField(
        required=False,
        decimal_places=2,
        label="Ajuste % costo",
        help_text="Ej: 10 para subir 10%, -5 para bajar 5%",
    )


@login_required
def dashboard(request):
    start_date = request.GET.get("start_date") or ""
    end_date = request.GET.get("end_date") or ""
    start_dt = None
    end_dt = None
    start_date_obj = None
    end_date_obj = None
    if start_date:
        try:
            parsed = datetime.strptime(start_date, "%Y-%m-%d")
            start_dt = timezone.make_aware(datetime.combine(parsed, time.min))
            start_date_obj = parsed.date()
        except ValueError:
            start_dt = None
    if end_date:
        try:
            parsed = datetime.strptime(end_date, "%Y-%m-%d")
            end_dt = timezone.make_aware(datetime.combine(parsed, time.max))
            end_date_obj = parsed.date()
        except ValueError:
            end_dt = None

    purchase_qs = Purchase.objects.all()
    sale_item_qs = SaleItem.objects.all()
    movement_qs = StockMovement.objects.filter(movement_type=StockMovement.MovementType.EXIT)
    tax_qs = TaxExpense.objects.all()
    if start_dt:
        purchase_qs = purchase_qs.filter(created_at__gte=start_dt)
        sale_item_qs = sale_item_qs.filter(sale__created_at__gte=start_dt)
        movement_qs = movement_qs.filter(created_at__gte=start_dt)
    if start_date_obj:
        tax_qs = tax_qs.filter(paid_at__gte=start_date_obj)
    if end_dt:
        purchase_qs = purchase_qs.filter(created_at__lte=end_dt)
        sale_item_qs = sale_item_qs.filter(sale__created_at__lte=end_dt)
        movement_qs = movement_qs.filter(created_at__lte=end_dt)
    if end_date_obj:
        tax_qs = tax_qs.filter(paid_at__lte=end_date_obj)

    purchase_total = purchase_qs.aggregate(total=Sum("total")).get("total") or Decimal("0.00")
    sale_total = sale_item_qs.aggregate(total=Sum("line_total")).get("total") or Decimal("0.00")
    tax_total = tax_qs.aggregate(total=Sum("amount")).get("total") or Decimal("0.00")
    net_margin = sale_total - purchase_total - tax_total

    ranking_qs = (
        movement_qs.values("product__id", "product__sku", "product__name")
        .annotate(total_quantity=Sum("quantity"))
        .order_by("-total_quantity")[:10]
    )

    ranking = [
        {
            "product_id": item["product__id"],
            "sku": item["product__sku"],
            "name": item["product__name"],
            "quantity": item["total_quantity"],
        }
        for item in ranking_qs
    ]

    context = {
        "purchase_total": purchase_total,
        "sale_total": sale_total,
        "gross_margin": net_margin,
        "gross_margin_pct": (net_margin / sale_total * Decimal("100.00")) if sale_total else None,
        "ranking": ranking,
        "start_date": start_date,
        "end_date": end_date,
        "tax_total": tax_total,
    }
    return render(request, "inventory/dashboard.html", context)


@login_required
def create_product(request):
    if request.method == "POST":
        form = ProductForm(request.POST)
        if form.is_valid():
            form.save()
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
            form.save()
            messages.success(request, "Producto actualizado.")
            return redirect("inventory_product_prices")
        messages.error(request, "Revisá los datos del producto.")
    else:
        form = ProductForm(instance=product)
    return render(request, "inventory/product_form.html", {"form": form, "title": "Editar producto"})


@login_required
def register_purchase(request):
    return purchases_list(request)


@login_required
def register_sale(request):
    return sales_list(request)


@login_required
def sale_receipt(request, sale_id: int):
    sale = get_object_or_404(
        Sale.objects.select_related("customer", "warehouse").prefetch_related("items__product"), pk=sale_id
    )
    subtotal = sum((item.line_total for item in sale.items.all()), Decimal("0.00"))
    context = {
        "sale": sale,
        "items": sale.items.all(),
        "subtotal": subtotal,
        "discount_total": sale.discount_total,
        "total": sale.total,
        "invoice_number": sale.invoice_number,
    }
    return render(request, "inventory/sale_receipt.html", context)


@login_required
def sale_receipt_pdf(request, sale_id: int):
    sale = get_object_or_404(
        Sale.objects.select_related("customer", "warehouse").prefetch_related("items__product"), pk=sale_id
    )
    from django.template.loader import render_to_string

    html = render_to_string(
        "inventory/sale_receipt.html",
        {
            "sale": sale,
            "items": sale.items.all(),
            "subtotal": sum((item.line_total for item in sale.items.all()), Decimal("0.00")),
            "discount_total": sale.discount_total,
            "total": sale.total,
            "is_pdf": True,
            "invoice_number": sale.invoice_number,
        },
        request=request,
    )
    try:
        from weasyprint import HTML
    except Exception:
        return HttpResponse(
            "WeasyPrint no está instalado. Instalalo con 'pip install weasyprint' para generar PDF.",
            status=500,
        )

    pdf_bytes = HTML(string=html, base_url=request.build_absolute_uri("/")).write_pdf()
    response = HttpResponse(pdf_bytes, content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="venta-{sale.id}.pdf"'
    return response


@login_required
def sales_list(request):
    SaleItemFormSet = formset_factory(SaleItemForm, extra=1, can_delete=True)
    customer_audiences = {
        str(customer.id): customer.audience
        for customer in Customer.objects.only("id", "audience")
    }
    customer_query = (request.GET.get("customer") or "").strip()
    customers = Customer.objects.order_by("name")
    if request.method == "POST":
        header_form = SaleHeaderForm(request.POST)
        formset = SaleItemFormSet(request.POST)
        if header_form.is_valid() and formset.is_valid():
            warehouse = header_form.cleaned_data["warehouse"]
            audience = header_form.cleaned_data.get("audiencia") or Customer.Audience.CONSUMER
            customer = header_form.cleaned_data.get("cliente")
            if customer:
                audience = customer.audience
            items = [f.cleaned_data for f in formset.forms if f.cleaned_data and not f.cleaned_data.get("DELETE")]
            if not items:
                messages.error(request, "Agregá al menos un producto.")
            else:
                try:
                    with transaction.atomic():
                        sale = Sale.objects.create(
                            customer=customer,
                            warehouse=warehouse,
                            audience=audience,
                            reference=f"Venta {audience}",
                            user=request.user,
                        )
                        total = Decimal("0.00")
                        discount_total = Decimal("0.00")
                        for data in items:
                            base_price = {
                                Customer.Audience.CONSUMER: data["product"].consumer_price,
                                Customer.Audience.BARBER: data["product"].barber_price,
                                Customer.Audience.DISTRIBUTOR: data["product"].distributor_price,
                            }.get(audience, data["product"].consumer_price)
                            discount = Decimal("0.00")
                            if customer:
                                discount_obj = CustomerProductDiscount.objects.filter(
                                    customer=customer, product=data["product"]
                                ).first()
                                if discount_obj:
                                    discount = discount_obj.discount_percent
                                elif data["product"].group:
                                    group_discount = CustomerGroupDiscount.objects.filter(
                                        customer=customer, group=data["product"].group
                                    ).first()
                                    if group_discount:
                                        discount = group_discount.discount_percent
                            final_price = base_price * (Decimal("1.00") - discount / Decimal("100.00"))
                            qty = Decimal(data["quantity"])
                            line_total = (qty * final_price).quantize(Decimal("0.01"))
                            discount_amount = (qty * (base_price - final_price)).quantize(Decimal("0.01"))
                            SaleItem.objects.create(
                                sale=sale,
                                product=data["product"],
                                quantity=qty,
                                unit_price=base_price,
                                discount_percent=discount,
                                final_unit_price=final_price,
                                line_total=line_total,
                                vat_percent=data.get("vat_percent") or Decimal("0.00"),
                            )
                            total += line_total
                            discount_total += discount_amount
                            services.register_exit(
                                product=data["product"],
                                warehouse=warehouse,
                                quantity=data["quantity"],
                                user=request.user,
                                reference=f"Venta {audience} #{sale.id}",
                                sale_price=final_price,
                                vat_percent=data.get("vat_percent") or Decimal("0.00"),
                                sale=sale,
                            )
                        sale.total = total
                        sale.discount_total = discount_total
                        sale.save(update_fields=["total", "discount_total"])
                    messages.success(request, "Venta registrada.")
                    return redirect("inventory_sale_receipt", sale_id=sale.id)
                except services.NegativeStockError:
                    messages.error(request, "No hay stock suficiente para completar la venta.")
                except services.InvalidMovementError as exc:
                    messages.error(request, str(exc))
        else:
            messages.error(request, "Revisá los campos de la venta.")
    else:
        header_form = SaleHeaderForm()
        formset = SaleItemFormSet()
    sales = (
        Sale.objects.select_related("customer", "warehouse", "user")
        .prefetch_related("items__product")
        .order_by("-created_at", "-id")
    )
    if customer_query:
        sales = sales.filter(customer__name__icontains=customer_query)
    for sale in sales:
        cost_total = sum(
            (item.quantity * (item.product.avg_cost or Decimal("0.00")) for item in sale.items.all()),
            Decimal("0.00"),
        )
        sale.profit_total = (sale.total or Decimal("0.00")) - cost_total
    return render(
        request,
        "inventory/sales_list.html",
        {
            "sales": sales,
            "customer_query": customer_query,
            "customers": customers,
            "form": header_form,
            "formset": formset,
            "customer_audiences": customer_audiences,
        },
    )


@login_required
@require_http_methods(["POST"])
def sale_delete(request, sale_id: int):
    sale = get_object_or_404(Sale.objects.prefetch_related("items", "movements"), pk=sale_id)
    try:
        with transaction.atomic():
            # Revert stock for each movement linked to this sale
            for movement in sale.movements.select_for_update():
                if movement.movement_type == StockMovement.MovementType.EXIT and movement.from_warehouse:
                    stock, _ = Stock.objects.select_for_update().get_or_create(
                        product=movement.product,
                        warehouse=movement.from_warehouse,
                        defaults={"quantity": Decimal("0.00")},
                    )
                    stock.quantity = (stock.quantity + movement.quantity).quantize(Decimal("0.01"))
                    stock.save(update_fields=["quantity"])
                movement.delete()
            sale.delete()
        messages.success(request, "Venta eliminada y stock ajustado.")
    except Exception as exc:
        messages.error(request, f"No se pudo eliminar la venta: {exc}")
    return redirect("inventory_sales_list")


@login_required
def purchases_list(request):
    PurchaseItemFormSet = formset_factory(PurchaseItemForm, extra=1, can_delete=True)
    if request.method == "POST":
        header_form = PurchaseHeaderForm(request.POST)
        formset = PurchaseItemFormSet(request.POST)
        if header_form.is_valid() and formset.is_valid():
            warehouse = header_form.cleaned_data["warehouse"]
            items = [f.cleaned_data for f in formset.forms if f.cleaned_data and not f.cleaned_data.get("DELETE")]
            if not items:
                messages.error(request, "Agregá al menos un producto.")
            else:
                try:
                    with transaction.atomic():
                        purchase_supplier = items[0].get("supplier") if items else None
                        purchase = Purchase.objects.create(
                            supplier=purchase_supplier,
                            warehouse=warehouse,
                            reference="Compra",
                            user=request.user,
                        )
                        total = Decimal("0.00")
                        for data in items:
                            qty = Decimal(data["quantity"])
                            unit_cost = data["unit_cost"]
                            total += qty * unit_cost
                            PurchaseItem.objects.create(
                                purchase=purchase,
                                product=data["product"],
                                quantity=qty,
                                unit_cost=unit_cost,
                                vat_percent=data.get("vat_percent") or Decimal("0.00"),
                            )
                            services.register_entry(
                                product=data["product"],
                                warehouse=warehouse,
                                quantity=qty,
                                unit_cost=unit_cost,
                                supplier=data["supplier"],
                                vat_percent=data.get("vat_percent") or Decimal("0.00"),
                                user=request.user,
                                reference=f"Compra #{purchase.id}",
                                purchase=purchase,
                            )
                        purchase.total = total
                        purchase.save(update_fields=["total"])
                    messages.success(request, "Compra registrada.")
                    return redirect("inventory_purchases_list")
                except Exception as exc:
                    messages.error(request, f"No se pudo registrar la compra: {exc}")
        else:
            messages.error(request, "Revisá los campos de la compra.")
    else:
        header_form = PurchaseHeaderForm()
        formset = PurchaseItemFormSet()

    purchases = (
        Purchase.objects.select_related("supplier", "warehouse", "user")
        .prefetch_related("items__product")
        .order_by("-created_at", "-id")
    )
    return render(
        request,
        "inventory/purchases_list.html",
        {
            "purchases": purchases,
            "form": header_form,
            "formset": formset,
        },
    )


@login_required
@require_http_methods(["POST"])
def purchase_delete(request, purchase_id: int):
    purchase = get_object_or_404(Purchase.objects.prefetch_related("items", "movements"), pk=purchase_id)
    try:
        with transaction.atomic():
            for movement in purchase.movements.select_for_update():
                if movement.movement_type == StockMovement.MovementType.ENTRY and movement.to_warehouse:
                    stock, _ = Stock.objects.select_for_update().get_or_create(
                        product=movement.product,
                        warehouse=movement.to_warehouse,
                        defaults={"quantity": Decimal("0.00")},
                    )
                    stock.quantity = (stock.quantity - movement.quantity).quantize(Decimal("0.01"))
                    if stock.quantity < 0:
                        raise services.NegativeStockError("Stock cannot go negative")
                    stock.save(update_fields=["quantity"])
                movement.delete()
            purchase.delete()
        messages.success(request, "Compra eliminada y stock ajustado.")
    except services.NegativeStockError:
        messages.error(request, "No se puede eliminar: el stock quedaría negativo.")
    except Exception as exc:
        messages.error(request, f"No se pudo eliminar la compra: {exc}")
    return redirect("inventory_purchases_list")


@login_required
@require_http_methods(["GET", "POST"])
def suppliers(request):
    supplier_form = SupplierForm()
    link_form = SupplierProductForm()
    suppliers_qs = Supplier.objects.prefetch_related("supplier_products__product")

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_supplier":
            supplier_form = SupplierForm(request.POST)
            if supplier_form.is_valid():
                supplier_form.save()
                messages.success(request, "Proveedor creado.")
                return redirect("inventory_suppliers")
        elif action == "link_supplier":
            link_form = SupplierProductForm(request.POST)
            if link_form.is_valid():
                supplier = link_form.cleaned_data["supplier"]
                product = link_form.cleaned_data["product"]
                last_cost = link_form.cleaned_data.get("last_cost") or product.avg_cost
                SupplierProduct.objects.update_or_create(
                    supplier=supplier,
                    product=product,
                    defaults={"last_cost": last_cost, "last_purchase_at": timezone.now()},
                )
                if product.default_supplier_id is None:
                    product.default_supplier = supplier
                    product.save(update_fields=["default_supplier"])
                messages.success(request, "Proveedor vinculado al producto.")
                return redirect("inventory_suppliers")
        elif action == "delete_supplier":
            supplier_id = request.POST.get("supplier_id")
            Supplier.objects.filter(pk=supplier_id).delete()
            messages.success(request, "Proveedor eliminado.")
            return redirect("inventory_suppliers")
        elif action == "remove_link":
            link_id = request.POST.get("link_id")
            SupplierProduct.objects.filter(pk=link_id).delete()
            messages.success(request, "Vínculo eliminado.")
            return redirect("inventory_suppliers")

    context = {
        "supplier_form": supplier_form,
        "link_form": link_form,
        "suppliers": suppliers_qs,
    }
    return render(request, "inventory/suppliers.html", context)


@login_required
def stock_list(request):
    decimal_field = DecimalField(max_digits=12, decimal_places=2)
    comun_code = Warehouse.WarehouseType.COMUN
    ml_code = Warehouse.WarehouseType.MERCADOLIBRE
    comun_wh = Warehouse.objects.filter(type=comun_code).first()
    ml_wh = Warehouse.objects.filter(type=ml_code).first()
    transfer_form = StockTransferForm()

    if request.method == "POST":
        transfer_form = StockTransferForm(request.POST)
        if not comun_wh or not ml_wh:
            messages.error(request, "Faltan depósitos configurados para transferir stock.")
            return redirect("inventory_stock_list")
        if transfer_form.is_valid():
            try:
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
    )
    return render(
        request,
        "inventory/stock_list.html",
        {
            "products": products,
            "transfer_form": transfer_form,
            "can_transfer": comun_wh is not None and ml_wh is not None,
        },
    )


@login_required
def product_prices(request):
    products = Product.objects.order_by("sku")
    return render(request, "inventory/product_prices.html", {"products": products})


@login_required
@require_http_methods(["GET", "POST"])
def product_costs(request):
    ProductCostFormSet = formset_factory(ProductCostRowForm, extra=0)
    query = (request.GET.get("q") or "").strip()
    products_qs = Product.objects.select_related("default_supplier").order_by("sku")
    if query:
        products_qs = products_qs.filter(
            Q(sku__icontains=query)
            | Q(name__icontains=query)
            | Q(group__icontains=query)
            | Q(default_supplier__name__icontains=query)
        )
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
        }
        for product in products
    ]
    formset = ProductCostFormSet(initial=initial)
    if request.method == "POST":
        action = request.POST.get("action", "update_costs")
        if action == "create_product":
            product_form = ProductForm(request.POST)
            if product_form.is_valid():
                product_form.save()
                messages.success(request, "Producto creado.")
                return redirect("inventory_product_costs")
            messages.error(request, "Revisá los datos del producto.")
        elif action == "import_costs":
            upload = request.FILES.get("file")
            if not upload:
                messages.error(request, "Subí un archivo XLSX.")
            else:
                result = _process_costs_xlsx(upload)
                if isinstance(result, str):
                    messages.error(request, result)
                else:
                    created, updated = result
                    if created == 0 and updated == 0:
                        messages.warning(
                            request,
                            "No se encontraron filas válidas para importar. Revisá las columnas y datos.",
                        )
                        return redirect("inventory_product_costs")
                    messages.success(
                        request,
                        f"Importación completa. Nuevos: {created}, Actualizados: {updated}.",
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
                    for group, description, _cost in rows:
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
        elif action == "bulk_update":
            bulk_form = ProductBulkUpdateForm(request.POST)
            if bulk_form.is_valid():
                group = (bulk_form.cleaned_data.get("group") or "").strip()
                supplier = bulk_form.cleaned_data.get("supplier")
                cost_percent = bulk_form.cleaned_data.get("cost_percent")
                if not query and not group and not supplier:
                    messages.error(request, "Usá el buscador o completá un filtro (grupo/proveedor) antes de aplicar.")
                elif not group and not supplier and cost_percent is None:
                    messages.error(request, "Completá al menos un campo para aplicar cambios.")
                else:
                    target_qs = Product.objects.select_related("default_supplier").order_by("sku")
                    if query:
                        target_qs = target_qs.filter(
                            Q(sku__icontains=query)
                            | Q(name__icontains=query)
                            | Q(group__icontains=query)
                            | Q(default_supplier__name__icontains=query)
                        )
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
                messages.success(request, "Costos y proveedores actualizados.")
                return redirect("inventory_product_costs")
            messages.error(request, "Revisá los costos ingresados.")
    return render(
        request,
        "inventory/cost_list.html",
        {
            "formset": formset,
            "product_form": product_form,
            "bulk_form": bulk_form,
            "query": query,
            "group_options": group_options,
        },
    )


def _col_letter(idx: int) -> str:
    result = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        result = chr(65 + rem) + result
    return result


def _build_xlsx(headers: list[str], rows: list[list[str | Decimal]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # Root relations
        zf.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
            '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
            "</Types>",
        )
        zf.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>'
            '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>'
            "</Relationships>",
        )
        zf.writestr(
            "docProps/core.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
            'xmlns:dc="http://purl.org/dc/elements/1.1/" '
            'xmlns:dcterms="http://purl.org/dc/terms/" '
            'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
            "<dc:title>Precios</dc:title>"
            "</cp:coreProperties>",
        )
        zf.writestr(
            "docProps/app.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
            'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
            "<Application>Django</Application>"
            "</Properties>",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
            "</Relationships>",
        )
        zf.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="Precios" sheetId="1" r:id="rId1"/></sheets>'
            "</workbook>",
        )

        cols = len(headers)
        rows_count = len(rows) + 1  # header row
        dimension = f"A1:{_col_letter(cols)}{rows_count}"

        def cell_xml(value, col_idx, row_idx, is_header=False):
            col = _col_letter(col_idx)
            ref = f"{col}{row_idx}"
            if is_header or isinstance(value, str):
                return (
                    f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'
                )
            return f'<c r="{ref}"><v>{value}</v></c>'

        rows_xml = []
        header_cells = "".join(cell_xml(h, i + 1, 1, is_header=True) for i, h in enumerate(headers))
        rows_xml.append(f'<row r="1">{header_cells}</row>')
        for ridx, row in enumerate(rows, start=2):
            cells = "".join(cell_xml(val, cidx + 1, ridx) for cidx, val in enumerate(row))
            rows_xml.append(f'<row r="{ridx}">{cells}</row>')

        sheet_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
            f' xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f'<dimension ref="{dimension}"/>'
            "<sheetData>"
            f'{"".join(rows_xml)}'
            "</sheetData>"
            "</worksheet>"
        )
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buf.getvalue()


@login_required
def product_prices_download(request, audience: str):
    products = Product.objects.order_by("sku")
    headers = ["SKU", "Producto", "Precio"]
    price_attr_map = {
        "consumer": "consumer_price",
        "barber": "barber_price",
        "distributor": "distributor_price",
    }
    if audience not in price_attr_map:
        return redirect("inventory_product_prices")

    attr = price_attr_map[audience]
    rows = [[p.sku, p.name, getattr(p, attr)] for p in products]
    xlsx_bytes = _build_xlsx(headers, rows)
    filename = f"precios_{audience}.xlsx"
    response = HttpResponse(
        xlsx_bytes,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def _decimal_or_zero(value: str | None) -> Decimal:
    if value is None or str(value).strip() == "":
        return Decimal("0.00")
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _parse_decimal(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0.00")
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value)).quantize(Decimal("0.01"))
    cleaned = str(value).strip().replace(".", "").replace(",", ".")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    if cleaned == "":
        return Decimal("0.00")
    try:
        return Decimal(cleaned).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _abbr(text: str | None, length: int) -> str:
    if not text:
        return "X" * length
    cleaned = re.sub(r"[^A-Za-z0-9]", "", text).upper()
    return cleaned[:length].ljust(length, "X")


def _sku_prefix(group: str, description: str) -> str:
    words = [w for w in re.split(r"\s+", (description or "").strip()) if w]
    word1 = _abbr(words[0], 3) if len(words) > 0 else "XXX"
    word2 = _abbr(words[1], 3) if len(words) > 1 else "XXX"
    return f"{_abbr(group, 4)}{word1}{word2}"


def _normalize_header(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    without_accents = "".join(c for c in normalized if not unicodedata.combining(c))
    return without_accents.strip().lower()


def _read_costs_xlsx_rows(upload) -> tuple[list[tuple[str, str, Decimal]], str | None]:
    try:
        from openpyxl import load_workbook
    except Exception:
        return [], "Falta la dependencia openpyxl. Instalá openpyxl en el entorno."

    try:
        wb = load_workbook(upload, data_only=True)
        ws = wb.active
    except Exception:
        return [], "No se pudo leer el archivo XLSX."

    headers = [
        str(cell.value).strip() if cell.value is not None else ""
        for cell in next(ws.iter_rows(min_row=1, max_row=1))
    ]
    header_map = {_normalize_header(h): idx for idx, h in enumerate(headers)}
    desc_keys = ["descripcion", "descripción", "producto", "descripcion producto", "nombre"]
    cost_keys = ["precio venta", "precio", "costo", "costo unitario", "precio costo", "precio venta unitario"]
    group_keys = ["grupo", "marca", "categoria", "categoría"]

    def _pick_index(keys: list[str]) -> int | None:
        for key in keys:
            normalized = _normalize_header(key)
            if normalized in header_map:
                return header_map[normalized]
        return None

    desc_idx = _pick_index(desc_keys)
    cost_idx = _pick_index(cost_keys)
    group_idx = _pick_index(group_keys)

    if desc_idx is None or cost_idx is None:
        return [], "Faltan columnas obligatorias: Descripción/Producto y Precio/Costo."

    rows: list[tuple[str, str, Decimal]] = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        group = str(row[group_idx] or "").strip() if group_idx is not None else ""
        description = str(row[desc_idx] or "").strip()
        cost = _parse_decimal(row[cost_idx])
        if not description:
            continue
        rows.append((group, description, cost))
    return rows, None


def _process_costs_xlsx(upload) -> tuple[int, int] | str:
    rows, error = _read_costs_xlsx_rows(upload)
    if error:
        return error

    created = 0
    updated = 0
    prefix_counters: dict[str, int] = {}

    for group, description, cost in rows:

        product = Product.objects.filter(name=description, group=group).first()
        if product:
            product.avg_cost = cost
            product.save(update_fields=["avg_cost"])
            updated += 1
            continue

        prefix = _sku_prefix(group, description)
        if prefix not in prefix_counters:
            existing = (
                Product.objects.filter(sku__startswith=prefix)
                .values_list("sku", flat=True)
            )
            max_suffix = 0
            for sku in existing:
                suffix = sku[len(prefix):]
                if suffix.isdigit():
                    max_suffix = max(max_suffix, int(suffix))
            prefix_counters[prefix] = max_suffix

        prefix_counters[prefix] += 1
        sku = f"{prefix}{prefix_counters[prefix]:04d}"

        Product.objects.create(
            sku=sku,
            name=description,
            group=group,
            avg_cost=cost,
        )
        created += 1

    return created, updated


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

        result = _process_costs_xlsx(upload)
        if isinstance(result, str):
            messages.error(request, result)
            return redirect("inventory_import_costs")
        created, updated = result
        messages.success(request, f"Importación completa. Nuevos: {created}, Actualizados: {updated}.")
        return redirect("inventory_product_prices")

    return render(request, "inventory/cost_import.html", {"title": "Importar costos"})


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


@login_required
@require_http_methods(["GET"])
def mercadolibre_connect(request):
    if not settings.ML_CLIENT_ID or not settings.ML_CLIENT_SECRET or not settings.ML_REDIRECT_URI:
        messages.error(request, "Faltan credenciales de MercadoLibre en las variables de entorno.")
        return redirect("inventory_mercadolibre_dashboard")
    state = secrets.token_urlsafe(16)
    request.session["ml_state"] = state
    return redirect(ml.get_authorize_url(state))


@login_required
@require_http_methods(["GET"])
def mercadolibre_callback(request):
    code = request.GET.get("code")
    state = request.GET.get("state")
    error = request.GET.get("error")
    error_description = request.GET.get("error_description")
    if error:
        return render(
            request,
            "inventory/mercadolibre_callback.html",
            {
                "code": code,
                "state": state,
                "error": error,
                "error_description": error_description,
            },
        )
    if not code:
        messages.error(request, "No se recibió el código de autorización.")
        return redirect("inventory_mercadolibre_dashboard")
    expected_state = request.session.pop("ml_state", None)
    if expected_state and state != expected_state:
        messages.error(request, "State inválido en el callback de MercadoLibre.")
        return redirect("inventory_mercadolibre_dashboard")
    token_data = ml.exchange_code_for_token(code)
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token", "")
    expires_in = int(token_data.get("expires_in", 0) or 0)
    if not access_token:
        messages.error(request, "No se pudo completar la conexión con MercadoLibre.")
        return redirect("inventory_mercadolibre_dashboard")

    connection, _ = MercadoLibreConnection.objects.get_or_create(user=request.user)
    connection.access_token = access_token
    connection.refresh_token = refresh_token
    connection.expires_at = timezone.now() + timedelta(seconds=expires_in) if expires_in else None
    profile = ml.get_user_profile(access_token)
    connection.ml_user_id = str(profile.get("id", "") or "")
    connection.nickname = profile.get("nickname", "") or ""
    connection.save(update_fields=["access_token", "refresh_token", "expires_at", "ml_user_id", "nickname"])

    messages.success(request, "MercadoLibre conectado correctamente.")
    return redirect("inventory_mercadolibre_dashboard")


@login_required
@require_http_methods(["GET", "POST"])
def mercadolibre_dashboard(request):
    missing_credentials = not settings.ML_CLIENT_ID or not settings.ML_CLIENT_SECRET or not settings.ML_REDIRECT_URI
    try:
        connection = MercadoLibreConnection.objects.filter(user=request.user).first()
        items = MercadoLibreItem.objects.select_related("product").order_by("-last_synced")[:200]
    except OperationalError:
        messages.error(request, "Faltan tablas de MercadoLibre. Ejecutá migrate y recargá.")
        connection = None
        items = []
    metrics = {}
    if connection and connection.last_metrics:
        try:
            metrics = json.loads(connection.last_metrics)
        except json.JSONDecodeError:
            metrics = {}

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "sync":
            if not connection or not connection.access_token:
                messages.error(request, "Primero conectá la cuenta de MercadoLibre.")
            else:
                result = ml.sync_items_and_stock(connection, request.user)
                metrics = result.metrics
                messages.success(
                    request,
                    f"Sync OK. Items: {result.total_items}, Matcheados: {result.matched}, "
                    f"Sin match: {result.unmatched}, Stock actualizado: {result.updated_stock}.",
                )

    return render(
        request,
        "inventory/mercadolibre_dashboard.html",
        {
            "connection": connection,
            "items": items,
            "metrics": metrics,
            "missing_credentials": missing_credentials,
        },
    )


@csrf_exempt
@require_http_methods(["GET", "POST"])
def mercadolibre_webhook(request):
    if request.method == "GET":
        return HttpResponse("OK")
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return HttpResponse("Invalid JSON", status=400)
    notification = MercadoLibreNotification.objects.create(
        topic=payload.get("topic", "") or "",
        resource=payload.get("resource", "") or "",
        ml_user_id=str(payload.get("user_id", "") or ""),
        application_id=str(payload.get("application_id", "") or ""),
        raw_payload=request.body.decode("utf-8"),
    )
    try:
        if notification.topic == "orders":
            resource = notification.resource or ""
            parts = resource.strip("/").split("/")
            order_id = ""
            if "orders" in parts:
                idx = parts.index("orders")
                if idx + 1 < len(parts):
                    order_id = parts[idx + 1]
            if order_id:
                connection = MercadoLibreConnection.objects.filter(ml_user_id=notification.ml_user_id).first()
                if connection:
                    User = get_user_model()
                    sync_user = (
                        User.objects.filter(is_superuser=True).order_by("id").first()
                        or User.objects.order_by("id").first()
                    )
                    if sync_user:
                        ml.sync_order(connection, order_id, sync_user)
    except Exception:
        pass
    return HttpResponse(f"OK:{notification.id}")


class CustomerForm(forms.ModelForm):
    class Meta:
        model = Customer
        fields = ["name", "email", "audience"]
        labels = {"name": "Nombre", "email": "Email", "audience": "Tipo"}


class CustomerDiscountForm(forms.ModelForm):
    class Meta:
        model = CustomerProductDiscount
        fields = ["customer", "product", "discount_percent"]
        labels = {
            "customer": "Cliente",
            "product": "Producto",
            "discount_percent": "Descuento %",
        }

    def validate_unique(self):
        return


class CustomerGroupDiscountForm(forms.ModelForm):
    class Meta:
        model = CustomerGroupDiscount
        fields = ["customer", "group", "discount_percent"]
        labels = {
            "customer": "Cliente",
            "group": "Marca / Grupo",
            "discount_percent": "Descuento %",
        }

    def validate_unique(self):
        return


class TaxExpenseForm(forms.ModelForm):
    class Meta:
        model = TaxExpense
        fields = ["description", "amount", "paid_at"]
        labels = {"description": "Descripción", "amount": "Monto", "paid_at": "Fecha"}
        widgets = {"paid_at": forms.DateInput(attrs={"type": "date"})}


@login_required
def customers_view(request):
    customer_form = CustomerForm()
    discount_form = CustomerDiscountForm()
    group_discount_form = CustomerGroupDiscountForm()

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_customer":
            customer_form = CustomerForm(request.POST)
            if customer_form.is_valid():
                customer_form.save()
                messages.success(request, "Cliente creado.")
                return redirect("inventory_customers")
            else:
                messages.error(request, "Revisá los datos del cliente.")
        elif action == "create_discount":
            discount_form = CustomerDiscountForm(request.POST)
            if discount_form.is_valid():
                customer = discount_form.cleaned_data["customer"]
                product = discount_form.cleaned_data["product"]
                discount = discount_form.cleaned_data["discount_percent"]
                CustomerProductDiscount.objects.update_or_create(
                    customer=customer,
                    product=product,
                    defaults={"discount_percent": discount},
                )
                messages.success(request, "Descuento asignado.")
                return redirect("inventory_customers")
            else:
                messages.error(request, "Revisá los datos del descuento.")
        elif action == "create_group_discount":
            group_discount_form = CustomerGroupDiscountForm(request.POST)
            if group_discount_form.is_valid():
                customer = group_discount_form.cleaned_data["customer"]
                group = (group_discount_form.cleaned_data["group"] or "").strip()
                discount = group_discount_form.cleaned_data["discount_percent"]
                CustomerGroupDiscount.objects.update_or_create(
                    customer=customer,
                    group=group,
                    defaults={"discount_percent": discount},
                )
                messages.success(request, "Descuento por grupo asignado.")
                return redirect("inventory_customers")
            messages.error(request, "Revisá los datos del descuento por grupo.")

    customers = Customer.objects.prefetch_related("discounts__product", "group_discounts").order_by("name")
    return render(
        request,
        "inventory/customers.html",
        {
            "customer_form": customer_form,
            "discount_form": discount_form,
            "group_discount_form": group_discount_form,
            "customers": customers,
        },
    )


@login_required
def taxes_view(request):
    tax_form = TaxExpenseForm()
    if request.method == "POST":
        tax_form = TaxExpenseForm(request.POST)
        if tax_form.is_valid():
            tax_form.save()
            messages.success(request, "Impuesto registrado.")
            return redirect("inventory_taxes")
        messages.error(request, "Revisá los datos del impuesto.")

    taxes = TaxExpense.objects.order_by("-paid_at", "-id")
    return render(
        request,
        "inventory/taxes.html",
        {"tax_form": tax_form, "taxes": taxes},
    )
