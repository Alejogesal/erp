"""All Django form classes for inventory views."""
from decimal import Decimal

from django import forms
from django.utils import timezone

from ..models import (
    Customer,
    CustomerGroupDiscount,
    CustomerProductDiscount,
    CustomerProductPrice,
    CustomerPayment,
    Product,
    Purchase,
    Sale,
    Supplier,
    SupplierPayment,
    TaxExpense,
    Warehouse,
)
from .common import _products_with_last_cost_queryset, _product_label_with_last_cost


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
    purchase_date = forms.DateField(
        label="Fecha",
        required=False,
        widget=forms.DateInput(attrs={"type": "date"}),
    )
    descuento_total = forms.DecimalField(
        label="Descuento %",
        min_value=Decimal("0.00"),
        max_value=Decimal("100.00"),
        decimal_places=2,
        required=False,
        initial=Decimal("0.00"),
    )
    costo_envio = forms.DecimalField(
        label="Costo de envío",
        min_value=Decimal("0.00"),
        decimal_places=2,
        required=False,
        initial=Decimal("0.00"),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.initial.get("warehouse"):
            first_warehouse = Warehouse.objects.order_by("name").first()
            if first_warehouse:
                self.initial["warehouse"] = first_warehouse
        if not self.initial.get("purchase_date"):
            self.initial["purchase_date"] = timezone.localdate()


class PurchaseItemForm(forms.Form):
    product = forms.ModelChoiceField(queryset=Product.objects.all())
    quantity = forms.IntegerField(min_value=1)
    unit_cost = forms.DecimalField(min_value=Decimal("0.00"), decimal_places=2)
    discount_percent = forms.DecimalField(
        label="DTO %",
        min_value=Decimal("0.00"),
        max_value=Decimal("100.00"),
        decimal_places=2,
        required=False,
        initial=Decimal("0.00"),
    )
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.all(), label="Proveedor", required=False)
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
        self.fields["product"].queryset = _products_with_last_cost_queryset()
        self.fields["product"].empty_label = ""
        self.fields["product"].label_from_instance = _product_label_with_last_cost


class SaleHeaderForm(forms.Form):
    warehouse = forms.ModelChoiceField(queryset=Warehouse.objects.all())
    sale_date = forms.DateField(
        label="Fecha",
        required=False,
        input_formats=["%Y-%m-%d", "%d/%m/%Y"],
        widget=forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
    )
    delivery_status = forms.ChoiceField(
        choices=Sale.DeliveryStatus.choices,
        label="Estado de entrega",
        required=False,
        initial=Sale.DeliveryStatus.NOT_DELIVERED,
    )
    audiencia = forms.ChoiceField(
        choices=Customer.Audience.choices,
        label="Tipo de venta",
        initial=Customer.Audience.CONSUMER,
        required=False,
    )
    cliente = forms.ModelChoiceField(queryset=Customer.objects.all(), required=False)
    total_venta = forms.DecimalField(
        label="Total de venta",
        min_value=Decimal("0.00"),
        decimal_places=2,
        required=False,
    )
    descuento_total = forms.DecimalField(
        label="DTO %",
        min_value=Decimal("0.00"),
        decimal_places=2,
        required=False,
        initial=Decimal("0.00"),
    )
    comision_ml = forms.DecimalField(
        label="Comisión ML",
        min_value=Decimal("0.00"),
        decimal_places=2,
        required=False,
    )
    impuestos_ml = forms.DecimalField(
        label="Impuestos ML",
        min_value=Decimal("0.00"),
        decimal_places=2,
        required=False,
    )


class SaleItemForm(forms.Form):
    product = forms.ModelChoiceField(queryset=Product.objects.all())
    quantity = forms.IntegerField(min_value=1)
    unit_price_override = forms.DecimalField(
        label="Precio unitario",
        min_value=Decimal("0.00"),
        decimal_places=2,
        required=False,
    )
    cost_unit_override = forms.DecimalField(
        label="Costo unitario",
        min_value=Decimal("0.00"),
        decimal_places=2,
        required=False,
    )
    discount_percent = forms.DecimalField(
        label="DTO %",
        min_value=Decimal("0.00"),
        max_value=Decimal("100.00"),
        decimal_places=2,
        required=False,
        initial=Decimal("0.00"),
    )
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
        self.fields["product"].queryset = _products_with_last_cost_queryset()
        self.fields["product"].empty_label = ""
        self.fields["product"].label_from_instance = _product_label_with_last_cost


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


class SupplierGroupForm(forms.Form):
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.all(), label="Proveedor")
    group = forms.CharField(label="Marca / Grupo")
    last_cost = forms.DecimalField(
        label="Último costo (opcional)",
        min_value=Decimal("0.00"),
        decimal_places=2,
        required=False,
    )


class SupplierUnlinkGroupForm(forms.Form):
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.all(), label="Proveedor")
    group = forms.CharField(label="Marca / Grupo")


class ProductVariantForm(forms.Form):
    name = forms.CharField(label="Variedad")
    quantity = forms.DecimalField(min_value=Decimal("0.00"), decimal_places=2, label="Stock")


class ProductVariantRowForm(forms.Form):
    variant_id = forms.IntegerField(widget=forms.HiddenInput)
    name = forms.CharField(label="Variedad")
    quantity = forms.DecimalField(min_value=Decimal("0.00"), decimal_places=2, label="Stock")
    delete = forms.BooleanField(required=False, label="Eliminar")


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
    margin_consumer = forms.DecimalField(required=False, decimal_places=2, label="Margen % consumidor final")
    margin_barber = forms.DecimalField(required=False, decimal_places=2, label="Margen % peluquerías/barberías")
    margin_distributor = forms.DecimalField(required=False, decimal_places=2, label="Margen % distribuidores")


class ProductBulkUpdateForm(forms.Form):
    group = forms.CharField(required=False, label="Marca / Grupo")
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.all(), required=False, label="Proveedor")
    cost_percent = forms.DecimalField(
        required=False,
        decimal_places=2,
        label="Ajuste % costo",
        help_text="Ej: 10 para subir 10%, -5 para bajar 5%",
    )
    margin_consumer = forms.DecimalField(
        required=False,
        decimal_places=2,
        label="Margen % consumidor final",
    )
    margin_barber = forms.DecimalField(
        required=False,
        decimal_places=2,
        label="Margen % peluquerías/barberías",
    )
    margin_distributor = forms.DecimalField(
        required=False,
        decimal_places=2,
        label="Margen % distribuidores",
    )


class CustomerForm(forms.ModelForm):
    email = forms.CharField(label="Teléfono", required=False)

    class Meta:
        model = Customer
        fields = ["name", "email", "audience"]
        labels = {"name": "Nombre", "email": "Teléfono", "audience": "Tipo"}


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


class CustomerProductPriceForm(forms.ModelForm):
    class Meta:
        model = CustomerProductPrice
        fields = ["customer", "product", "unit_price", "unit_cost"]
        labels = {
            "customer": "Cliente",
            "product": "Producto",
            "unit_price": "Precio fijo (opcional)",
            "unit_cost": "Costo fijo (opcional)",
        }

    def validate_unique(self):
        return

    def clean(self):
        cleaned_data = super().clean()
        unit_price = cleaned_data.get("unit_price")
        unit_cost = cleaned_data.get("unit_cost")
        if unit_price is not None and unit_price < 0:
            self.add_error("unit_price", "El precio no puede ser negativo.")
        if unit_cost is not None and unit_cost < 0:
            self.add_error("unit_cost", "El costo no puede ser negativo.")
        return cleaned_data


class CustomerPaymentForm(forms.ModelForm):
    class Meta:
        model = CustomerPayment
        fields = ["sale", "amount", "method", "kind", "paid_at", "notes"]
        labels = {
            "sale": "Pedido",
            "amount": "Monto",
            "method": "Método",
            "kind": "Tipo",
            "paid_at": "Fecha",
            "notes": "Notas",
        }
        widgets = {"paid_at": forms.DateInput(attrs={"type": "date"})}

    def __init__(self, *args, **kwargs):
        customer = kwargs.pop("customer", None)
        super().__init__(*args, **kwargs)
        self.fields["amount"].min_value = Decimal("0.01")
        self.fields["sale"].required = False
        self.fields["notes"].required = False
        if customer is not None:
            self.fields["sale"].queryset = Sale.objects.filter(customer=customer).order_by("-created_at")
        self.fields["sale"].empty_label = "Cuenta corriente"


class CustomerCreditNoteForm(forms.ModelForm):
    class Meta:
        model = CustomerPayment
        fields = ["sale", "amount", "paid_at", "notes"]
        labels = {
            "sale": "Venta relacionada (opcional)",
            "amount": "Monto",
            "paid_at": "Fecha",
            "notes": "Motivo",
        }
        widgets = {"paid_at": forms.DateInput(attrs={"type": "date"})}

    def __init__(self, *args, customer=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["amount"].min_value = Decimal("0.01")
        self.fields["sale"].required = False
        self.fields["notes"].required = False
        if customer is not None:
            self.fields["sale"].queryset = Sale.objects.filter(customer=customer).order_by("-created_at")
        self.fields["sale"].empty_label = "Sin venta específica"


class SupplierPaymentForm(forms.ModelForm):
    class Meta:
        model = SupplierPayment
        fields = ["purchase", "amount", "method", "kind", "paid_at", "notes"]
        labels = {
            "purchase": "Compra",
            "amount": "Monto",
            "method": "Método",
            "kind": "Tipo",
            "paid_at": "Fecha",
            "notes": "Notas",
        }
        widgets = {"paid_at": forms.DateInput(attrs={"type": "date"})}

    def __init__(self, *args, **kwargs):
        supplier = kwargs.pop("supplier", None)
        super().__init__(*args, **kwargs)
        self.fields["amount"].min_value = Decimal("0.01")
        self.fields["purchase"].required = False
        self.fields["notes"].required = False
        if supplier is not None:
            self.fields["purchase"].queryset = Purchase.objects.filter(supplier=supplier).order_by("-created_at")
        self.fields["purchase"].empty_label = "Cuenta corriente"


class TaxExpenseForm(forms.ModelForm):
    class Meta:
        model = TaxExpense
        fields = ["description", "amount", "paid_at"]
        labels = {"description": "Descripción", "amount": "Monto", "paid_at": "Fecha"}
        widgets = {"paid_at": forms.DateInput(attrs={"type": "date"})}
