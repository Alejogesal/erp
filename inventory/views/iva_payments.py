"""IVA payments view — listar, crear y editar pagos de IVA."""
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import get_object_or_404, redirect, render

from ..models import IVAPayment
from .forms import IVAPaymentForm


@login_required
def iva_payments_view(request):
    form = IVAPaymentForm()

    if request.method == "POST":
        action = request.POST.get("action", "")

        if action == "delete":
            pk = request.POST.get("payment_id")
            deleted, _ = IVAPayment.objects.filter(id=pk).delete()
            if deleted:
                messages.success(request, "Pago de IVA eliminado.")
            else:
                messages.error(request, "No se encontró el pago.")
            return redirect("inventory_iva_payments")

        form = IVAPaymentForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Pago de IVA registrado.")
            return redirect("inventory_iva_payments")
        messages.error(request, "Revisá los datos ingresados.")

    payments = IVAPayment.objects.all()
    return render(request, "inventory/iva_payments.html", {"form": form, "payments": payments})


@login_required
def iva_payment_edit(request, pk):
    payment = get_object_or_404(IVAPayment, pk=pk)
    form = IVAPaymentForm(instance=payment)

    if request.method == "POST":
        form = IVAPaymentForm(request.POST, instance=payment)
        if form.is_valid():
            form.save()
            messages.success(request, "Pago de IVA actualizado.")
            return redirect("inventory_iva_payments")
        messages.error(request, "Revisá los datos ingresados.")

    return render(request, "inventory/iva_payment_edit.html", {"form": form, "payment": payment})
