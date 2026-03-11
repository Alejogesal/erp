"""Taxes view."""
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render

from ..models import TaxExpense
from .forms import TaxExpenseForm


@login_required
def taxes_view(request):
    tax_form = TaxExpenseForm()
    if request.method == "POST":
        action = request.POST.get("action") or ""
        if action == "delete_tax":
            tax_id = request.POST.get("tax_id")
            deleted, _ = TaxExpense.objects.filter(id=tax_id).delete()
            if deleted:
                messages.success(request, "Impuesto eliminado.")
            else:
                messages.error(request, "No se encontró el impuesto.")
            return redirect("inventory_taxes")

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
