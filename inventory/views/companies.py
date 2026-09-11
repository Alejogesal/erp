"""Company (cuentas/empresas multi-cuenta) views."""
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render

from ..models import Company, MercadoLibreConnection, Warehouse
from .forms import CompanyForm


@login_required
def companies_view(request):
    edit_id = request.GET.get("edit")
    editing = Company.objects.filter(id=edit_id).first() if edit_id else None
    company_form = CompanyForm(instance=editing) if editing else CompanyForm()

    if request.method == "POST" and request.POST.get("action") == "save_company":
        instance_id = request.POST.get("company_id")
        instance = Company.objects.filter(id=instance_id).first() if instance_id else None
        company_form = CompanyForm(request.POST, instance=instance)
        if company_form.is_valid():
            is_new = instance is None
            company = company_form.save()
            if is_new:
                # Toda cuenta ML nueva necesita su propio depósito Full — se
                # crea acá para no obligar a un segundo paso en el admin.
                Warehouse.objects.get_or_create(
                    type=Warehouse.WarehouseType.MERCADOLIBRE,
                    company=company,
                    defaults={"name": f"MercadoLibre {company.name}"},
                )
                messages.success(
                    request,
                    f"Empresa «{company.name}» creada, con su depósito Full listo. "
                    "Ahora conectala desde MercadoLibre.",
                )
            else:
                messages.success(request, f"Empresa «{company.name}» actualizada.")
            return redirect("inventory_companies")
        messages.error(request, "Revisá los datos de la empresa.")

    connections = {
        c.company_id: c
        for c in MercadoLibreConnection.objects.exclude(company__isnull=True)
    }
    warehouses_by_company = {
        wh.company_id: wh
        for wh in Warehouse.objects.filter(type=Warehouse.WarehouseType.MERCADOLIBRE)
    }
    rows = []
    for company in Company.objects.order_by("name"):
        connection = connections.get(company.id)
        rows.append({
            "company": company,
            "warehouse": warehouses_by_company.get(company.id),
            "connection": connection,
            "is_connected": bool(connection and connection.access_token),
        })

    return render(
        request,
        "inventory/companies_list.html",
        {
            "rows": rows,
            "form": company_form,
            "editing": editing,
        },
    )
