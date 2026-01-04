from django.urls import path

from . import views

urlpatterns = [
    path("", views.dashboard, name="inventory_dashboard"),
    path("mercadolibre/", views.mercadolibre_dashboard, name="inventory_mercadolibre_dashboard"),
    path("mercadolibre/conectar/", views.mercadolibre_connect, name="inventory_mercadolibre_connect"),
    path("mercadolibre/callback/", views.mercadolibre_callback, name="inventory_mercadolibre_callback"),
    path("mercadolibre/webhook/", views.mercadolibre_webhook, name="inventory_mercadolibre_webhook"),
    path("productos/nuevo/", views.create_product, name="inventory_create_product"),
    path("productos/<int:pk>/editar/", views.edit_product, name="inventory_edit_product"),
    path("productos/importar/", views.import_products, name="inventory_import_products"),
    path("productos/precios/", views.product_prices, name="inventory_product_prices"),
    path("productos/costos/", views.product_costs, name="inventory_product_costs"),
    path("productos/<int:pk>/eliminar/", views.product_delete, name="inventory_product_delete"),
    path("productos/precios/<str:audience>/", views.product_prices_download, name="inventory_product_prices_download"),
    path("productos/importar-costos/", views.import_costs_xlsx, name="inventory_import_costs"),
    path("clientes/", views.customers_view, name="inventory_customers"),
    path("proveedores/", views.suppliers, name="inventory_suppliers"),
    path("compras/registrar/", views.register_purchase, name="inventory_register_purchase"),
    path("compras/", views.purchases_list, name="inventory_purchases_list"),
    path("compras/<int:purchase_id>/eliminar/", views.purchase_delete, name="inventory_purchase_delete"),
    path("ventas/registrar/", views.register_sale, name="inventory_register_sale"),
    path("ventas/", views.sales_list, name="inventory_sales_list"),
    path("ventas/<int:sale_id>/comprobante/", views.sale_receipt, name="inventory_sale_receipt"),
    path("ventas/<int:sale_id>/comprobante.pdf", views.sale_receipt_pdf, name="inventory_sale_receipt_pdf"),
    path("ventas/<int:sale_id>/eliminar/", views.sale_delete, name="inventory_sale_delete"),
    path("stock/", views.stock_list, name="inventory_stock_list"),
    path("impuestos/", views.taxes_view, name="inventory_taxes"),
]
