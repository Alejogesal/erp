"""Inventory views package — re-exports all view functions used by urls.py."""
from .dashboard import dashboard
from .products import (
    create_product,
    edit_product,
    import_costs_xlsx,
    import_products,
    product_costs,
    product_delete,
    product_info,
    product_margins,
    product_prices,
    product_prices_download,
    product_search,
    product_variants,
)
from .sales import (
    register_sale,
    sale_delete,
    sale_delivery_status_update,
    sale_edit,
    sale_receipt,
    sale_receipt_pdf,
    sales_list,
)
from .purchases import (
    purchase_delete,
    purchase_edit,
    purchase_receipt,
    purchase_receipt_pdf,
    purchases_list,
    register_purchase,
)
from .stock import stock_list
from .customers import customers_view, customer_history_view, create_credit_note
from .suppliers import suppliers, supplier_history_view
from .mercadolibre import (
    mercadolibre_callback,
    mercadolibre_connect,
    mercadolibre_dashboard,
    mercadolibre_order_sheet,
    mercadolibre_webhook,
    ml_stock_push,
)
from .taxes import taxes_view
from .koda import koda_chat, koda_confirm

__all__ = [
    "dashboard",
    "create_product",
    "edit_product",
    "import_costs_xlsx",
    "import_products",
    "product_costs",
    "product_delete",
    "product_info",
    "product_margins",
    "product_prices",
    "product_prices_download",
    "product_search",
    "product_variants",
    "register_sale",
    "sale_delete",
    "sale_delivery_status_update",
    "sale_edit",
    "sale_receipt",
    "sale_receipt_pdf",
    "sales_list",
    "purchase_delete",
    "purchase_edit",
    "purchase_receipt",
    "purchase_receipt_pdf",
    "purchases_list",
    "register_purchase",
    "stock_list",
    "customers_view",
    "customer_history_view",
    "create_credit_note",
    "suppliers",
    "supplier_history_view",
    "mercadolibre_callback",
    "mercadolibre_connect",
    "mercadolibre_dashboard",
    "mercadolibre_order_sheet",
    "mercadolibre_webhook",
    "ml_stock_push",
    "taxes_view",
    "koda_chat",
    "koda_confirm",
]
