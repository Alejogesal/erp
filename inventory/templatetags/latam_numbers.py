from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from django import template

register = template.Library()


@register.filter(name="latam_number")
def latam_number(value, decimals=2):
    """Format numbers as 1.234,56 with configurable decimals."""
    try:
        decimals = int(decimals)
    except (TypeError, ValueError):
        decimals = 2

    if value is None:
        value = Decimal("0")

    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return value

    quantizer = Decimal("1") if decimals <= 0 else Decimal("1." + ("0" * decimals))
    number = number.quantize(quantizer, rounding=ROUND_HALF_UP)
    formatted = f"{number:,.{decimals}f}"
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")
