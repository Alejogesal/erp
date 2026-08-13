import threading

_current_user = threading.local()


def get_current_user():
    return getattr(_current_user, "user", None)


class AuditMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if hasattr(request, "user") and request.user.is_authenticated:
            _current_user.user = request.user
        else:
            _current_user.user = None
        response = self.get_response(request)
        return response


class MercadoLibreAutoSyncMiddleware:
    """Mantener la sincronización con ML sin depender del proceso de fondo.

    El loop del contenedor sincroniza cada pocos minutos, pero puede no estar
    corriendo (start command distinto al del Dockerfile, servicio dormido,
    reinicio) y ahí el stock queda viejo hasta que alguien aprieta el botón.
    Usar el ERP alcanza: si pasó el intervalo, la request dispara el sync en
    segundo plano. Corre después de responder, así no le agrega tiempo a la
    página, y nunca puede romper la request.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        try:
            user = getattr(request, "user", None)
            if user is not None and user.is_authenticated:
                from . import mercadolibre as ml

                ml.maybe_start_background_sync(user)
        except Exception:
            pass
        return response
