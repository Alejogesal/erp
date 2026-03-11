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
