from user_agents import parse

from request_tracker.models import RequestTracker


class RequestLoggerMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Process request
        response = self.get_response(request)

        # Process response
        if not request.path.startswith("/admin"):
            self.log_request(request, response)

        return response

    def log_request(self, request, response):
        ip_address = request.META.get(
            'HTTP_X_FORWARDED_FOR') or request.META.get('REMOTE_ADDR')
        user_agent = request.META.get('HTTP_USER_AGENT', '')
        user_agent_data = parse(user_agent)
        user = request.user if request.user.is_authenticated else None
        log_entry = RequestTracker(
            user=user,
            path=request.path,
            method=request.method,
            status_code=response.status_code,
            request_headers=request.headers,
            request_body=request.body.decode('utf-8'),
            response_headers=dict(response.items()),
            response_body=response.content.decode('utf-8'),
            ip=ip_address,
            mac=request.META.get('HTTP_X_DEVICE_MAC'),
            browser=user_agent,
            os=user_agent_data.os.family,
        )
 

        print (log_entry)
        log_entry.save()
