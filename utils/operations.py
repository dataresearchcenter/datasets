from memorious.logic.context import Context
from servicelayer import env


DEBUG = env.to_bool("DEBUG")
PROXY = env.get("MEMORIOUS_CRAWL_PROXY")


def init(context: Context, data):
    """Set crawl proxy if not running in debug mode"""
    if not DEBUG and PROXY:
        context.http.reset()
        proxies = {"http": PROXY, "https": PROXY}
        context.http.session.proxies = proxies
        # Explicitly save the session because no actual HTTP requests were made.
        context.http.save()
    context.emit(data=data)
